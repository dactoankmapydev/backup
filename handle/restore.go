package handle

import (
	"backup-chunk/cache"
	"backup-chunk/storage"
	supportos "backup-chunk/supportos/unix"

	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type Download struct {
	Storage storage.S3
}

func (d *Download) Download(recoveryPointID string, destDir string) error {
	indexPath := filepath.Join(".cache", recoveryPointID, "index.json")
	indexKey := filepath.Join(recoveryPointID, "index.json")
	_, err := os.Stat(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("Get %s from storage \n", indexKey)
			buf, err := d.Storage.GetObject(bucket, indexKey)
			if err == nil {
				_ = os.MkdirAll(filepath.Join(".cache", recoveryPointID), 0700)
				if err := ioutil.WriteFile(indexPath, buf, 0700); err != nil {
					fmt.Printf("Error writing %s file %+v ", indexKey, err)
					return err
				}
			} else {
				fmt.Printf("Get %s from storage error %+v ", indexKey, err)
				return err
			}
		} else {
			fmt.Printf("Start %s file error %+v ", indexKey, err)
			return err
		}
	}

	index := cache.Index{}
	buf, err := ioutil.ReadFile(indexPath)
	if err != nil {
		fmt.Printf("Read %s error %+v ", indexKey, err)
		return err
	} else {
		_ = json.Unmarshal([]byte(buf), &index)
	}

	fmt.Printf("Download to directory %s \n", filepath.Clean(destDir))
	if err := d.restoreDirectory(index, filepath.Clean(destDir)); err != nil {
		fmt.Printf("Download file %s error %+v ", filepath.Clean(destDir), err)
		return err
	}

	return nil
}

func (d *Download) restoreDirectory(index cache.Index, destDir string) error {
	sem := semaphore.NewWeighted(int64(5))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	group, ctx := errgroup.WithContext(ctx)

	for _, item := range index.Items {
		item := item
		err := sem.Acquire(ctx, 1)
		if err != nil {
			continue
		}
		group.Go(func() error {
			defer sem.Release(1)
			err := d.downloadItem(ctx, *item, destDir)
			if err != nil {
				fmt.Printf("Download item %s error %+v ", item.AbsolutePath, err)
				return err
			}
			return nil
		})

	}

	if err := group.Wait(); err != nil {
		fmt.Printf("Has goroutine error %+v ", err)
		cancel()
		return err
	}

	return nil
}

func (d *Download) downloadItem(ctx context.Context, item cache.Node, destDir string) error {
	select {
	case <-ctx.Done():
		return errors.New("Download item done")
	default:
		var pathItem string
		if destDir == item.BasePath {
			pathItem = item.AbsolutePath
		} else {
			pathItem = filepath.Join(destDir, item.RelativePath)
		}
		switch item.Type {
		case "symlink":
			err := d.downloadSymlink(pathItem, item)
			if err != nil {
				fmt.Printf("Download symlink error %+v ", err)
				return err
			}
		case "dir":
			err := d.downloadDirectory(pathItem, item)
			if err != nil {
				fmt.Printf("Download directory error %+v ", err)
				return err
			}
		case "file":
			err := d.downloadFile(pathItem, item)
			if err != nil {
				fmt.Printf("Download file error %+v ", err)
				return err
			}
		}
	}
	return nil
}

func (d *Download) downloadSymlink(pathItem string, item cache.Node) error {
	_, err := os.Stat(pathItem)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Symlink not exist, create ", pathItem)
			err := d.createSymlink(item.LinkTarget, pathItem, item.Mode, int(item.UID), int(item.GID))
			if err != nil {
				return err
			}
			return nil
		} else {
			return err
		}
	} else {
		fmt.Println("Symlink exist ", pathItem)
	}
	return nil
}

func (d *Download) downloadDirectory(pathItem string, item cache.Node) error {
	_, err := os.Stat(pathItem)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Directory not exist, create ", pathItem)
			err := d.createDirectory(pathItem, os.ModeDir|item.Mode, int(item.UID), int(item.GID), item.AccessTime, item.ModTime)
			if err != nil {
				return err
			}
			return nil
		} else {
			return err
		}
	} else {
		fmt.Println("Directory exist ", pathItem)
	}
	return nil
}

func (d *Download) downloadFile(pathItem string, item cache.Node) error {
	_, err := os.Stat(pathItem)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("File not exist, create ", pathItem)
			file, err := d.createFile(pathItem, item.Mode, int(item.UID), int(item.GID))
			if err != nil {
				return err
			}

			err = d.writeFile(file, item)
			if err != nil {
				return err
			}
			return nil
		} else {
			return err
		}
	} else {
		fmt.Println("File exist ", pathItem)
	}

	return nil
}

func (d *Download) writeFile(file *os.File, item cache.Node) error {
	for _, info := range item.Content {

		offset := info.Start
		key := info.Etag

		data, err := d.Storage.GetObject(bucket, key)
		if err != nil {
			return err
		}

		_, err = file.WriteAt(data, int64(offset))
		if err != nil {
			fmt.Printf("Write file error %+v", err)
			return err
		}
	}

	err := os.Chmod(file.Name(), item.Mode)
	if err != nil {
		return err
	}
	_ = supportos.SetChownItem(file.Name(), int(item.UID), int(item.GID))
	err = os.Chtimes(file.Name(), item.AccessTime, item.ModTime)
	if err != nil {
		return err
	}

	return nil
}

func (d *Download) createSymlink(symlinkPath string, path string, mode fs.FileMode, uid int, gid int) error {
	dirName := filepath.Dir(path)
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		if err := os.MkdirAll(dirName, os.ModePerm); err != nil {
			return err
		}
	}

	_ = os.Symlink(symlinkPath, path)

	_ = os.Chmod(path, mode)

	_ = supportos.SetChownItem(path, uid, gid)

	return nil
}

func (d *Download) createDirectory(path string, mode fs.FileMode, uid int, gid int, atime time.Time, mtime time.Time) error {
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return err
	}

	err = os.Chmod(path, mode)
	if err != nil {
		return err
	}

	_ = supportos.SetChownItem(path, uid, gid)
	err = os.Chtimes(path, atime, mtime)
	if err != nil {
		return err
	}

	return nil
}

func (d *Download) createFile(path string, mode fs.FileMode, uid int, gid int) (*os.File, error) {
	dirName := filepath.Dir(path)
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		if err := os.MkdirAll(dirName, 0700); err != nil {
			return nil, err
		}
	}
	var file *os.File
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	err = os.Chmod(path, mode)
	if err != nil {
		return nil, err
	}

	_ = supportos.SetChownItem(path, uid, gid)

	return file, nil
}

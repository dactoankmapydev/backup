package handle

import (
	"backup-chunk/cache"
	"backup-chunk/storage"

	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/google/uuid"
	"github.com/restic/chunker"
)

const bucket = "backup-hn-1"

type Upload struct {
	Storage storage.S3
}

func walkerDir(dir string, index *cache.Index) error {
	err := filepath.Walk(dir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		node, err := cache.NodeFromFileInfo(dir, path, fi)
		if err != nil {
			return err
		}
		index.Items[path] = node

		if !fi.IsDir() {
			index.TotalFiles++
		}

		return nil
	})
	if err != nil {
		return err
	}

	return nil
}

func (u *Upload) Upload(path string) error {
	recoveryPointID := uuid.New()
	cacheWriter, err := cache.NewRepository(".cache", recoveryPointID.String())
	if err != nil {
		return err
	}

	index := cache.NewIndex(recoveryPointID.String())

	err = walkerDir(path, index)
	if err != nil {
		return err
	}

	for _, itemInfo := range index.Items {
		if itemInfo.Type == "file" {
			file, err := os.Open(itemInfo.AbsolutePath)
			if err != nil {
				return err
			}

			chk := chunker.New(file, 0x3dea92648f6e83)
			buf := make([]byte, 16*1024*1024)
			hash := sha256.New()
			for {
				chunk, err := chk.Next(buf)
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				data := make([]byte, chunk.Length)
				length := copy(data, chunk.Data)
				if uint(length) != chunk.Length {
					return errors.New("copy chunk data error")
				}
				chunkToUpload := cache.ChunkInfo{
					Start:  chunk.Start,
					Length: chunk.Length,
				}
				hash.Write(data)
				itemInfo.Content = append(itemInfo.Content, &chunkToUpload)

				hashData := md5.Sum(data)
				key := hex.EncodeToString(hashData[:])
				chunkToUpload.Etag = key

				fmt.Println("Heat chunk ", key)
				exist, err := u.Storage.HeadObject(bucket, key)
				if aerr, ok := err.(awserr.Error); ok {
					if aerr.Code() == "NotFound" {
						err = nil
					}
				}

				if !exist {
					fmt.Printf("Chunk %s not exist, put chunk\n", key)
					err = u.Storage.PutObject(bucket, key, data)
					if err != nil {
						return err
					}
				} else {
					fmt.Printf("Chunk %s exist, not put chunk\n", key)
				}
			}
			itemInfo.Sha256Hash = hash.Sum(nil)
		}
		err = cacheWriter.SaveIndex(index)
		if err != nil {
			return err
		}
	}

	fmt.Println("Put file index ", recoveryPointID.String())
	err = u.PutIndex(recoveryPointID.String())
	if err != nil {
		return err
	}

	return nil
}

func (u *Upload) PutIndex(recoveryPointID string) error {
	indexPath := filepath.Join(".cache", recoveryPointID, "index.json")

	buf, err := ioutil.ReadFile(indexPath)
	if err != nil {
		return err
	}

	err = u.Storage.PutObject(bucket, filepath.Join(recoveryPointID, "index.json"), buf)
	if err != nil {
		os.RemoveAll(filepath.Join(recoveryPointID))
		return err
	}

	return nil
}

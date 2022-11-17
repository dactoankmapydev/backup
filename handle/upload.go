package handle

import (
	"backup-chunk/cache"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/restic/chunker"
)

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

func Upload(path string) error {
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
			}
			itemInfo.Sha256Hash = hash.Sum(nil)
		}
		err = cacheWriter.SaveIndex(index)
		if err != nil {
			return err
		}
	}

	return nil
}

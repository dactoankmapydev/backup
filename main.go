package main

import (
	"backup-chunk/cache"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/restic/chunker"
)

const (
	uploadPath = "/home/dactoan/Documents/under"
)

func WalkerDir(dir string, index *cache.Index) (int64, error) {
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
		return 0, err
	}

	return index.TotalFiles, nil
}

func backup() error {
	recoveryPointID := uuid.New()
	cacheWriter, err := cache.NewRepository(".cache", recoveryPointID.String())
	if err != nil {
		return err
	}

	index := cache.NewIndex(recoveryPointID.String())

	totalFiles, err := WalkerDir(uploadPath, index)
	if err != nil {
		return err
	}
	fmt.Println(totalFiles)

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
				itemInfo.Content = append(itemInfo.Content, &chunkToUpload)

				hash := md5.Sum(data)
				key := hex.EncodeToString(hash[:])
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

func main() {
	backup()

	// chk := chunker.New(os.Stdin, 0x3dea92648f6e83)
	// buf := make([]byte, 16*1024*1024) // 16 MiB
	// for {
	// 	chunk, err := chk.Next(buf)
	// 	if err == io.EOF {
	// 		break
	// 	}

	// 	if err != nil {
	// 		panic(err)
	// 	}

	// 	hash := sha256.Sum256(chunk.Data)
	// 	fmt.Printf("%d\t%d\t%016x\t%032x\n", chunk.Start, chunk.Length, chunk.Cut, hash)
	// }
}

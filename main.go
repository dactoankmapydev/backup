package main

import (
	"backup-chunk/handle"
	"backup-chunk/storage"
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

func init() {
	if err := godotenv.Load(".env"); err != nil {
		fmt.Println("Not environment variable")
	}
}

func main() {
	accessKey := os.Getenv("ACCESS_KEY_ID")
	secretKey := os.Getenv("SECRET_ACCESS_KEY")
	region := os.Getenv("REGION")

	s3Session := &storage.S3Storage{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Region:    region,
	}
	s3Session.NewS3()

	// backup := handle.Upload{
	// 	Storage: storage.NewImplementS3(s3Session),
	// }
	// backup.Upload("/home/dactoan/Documents/under")

	restore := handle.Download{
		Storage: storage.NewImplementS3(s3Session),
	}
	restore.Download("e18e4b51-52e9-4184-bc68-f44a37bbdf74", "/home/dactoan/restore")
}

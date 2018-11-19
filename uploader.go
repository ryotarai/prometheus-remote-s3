package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type Uploader struct {
	interval  time.Duration
	bucket    string
	keyPrefix string
	buffer    *Buffer
	s3        *s3.S3
}

func NewUploader(interval time.Duration, buffer *Buffer, bucket string, keyPrefix string) *Uploader {
	sess := session.Must(session.NewSession())
	svc := s3.New(sess)

	return &Uploader{
		interval:  interval,
		bucket:    bucket,
		keyPrefix: keyPrefix,
		buffer:    buffer,
		s3:        svc,
	}
}

func (u *Uploader) RunLoop() {
	ticker := time.NewTicker(u.interval)
	for {
		<-ticker.C
		u.Run()
	}
}

func (u *Uploader) Run() {
	path, err := u.buffer.Rotate()
	if err != nil {
		log.Printf("Rotating a file failed: %s", err)
		return
	}

	var compressedPath string
	for {
		compressedPath, err = u.compressFile(path)
		if err == nil {
			break
		}
		log.Printf("Compressing a file failed: %s", err)
		log.Printf("Retrying in 10 sec")
		time.Sleep(time.Second * 10)
	}

	for {
		err = u.uploadFile(compressedPath)
		if err == nil {
			break
		}
		log.Printf("Uploading a file failed: %s", err)
		log.Printf("Retrying in 10 sec")
		time.Sleep(time.Second * 10)
	}

	log.Printf("Uploading succeeded")

	err = u.deleteFile(compressedPath)
	if err != nil {
		log.Printf("Deleting %s failed: %s", compressedPath, err)
	}
}

func (u *Uploader) compressFile(path string) (string, error) {
	log.Printf("Compressing %s", path)
	err := exec.Command("gzip", path).Run()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s.gz", path), nil
}

func (u *Uploader) uploadFile(path string) error {
	log.Printf("Uploading %s", path)

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	key := fmt.Sprintf("%s%s.ltsv.gz", u.keyPrefix, time.Now().UTC().Format("2006/01/02/20060102_150405"))
	log.Printf("PutObject %s", key)
	_, err = u.s3.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(u.bucket),
		Key:    aws.String(key),
		Body:   f,
	})
	if err != nil {
		return err
	}

	return nil
}

func (u *Uploader) deleteFile(path string) error {
	log.Printf("Deleting %s", path)
	return os.Remove(path)
}

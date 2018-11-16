package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"
)

func main() {
	s3Bucket := flag.String("s3-bucket", "", "S3 bucket name")
	s3KeyPrefix := flag.String("s3-key-prefix", "", "S3 key prefix")
	bufferDir := flag.String("buffer-dir", "", "Path to a buffer directory")
	uploadIntervalStr := flag.String("upload-interval", "1h", "Interval duration to upload")
	listen := flag.String("listen", ":8080", "Address to listen on")
	pprof := flag.String("pprof", "", "To enable pprof, pass address to listen such as 'localhost:6060'")
	flag.Parse()

	if *s3Bucket == "" {
		log.Fatal("-s3-bucket is required")
	}

	if *bufferDir == "" {
		log.Fatal("-buffer-dir is required")
	}

	uploadInterval, err := time.ParseDuration(*uploadIntervalStr)
	if err != nil {
		log.Fatal("Upload interval '%s' is not valid", *uploadIntervalStr)
	}

	if *pprof != "" {
		go func() {
			log.Printf("Enabling pprof on %s", *pprof)
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	buffer := NewBuffer(*bufferDir)
	s, err := NewServer(buffer)
	if err != nil {
		log.Fatal(err)
	}

	uploader := NewUploader(uploadInterval, buffer, *s3Bucket, *s3KeyPrefix)
	go uploader.RunLoop()

	log.Printf("Listening %s", *listen)
	err = http.ListenAndServe(*listen, s)
	if err != nil {
		log.Fatal(err)
	}
}

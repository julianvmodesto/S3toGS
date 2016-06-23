package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"
)

var (
	awsProfile   = flag.String("awsProfile", "", "aws profile")
	s3Bucket     = flag.String("s3Bucket", "", "s3 bucket")
	s3Prefix     = flag.String("s3Prefix", "", "s3 prefix")
	localDir     = flag.String("localDir", "", "local directory")
	gcpProjectId = flag.String("gcpProjectId", "", "gcp project id")
	gsBucket     = flag.String("gsBucket", "", "gs bucket")
)

func main() {
	flag.Parse()

	// Set up AWS clients
	sess := session.New(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", *awsProfile),
	})
	s3Client := s3.New(sess)
	s3ListRes, err := s3Client.ListObjects(&s3.ListObjectsInput{
		Bucket: aws.String(*s3Bucket),
		Prefix: aws.String(*s3Prefix),
	})
	if err != nil {
		log.Fatal(err)
		return
	}
	s3Downloader := s3manager.NewDownloader(sess)

	// Set up GCP clients
	ctx := context.Background()
	gsClient, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer gsClient.Close()

	for _, key := range s3ListRes.Contents {
		localFilepath := filepath.Join(*localDir, *key.Key)

		fmt.Println("Downloading", *key.Key, "from S3 to", localFilepath)
		err := os.MkdirAll(filepath.Dir(localFilepath), 0777)
		if err != nil {
			log.Fatal("Failed to create dirs", err)
		}
		file, err := os.Create(localFilepath)
		if err != nil {
			log.Fatal("Failed to create file", err)
		}
		defer file.Close()

		getParams := &s3.GetObjectInput{
			Bucket: aws.String(*s3Bucket),
			Key:    aws.String(*key.Key),
		}
		s3Downloader.Download(file, getParams)

		// Upload to GS
		// https://github.com/golang/build/blob/master/cmd/upload/upload.go
		fmt.Println("Uploading to GS at", *key.Key)
		w := gsClient.Bucket(*gsBucket).Object(*key.Key).NewWriter(ctx)
		var content io.Reader
		content, err = os.Open(file.Name())
		if err != nil {
			log.Fatal(err)
		}
		const maxSlurp = 1 << 20
		var buf bytes.Buffer
		n, err := io.CopyN(&buf, content, maxSlurp)
		if err != nil && err != io.EOF {
			log.Fatalf("Error reading from stdin: %v, %v", n, err)
		}
		w.ContentType = http.DetectContentType(buf.Bytes())
		_, err = io.Copy(w, io.MultiReader(&buf, content))
		if cerr := w.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			log.Fatalf("Write error: %v", err)
		}

		fmt.Println("Removing", file.Name())
		os.Remove(file.Name())
	}

	os.Exit(0)
}

package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"golang.org/x/net/context"
	"google.golang.org/cloud/storage"

	"github.com/pivotal-golang/bytefmt"
)

var (
	awsProfile   = flag.String("awsProfile", "", "aws profile")
	s3Bucket     = flag.String("s3Bucket", "", "s3 bucket")
	s3Prefix     = flag.String("s3Prefix", "", "s3 prefix")
	localDir     = flag.String("localDir", "", "local directory")
	gcpProjectId = flag.String("gcpProjectId", "", "gcp project id")
	gsBucket     = flag.String("gsBucket", "", "gs bucket")
	dryRun       = flag.Bool("dryRun", false, "dry run")
)

type Exit struct{ Code int }

// exit code handler
// http://stackoverflow.com/a/27630092/1881379
func handleExit() {
	if e := recover(); e != nil {
		if exit, ok := e.(Exit); ok == true {
			os.Exit(exit.Code)
		}
		panic(e) // not an Exit, bubble up
	}
	os.Exit(0)
}

// timing
// https://coderwall.com/p/cp5fya/measuring-execution-time-in-go
func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("%s took %s", name, elapsed)
}

func writeToGS(file *os.File, w *storage.Writer) error {
	var content io.Reader
	content, err := os.Open(file.Name())
	if err != nil {
		log.Fatal(err)
		return err
	}
	const maxSlurp = 1 << 20
	var buf bytes.Buffer
	n, err := io.CopyN(&buf, content, maxSlurp)
	if err != nil && err != io.EOF {
		log.Fatalf("Error reading from stdin: %v, %v", n, err)
		return err
	}
	w.ContentType = http.DetectContentType(buf.Bytes())
	_, err = io.Copy(w, io.MultiReader(&buf, content))
	if cerr := w.Close(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		log.Fatalf("Write error: %v", err)
		return err
	}
	return nil
}

func main() {
	defer handleExit()
	defer timeTrack(time.Now(), "S3toGS")

	flag.Parse()

	// Set up AWS clients
	awsSession := session.New(&aws.Config{
		Region:      aws.String("us-east-1"),
		Credentials: credentials.NewSharedCredentials("", *awsProfile),
	})
	s3Client := s3.New(awsSession)
	s3Downloader := s3manager.NewDownloader(awsSession)

	// Set up GCP clients
	gcpContext := context.Background()
	gsClient, err := storage.NewClient(gcpContext)
	if err != nil {
		log.Fatal(err)
		panic(Exit{1})
	}
	defer gsClient.Close()

	// S3 List
	s3List, err := s3Client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(*s3Bucket),
		Prefix: aws.String(*s3Prefix),
	})
	if err != nil {
		log.Fatal(err)
		panic(Exit{1})
	}

	amtTransferred := uint64(0)

	for _, key := range s3List.Contents {
		gsAttrs, gsErr := gsClient.Bucket(*gsBucket).Object(*key.Key).Attrs(gcpContext)

		s3MD5 := strings.Replace(*key.ETag, "\"", "", -1)
		s3Size := *key.Size

		localFilepath := filepath.Join(*localDir, filepath.Base(*key.Key))

		if gsErr != nil || // doesn't exist in GS
			!strings.EqualFold(s3MD5, hex.EncodeToString(gsAttrs.MD5)) ||
			s3Size != gsAttrs.Size {

			if gsErr == nil && strings.EqualFold(s3MD5, hex.EncodeToString(gsAttrs.MD5)) {
				fmt.Println("Hash matches, skipping", *key.Key)
			} else if gsErr == nil && s3Size == gsAttrs.Size {
				fmt.Println("Size matches, skipping", *key.Key)
			} else if *dryRun {
				amtTransferred += uint64(s3Size)
				fmt.Println("Would download/upload", *key.Key)
			} else {
				amtTransferred += uint64(s3Size)

				// Create local file path and file
				err := os.MkdirAll(filepath.Dir(localFilepath), 0777)
				if err != nil {
					log.Fatal("Failed to create dirs", err)
					panic(Exit{1})
				}
				file, err := os.Create(localFilepath)
				if err != nil {
					log.Fatal("Failed to create file", err)
					panic(Exit{1})
				}
				defer file.Close()

				// Download from S3
				fmt.Println("Downloading from S3", *key.Key, "to", localFilepath)
				s3Downloader.Download(file,
					&s3.GetObjectInput{
						Bucket: aws.String(*s3Bucket),
						Key:    aws.String(*key.Key),
					})

				// Upload to GS
				// https://github.com/golang/build/blob/master/cmd/upload/upload.go
				fmt.Println("Uploading", localFilepath, "to GS at", *key.Key)
				w := gsClient.Bucket(*gsBucket).Object(*key.Key).NewWriter(gcpContext)
				writeToGS(file, w)

				// Delete local file
				fmt.Println("Removing", file.Name())
				os.Remove(file.Name())

				gsAttrs, gsErr := gsClient.Bucket(*gsBucket).Object(*key.Key).Attrs(gcpContext)
				if gsErr != nil || s3Size != gsAttrs.Size {
					log.Fatal("Upload failed")
					panic(Exit{1})
				}
			}
		} else {
			fmt.Println("Already in GS, skipping", *key.Key)
		}
	}

	fmt.Println("Amount transferred", bytefmt.ByteSize(amtTransferred))
}

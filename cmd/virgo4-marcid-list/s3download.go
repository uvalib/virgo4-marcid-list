package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	//"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var downloader *s3manager.Downloader
var ErrBadName = fmt.Errorf("bad file name")

// set up our S3 management objects
func init() {

	sess, err := session.NewSession()
	if err == nil {
		downloader = s3manager.NewDownloader(sess)
	}
}

// taken from https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/go/example_code/s3/s3_download_object.go

func s3download(downloadDir string, sourcename string) (string, error) {

	file, err := ioutil.TempFile(downloadDir, "")
	if err != nil {
		return "", err
	}
	defer file.Close()

	log.Printf("INFO: downloading %s to %s", sourcename, file.Name())

	// remove the prefix
	str := strings.Replace(sourcename, "s3://", "", 1)
	tokens := strings.Split(str, "/")
	if len(tokens) < 2 {
		return "", ErrBadName
	}

	bucket := tokens[0]
	object := strings.Replace(str, fmt.Sprintf("%s/", bucket), "", 1)

	//start := time.Now()
	_, err = downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(object),
		})

	if err != nil {
		return "", err
	}

	//duration := time.Since(start)
	//log.Printf("INFO: download of %s complete in %0.2f seconds (%d bytes, %0.2f bytes/sec)", sourcename, duration.Seconds(), fileSize, float64(fileSize)/duration.Seconds())
	return file.Name(), nil
}

//
// end of file
//

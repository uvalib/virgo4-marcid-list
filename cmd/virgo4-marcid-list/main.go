package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/uvalib/uva-aws-s3-sdk/uva-s3"
)

var downloadDir = "/tmp"
var ErrBadName = fmt.Errorf("bad file name")

//
// main entry point
//
func main() {

	cfg := LoadConfiguration()
	localName := cfg.InFileName

	var err error

	if cfg.MustDownload == true {

		// load our AWS s3 helper object
		s3Svc, err := uva_s3.NewUvaS3(uva_s3.UvaS3Config{Logging: true})
		if err != nil {
			log.Fatal(err)
		}

		// create temp file
		tmp, err := ioutil.TempFile(downloadDir, "")
		if err != nil {
			log.Fatal(err)
		}
		tmp.Close()
		localName = tmp.Name()

		// download the file
		bucket, key := s3Split(cfg.InFileName)
		o := uva_s3.NewUvaS3Object(bucket, key)
		err = s3Svc.GetToFile(o, localName)
		if err != nil {
			log.Fatal(err)
		}
	}

	loader, err := NewRecordLoader(cfg.InFileName, localName)
	if err != nil {
		log.Fatal(err)
	}

	rec, err := loader.First(true)
	for {
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}

		id, err := rec.Id()
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("%s\n", id)
		rec, err = loader.Next(true)
		if err == io.EOF {
			break
		}
	}

	loader.Done()

	// remove the downloaded file if necessary
	if cfg.MustDownload == true {
		_ = os.Remove(localName)
	}
}

func s3Split(s3Name string) (string, string) {

	// remove the prefix
	str := strings.Replace(s3Name, "s3://", "", 1)

	// split the rest
	tokens := strings.Split(str, "/")
	if len(tokens) < 2 {
		log.Fatal(ErrBadName)
	}

	bucket := tokens[0]
	key := strings.Replace(str, fmt.Sprintf("%s/", bucket), "", 1)
	return bucket, key
}

//
// end of file
//

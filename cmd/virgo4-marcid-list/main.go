package main

import (
	"fmt"
	"io"
	"log"
	"os"
)

var downloadDir = "/tmp"

//
// main entry point
//
func main() {

	cfg := LoadConfiguration()
	localName := cfg.InFileName

	var err error
	if cfg.MustDownload == true {
		localName, err = s3download(downloadDir, cfg.InFileName)
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

//
// end of file
//

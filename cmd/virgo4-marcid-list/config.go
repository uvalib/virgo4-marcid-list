package main

import (
	"flag"
	"log"
	"strings"
)

// ServiceConfig defines all of the service configuration parameters
type ServiceConfig struct {
	InFileName   string
	MustDownload bool
}

// LoadConfiguration will load the service configuration from env/cmdline
// and return a pointer to it. Any failures are fatal.
func LoadConfiguration() *ServiceConfig {

	var cfg ServiceConfig

	flag.StringVar(&cfg.InFileName, "infile", "", "Input file name")

	flag.Parse()

	if len(cfg.InFileName) == 0 {
		log.Fatalf("InFileName cannot be blank")
	}

	if strings.HasPrefix(cfg.InFileName, "s3://") {
		cfg.MustDownload = true
	}
	return &cfg
}

//
// end of file
//

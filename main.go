package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/erick-otenyo/geotiff-clip/utils"
)

func main() {

	filePath := flag.String("f", "", "File Path")
	geomPath := flag.String("g", "", "Geom Path")
	outPath := flag.String("o", "", "Out Path")

	flag.Parse()

	if *filePath == "" {
		msg := fmt.Errorf("file required")
		log.Fatal(msg)
	}

	if *geomPath == "" {
		msg := fmt.Errorf("geojson required")
		log.Fatal(msg)
	}
	if *outPath == "" {
		msg := fmt.Errorf("out File required")
		log.Fatal(msg)
	}

	utils.InitGdal()

	err := Clip(*filePath, *geomPath, *outPath)
	if err != nil {
		log.Fatal(err)
	}
}

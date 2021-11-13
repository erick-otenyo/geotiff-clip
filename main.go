package main

import (
	"flag"
	"fmt"
	"log"
	"time"

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

	start := time.Now()

	utils.InitGdal()

	// parse the geojson file
	featureCol, err := ParseGeojson(*geomPath)

	if err != nil {
		log.Fatal(err)
	}

	// take the first feature
	feat := featureCol.Features[0]

	//read data within geom
	geomData, err := ReadDataWithinGeom(feat, *filePath)
	if err != nil {
		log.Fatal(err)
	}

	err = WriteDataToGeoTiff(geomData, *outPath)

	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)

	fmt.Printf("Done. Took %v\n", elapsed)
}

// /Users/erickotenyo/Downloads/Data/ICPAC/LandCover/3974442.geojson

// /Users/erickotenyo/Downloads/Data/ICPAC/LandCover/land_cover_esa_10m/ESA_WorldCover_10m_2020_v100_N15E039_Map.tif

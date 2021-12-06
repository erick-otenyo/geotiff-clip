package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/erick-otenyo/geotiff-clip/utils"
)

func main() {

	// filePath := flag.String("f", "", "File Path")
	geomPath := flag.String("g", "", "Geom Path")
	// outPath := flag.String("o", "", "Out Path")

	flag.Parse()

	// if *filePath == "" {
	// 	msg := fmt.Errorf("file required")
	// 	log.Fatal(msg)
	// }

	if *geomPath == "" {
		msg := fmt.Errorf("geojson required")
		log.Fatal(msg)
	}

	// if *outPath == "" {
	// 	msg := fmt.Errorf("out File required")
	// 	log.Fatal(msg)
	// }

	start := time.Now()

	utils.InitGdal()

	// parse the geojson file
	featureCol, err := ParseGeojson(*geomPath)

	if err != nil {
		log.Fatal(err)
	}

	// take the first feature
	feat := featureCol.Features[0]

	// //read data within geom
	// geomData, err := ReadDataWithinGeom(feat, *filePath)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// maskData, err := CreatGeomMask(feat, *filePath, true)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// // fmt.Println(len(geomData.Data.([]float32)))

	// err = WriteDataToGeoTiff(maskData, *outPath)

	// if err != nil {
	// 	log.Fatal(err)
	// }

	// BBox: geoReq.BBox, Height: geoReq.Height, Width: geoReq.Width, OffX: geoReq.OffX, OffY: geoReq.OffY, CRS: geoReq.CRS

	geot := []float64{4.383204949985147e+06, 2445.9849051256388, 0, 626172.1357121649, 0, -2445.984905125644}

	err = CreatVisMemGeomMask(feat, "EPSG:3857", 256, 256, geot)

	if err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start)

	fmt.Printf("Done. Took %v\n", elapsed)
}

// /Users/erickotenyo/Downloads/Data/ICPAC/LandCover/3974442.geojson

// /Users/erickotenyo/Downloads/Data/ICPAC/LandCover/land_cover_esa_10m/ESA_WorldCover_10m_2020_v100_N15E039_Map.tif

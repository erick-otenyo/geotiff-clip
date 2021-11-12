package main

// #include "gdal.h"
// #include "gdal_alg.h"
// #include "ogr_api.h"
// #include "ogr_srs_api.h"
// #include "cpl_string.h"
// #cgo pkg-config: gdal
import "C"

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"unsafe"

	geo "github.com/nci/geometry"
)

type ClipFileDescriptor struct {
	SrcBBox []int32
	DstBBox []int32
	Mask    []uint8
	SrcGeot []float64
	DstGeot []float64
}

var cWGS84WKT = C.CString(`GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9108"]],AUTHORITY["EPSG","4326"]]","proj4":"+proj=longlat +ellps=WGS84 +towgs84=0,0,0,0,0,0,0 +no_defs `)

func Clip(filePath string, geojsonPath string, outPath string) error {

	geojsonFile, err := os.Open(geojsonPath)

	if err != nil {
		return err
	}

	defer geojsonFile.Close()

	byteValue, _ := ioutil.ReadAll(geojsonFile)

	var featCol geo.FeatureCollection
	err = json.Unmarshal(byteValue, &featCol)

	if err != nil {
		return fmt.Errorf("problem unmarshalling geometry")
	}

	feat := featCol.Features[0]

	geomGeoJSON, err := json.Marshal(feat.Geometry)

	if err != nil {
		return fmt.Errorf("problem marshaling GeoJSON geometry: %v", err)
	}

	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))
	ds := C.GDALOpen(cPath, C.GDAL_OF_READONLY)
	if ds == nil {
		return fmt.Errorf("gdal could not open dataset: %s", filePath)
	}
	defer C.GDALClose(ds)

	cGeom := C.CString(string(geomGeoJSON))
	defer C.free(unsafe.Pointer(cGeom))
	geom := C.OGR_G_CreateGeometryFromJson(cGeom)
	if geom == nil {
		return fmt.Errorf("geometry could not be parsed")
	}

	selSRS := C.OSRNewSpatialReference(cWGS84WKT)
	defer C.OSRDestroySpatialReference(selSRS)

	C.OGR_G_AssignSpatialReference(geom, selSRS)

	readData(ds, geom, outPath)

	return nil
}

func readData(ds C.GDALDatasetH, geom C.OGRGeometryH, outPath string) error {

	// get mask
	dsDscr, err := getDrillFileDescriptor(ds, geom, 0, 0)

	if err != nil {
		return err
	}

	// it is safe to assume all data bands have same data type and nodata value
	bandH := C.GDALGetRasterBand(ds, C.int(1))
	dType := C.GDALGetRasterDataType(bandH)

	dSize := C.GDALGetDataTypeSizeBytes(dType)
	if dSize == 0 {
		return fmt.Errorf("gdal data type not implemented")
	}

	nodata := float32(C.GDALGetRasterNoDataValue(bandH, nil))

	// band 1
	bandsRead := []int32{1}

	// read band
	dataBuf := make([]float32, dsDscr.DstBBox[2]*dsDscr.DstBBox[3]*int32(1))
	C.GDALDatasetRasterIO(ds, C.GF_Read, C.int(dsDscr.SrcBBox[0]), C.int(dsDscr.SrcBBox[1]), C.int(dsDscr.SrcBBox[2]), C.int(dsDscr.SrcBBox[3]), unsafe.Pointer(&dataBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Float32, C.int(1), (*C.int)(unsafe.Pointer(&bandsRead[0])), 0, 0, 0)

	bandSize := int(dsDscr.DstBBox[2] * dsDscr.DstBBox[3])

	// ouput data
	dataOutBuf := make([]float32, dsDscr.DstBBox[2]*dsDscr.DstBBox[3]*int32(1))

	// reading of the non masked pixels
	for i := 0; i < bandSize; i++ {
		var val float32
		if dsDscr.Mask[i] == 255 {
			val = dataBuf[i]
		} else {
			// this is outside the geometry. Write nodata
			val = nodata
		}

		dataOutBuf[i] = val
	}

	var geot []float64

	// if dsDscr.DstGeot > 1, it means we have a geometry mask. Otherwise no clipping was
	// necessary as the data is completely within the geometry
	if len(dsDscr.DstGeot) > 1 {
		geot = dsDscr.DstGeot
	} else {
		geot = dsDscr.SrcGeot
	}

	// creating clipped file
	driverStr := C.CString("GTiff")
	defer C.free(unsafe.Pointer(driverStr))
	hDriver := C.GDALGetDriverByName(driverStr)

	outFileC := C.CString(outPath)
	defer C.free(unsafe.Pointer(outFileC))

	var driverOptions []*C.char

	// driver options: NOTE research more details on this. Right now these were picked from GSKY

	driverOptions = append(driverOptions, C.CString("COMPRESS=PACKBITS"))
	driverOptions = append(driverOptions, C.CString("TILED=YES"))
	driverOptions = append(driverOptions, C.CString("BIGTIFF=YES"))
	driverOptions = append(driverOptions, C.CString("INTERLEAVE=BAND"))

	driverOptions = append(driverOptions, nil)

	hDs := C.GDALCreate(hDriver, outFileC, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), 1, C.GDT_Float32, &driverOptions[0])

	if hDs == nil {
		os.Remove(outPath)
		return fmt.Errorf("error creating raster")
	}

	// Set projection
	hSRS := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(hSRS)
	C.OSRImportFromEPSG(hSRS, C.int(4326))
	var projWKT *C.char
	defer C.free(unsafe.Pointer(projWKT))
	C.OSRExportToWkt(hSRS, &projWKT)
	C.GDALSetProjection(hDs, projWKT)

	hBand := C.GDALGetRasterBand(hDs, C.int(1))
	gerr := C.CPLErr(0)

	// Set geotransform
	C.GDALSetGeoTransform(hDs, (*C.double)(&geot[0]))

	// set no data value
	C.GDALSetRasterNoDataValue(hBand, C.double(nodata))

	if gerr != 0 {
		return fmt.Errorf("Error")
	}

	// write band data
	gerr = C.GDALRasterIO(hBand, C.GF_Write, 0, 0, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), unsafe.Pointer(&dataOutBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Float32, 0, 0)

	if gerr != 0 {
		C.GDALClose(hDs)
		return fmt.Errorf("rrror writing raster band")
	}

	// close dataset
	C.GDALClose(hDs)

	return nil
}

func createMask(ds C.GDALDatasetH, g C.OGRGeometryH, geoTrans []float64, bbox []int32) ([]uint8, error) {
	canvas := make([]uint8, bbox[2]*bbox[3])

	memStr := fmt.Sprintf("MEM:::DATAPOINTER=%d,PIXELS=%d,LINES=%d,DATATYPE=Byte", unsafe.Pointer(&canvas[0]), bbox[2], bbox[3])
	memStrC := C.CString(memStr)
	defer C.free(unsafe.Pointer(memStrC))
	hDstDS := C.GDALOpen(memStrC, C.GA_Update)
	if hDstDS == nil {
		return nil, fmt.Errorf("couldn't create memory driver")
	}
	defer C.GDALClose(hDstDS)

	var gdalErr C.CPLErr
	if gdalErr = C.GDALSetProjection(hDstDS, C.GDALGetProjectionRef(ds)); gdalErr != 0 {
		return nil, fmt.Errorf("couldn't set a projection in the mem raster %v", gdalErr)
	}

	if gdalErr = C.GDALSetGeoTransform(hDstDS, (*C.double)(&geoTrans[0])); gdalErr != 0 {
		return nil, fmt.Errorf("couldn't set the geotransform on the destination dataset %v", gdalErr)
	}

	ic := C.OGR_G_Clone(g)
	defer C.OGR_G_DestroyGeometry(ic)

	geomBurnValue := C.double(255)
	panBandList := []C.int{C.int(1)}
	pahGeomList := []C.OGRGeometryH{ic}

	opts := []*C.char{C.CString("ALL_TOUCHED=TRUE"), nil}
	defer C.free(unsafe.Pointer(opts[0]))

	if gdalErr = C.GDALRasterizeGeometries(hDstDS, 1, &panBandList[0], 1, &pahGeomList[0], nil, nil, &geomBurnValue, &opts[0], nil, nil); gdalErr != 0 {
		return nil, fmt.Errorf("GDALRasterizeGeometry error %v", gdalErr)
	}

	return canvas, nil
}

func envelopePolygon(hDS C.GDALDatasetH, xSize float64, ySize float64) (C.OGRGeometryH, error) {
	geoTrans := make([]float64, 6)
	C.GDALGetGeoTransform(hDS, (*C.double)(&geoTrans[0]))

	var ulX, ulY C.double
	C.GDALApplyGeoTransform((*C.double)(&geoTrans[0]), C.double(0), C.double(0), &ulX, &ulY)
	var lrX, lrY C.double
	C.GDALApplyGeoTransform((*C.double)(&geoTrans[0]), C.double(xSize), C.double(ySize), &lrX, &lrY)

	polyWKT := fmt.Sprintf("POLYGON ((%f %f,%f %f,%f %f,%f %f,%f %f))", ulX, ulY,
		ulX, lrY,
		lrX, lrY,
		lrX, ulY,
		ulX, ulY)

	ppszData := C.CString(polyWKT)
	ppszDataTmp := ppszData

	var hGeom C.OGRGeometryH
	hSRS := C.OSRNewSpatialReference(C.GDALGetProjectionRef(hDS))

	// OGR_G_CreateFromWkt intrnally updates &ppszData pointer value
	errC := C.OGR_G_CreateFromWkt(&ppszData, hSRS, &hGeom)

	C.OSRRelease(hSRS)
	C.free(unsafe.Pointer(ppszDataTmp))

	if errC != C.OGRERR_NONE {
		return nil, fmt.Errorf("failed to compute envelope polygon: %v", polyWKT)
	}

	return hGeom, nil
}

func getDrillFileDescriptor(ds C.GDALDatasetH, g C.OGRGeometryH, rasterXSize float64, rasterYSize float64) (*ClipFileDescriptor, error) {
	gCopy := C.OGR_G_Buffer(g, C.double(0.0), C.int(30))
	if C.OGR_G_IsEmpty(gCopy) == C.int(1) {
		gCopy = C.OGR_G_Clone(g)
	}

	defer C.OGR_G_DestroyGeometry(gCopy)

	if C.GoString(C.GDALGetProjectionRef(ds)) != "" {
		desSRS := C.OSRNewSpatialReference(C.GDALGetProjectionRef(ds))
		defer C.OSRDestroySpatialReference(desSRS)
		srcSRS := C.OSRNewSpatialReference(cWGS84WKT)
		defer C.OSRDestroySpatialReference(srcSRS)
		C.OSRSetAxisMappingStrategy(srcSRS, C.OAMS_TRADITIONAL_GIS_ORDER)
		C.OSRSetAxisMappingStrategy(desSRS, C.OAMS_TRADITIONAL_GIS_ORDER)
		trans := C.OCTNewCoordinateTransformation(srcSRS, desSRS)
		C.OGR_G_Transform(gCopy, trans)
		C.OCTDestroyCoordinateTransformation(trans)
	}

	xSize := float64(C.GDALGetRasterXSize(ds))
	ySize := float64(C.GDALGetRasterYSize(ds))

	fileEnv, err := envelopePolygon(ds, xSize, ySize)
	if err != nil {
		return nil, err
	}
	defer C.OGR_G_DestroyGeometry(fileEnv)

	inters := C.OGR_G_Intersection(gCopy, fileEnv)
	defer C.OGR_G_DestroyGeometry(inters)

	var env C.OGREnvelope
	C.OGR_G_GetEnvelope(inters, &env)

	geot := make([]float64, 6)
	gdalErr := C.GDALGetGeoTransform(ds, (*C.double)(&geot[0]))
	if gdalErr != 0 {
		return nil, fmt.Errorf("couldn't get the geotransform from the source dataset %v", gdalErr)
	}

	srcBBox, err := getPixelLineBBox(geot, &env)
	if err != nil {
		return nil, err
	}

	if srcBBox[0] >= int32(xSize) {
		srcBBox[0] = int32(xSize) - 1
	}
	if srcBBox[2]+srcBBox[0] >= int32(xSize) {
		srcBBox[2] = int32(xSize) - srcBBox[0]
	}

	if srcBBox[1] >= int32(ySize) {
		srcBBox[1] = int32(ySize) - 1
	}
	if srcBBox[3]+srcBBox[1] >= int32(ySize) {
		srcBBox[3] = int32(ySize) - srcBBox[1]
	}

	if srcBBox[2] <= 3 && srcBBox[3] <= 3 {
		numPixels := int(srcBBox[2] * srcBBox[3])
		mask := make([]uint8, numPixels)
		for i := 0; i < numPixels; i++ {
			mask[i] = 255
		}
		return &ClipFileDescriptor{srcBBox, srcBBox, mask, geot, []float64{}}, nil
	}

	dstGeot := make([]float64, len(geot))
	copy(dstGeot, geot)

	dstGeot[0] += dstGeot[1] * float64(srcBBox[0])
	dstGeot[3] += dstGeot[5] * float64(srcBBox[1])

	if srcBBox[2] <= 3 || srcBBox[3] <= 3 {
		rasterXSize = 0
		rasterYSize = 0
	}

	hasNewGeot := false
	if (rasterXSize > 0 && rasterXSize <= 1) || (rasterYSize > 0 && rasterYSize <= 1) {
		dstXSize := xSize
		dstYSize := ySize

		if rasterXSize > 0 && rasterXSize <= 1 {
			dstXSize = float64(int(xSize*rasterXSize + 0.5))
		}

		if rasterYSize > 0 && rasterYSize <= 1 {
			dstYSize = float64(int(ySize*rasterYSize + 0.5))
		}

		if rasterXSize <= 0 && rasterYSize > 0 {
			dstXSize = float64(int(float64(dstYSize)*xSize/ySize + 0.5))
		} else if rasterXSize > 0 && rasterYSize <= 0 {
			dstYSize = float64(int(float64(dstXSize)*ySize/xSize + 0.5))
		}

		if dstXSize < 1 {
			dstXSize = 1
		}

		if dstYSize < 1 {
			dstYSize = 1
		}

		dstGeot[1] *= xSize / dstXSize
		dstGeot[5] *= ySize / dstYSize

		hasNewGeot = true
	} else if rasterXSize > 1 || rasterYSize > 1 {
		var dstEnv C.OGREnvelope
		C.OGR_G_GetEnvelope(gCopy, &dstEnv)

		envMinX := float64(dstEnv.MinX)
		envMaxX := float64(dstEnv.MaxX)
		envMinY := float64(dstEnv.MinY)
		envMaxY := float64(dstEnv.MaxY)

		var dstXSize, dstYSize float64
		if rasterXSize > 1 && rasterYSize > 1 {
			dstXSize = rasterXSize
			dstYSize = rasterYSize
		} else {
			var dstSize float64
			if rasterXSize > 1 {
				dstSize = rasterXSize
			} else {
				dstSize = rasterYSize
			}

			if xSize >= ySize {
				dstXSize = dstSize
				rasterXSize = dstSize
				rasterYSize = 0
			} else {
				dstYSize = dstSize
				rasterXSize = 0
				rasterYSize = dstSize
			}
		}

		if rasterXSize > 1 && rasterYSize <= 0 {
			dstYSize = rasterXSize * ySize / xSize
		}

		if rasterYSize > 1 && rasterXSize <= 0 {
			dstXSize = rasterYSize * xSize / ySize
		}

		if dstXSize < 1 {
			dstXSize = 1
		}

		if dstYSize < 1 {
			dstYSize = 1
		}

		xRes := (envMaxX - envMinX) / dstXSize
		if xRes > math.Abs(geot[1]) {
			dstGeot[1] = math.Copysign(xRes, geot[1])
			hasNewGeot = true
		}

		yRes := (envMaxY - envMinY) / dstYSize
		if yRes > math.Abs(geot[5]) {
			dstGeot[5] = math.Copysign(yRes, geot[5])
			hasNewGeot = true
		}

	} else if rasterXSize > 0 || rasterYSize > 0 {
		return nil, fmt.Errorf("unsupported raster size: %v, %v", rasterXSize, rasterYSize)
	}

	var dstBBox []int32
	if !hasNewGeot {
		dstBBox = srcBBox
	} else {
		dstBBox, err = getPixelLineBBox(dstGeot, &env)
		if err != nil {
			dstBBox = srcBBox
		}
	}

	//log.Printf("xSize:%v, ySize:%v, srcBBox:%v, dstBBox:%v, srcGeot:%v, dstGeot:%v",
	//  xSize, ySize, srcBBox, dstBBox, geot, dstGeot)
	mask, err := createMask(ds, gCopy, dstGeot, dstBBox)
	return &ClipFileDescriptor{srcBBox, dstBBox, mask, geot, dstGeot}, err
}

func getPixelLineBBox(geot []float64, env *C.OGREnvelope) ([]int32, error) {
	invGeot := make([]float64, 6)
	gdalErr := C.GDALInvGeoTransform((*C.double)(&geot[0]), (*C.double)(&invGeot[0]))
	if gdalErr == C.int(0) {
		return nil, fmt.Errorf("invert geo transform failed")
	}

	var offMinXC, offMinYC, offMaxXC, offMaxYC C.double
	C.GDALApplyGeoTransform((*C.double)(&invGeot[0]), env.MinX, env.MinY, &offMinXC, &offMinYC)
	C.GDALApplyGeoTransform((*C.double)(&invGeot[0]), env.MaxX, env.MaxY, &offMaxXC, &offMaxYC)

	offMinX := math.Min(float64(offMinXC), float64(offMaxXC))
	offMaxX := math.Max(float64(offMinXC), float64(offMaxXC))
	offMinY := math.Min(float64(offMinYC), float64(offMaxYC))
	offMaxY := math.Max(float64(offMinYC), float64(offMaxYC))

	offsetX := int32(offMinX + 0.5)
	offsetY := int32(offMinY + 0.5)
	countX := int32(offMaxX - offMinX + 0.5)
	countY := int32(offMaxY - offMinY + 0.5)

	if countX == 0 {
		countX++
	}
	if countY == 0 {
		countY++
	}
	if offsetX < 0 {
		offsetX = 0
	}
	if offsetY < 0 {
		offsetY = 0
	}

	return []int32{offsetX, offsetY, countX, countY}, nil
}

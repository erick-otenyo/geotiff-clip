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
	"time"
	"unsafe"

	geo "github.com/nci/geometry"

	"github.com/erick-otenyo/geotiff-clip/utils"
)

var cWGS85MercatorWKT = C.CString(`PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Mercator_1SP"],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH],EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0 +lon_0=0 +x_0=0 +y_0=0 +k=1 +units=m +nadgrids=@null +wktext +no_defs"],AUTHORITY["EPSG","3857"]]`)

const ISOFormat = "2006-01-02T15:04:05.000Z"

type ClipFileDescriptor struct {
	SrcBBox []int32
	DstBBox []int32
	Mask    []uint8
	SrcGeot []float64
	DstGeot []float64
}

type GeomRasterData struct {
	DataType    string
	Data        interface{}
	ClipDescr   ClipFileDescriptor
	NoDataValue float64
}

type DatasetAxis struct {
	Name               string    `json:"name"`
	Params             []float64 `json:"params"`
	Strides            []int     `json:"strides"`
	Shape              []int     `json:"shape"`
	Grid               string    `json:"grid"`
	IntersectionIdx    []int
	IntersectionValues []float64
	Order              int
	Aggregate          int
}

type GeoLocInfo struct {
	XDSName     string `json:"x_ds_name"`
	XBand       int    `json:"x_band"`
	YDSName     string `json:"y_ds_name"`
	YBand       int    `json:"y_band"`
	LineOffset  int    `json:"line_offset"`
	PixelOffset int    `json:"pixel_offset"`
	LineStep    int    `json:"line_step"`
	PixelStep   int    `json:"pixel_step"`
}

type GDALDataset struct {
	RawPath      string         `json:"file_path"`
	DSName       string         `json:"ds_name"`
	NameSpace    string         `json:"namespace"`
	ArrayType    string         `json:"array_type"`
	SRS          string         `json:"srs"`
	GeoTransform []float64      `json:"geo_transform"`
	TimeStamps   []time.Time    `json:"timestamps"`
	Polygon      string         `json:"polygon"`
	Means        []float64      `json:"means"`
	SampleCounts []int          `json:"sample_counts"`
	NoData       float64        `json:"nodata"`
	Axes         []*DatasetAxis `json:"axes"`
	GeoLocation  *GeoLocInfo    `json:"geo_loc"`
	IsOutRange   bool
}

type MetadataResponse struct {
	Error        string         `json:"error"`
	GDALDatasets []*GDALDataset `json:"gdal"`
}

var cWGS84WKT = C.CString(`GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],TOWGS84[0,0,0,0,0,0,0],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9108"]],AUTHORITY["EPSG","4326"]]","proj4":"+proj=longlat +ellps=WGS84 +towgs84=0,0,0,0,0,0,0 +no_defs `)

func ParseGeojson(filePath string) (geo.FeatureCollection, error) {

	var featureCol geo.FeatureCollection

	geojsonFile, err := os.Open(filePath)

	if err != nil {
		return featureCol, err
	}

	defer geojsonFile.Close()

	byteValue, _ := ioutil.ReadAll(geojsonFile)

	err = json.Unmarshal(byteValue, &featureCol)

	if err != nil {
		return featureCol, fmt.Errorf("problem unmarshalling geometry")
	}

	return featureCol, nil
}

func ReadDataWithinGeom(feature geo.Feature, filePath string) (*GeomRasterData, error) {

	geomGeoJSON, err := json.Marshal(feature.Geometry)

	if err != nil {
		return nil, fmt.Errorf("problem marshaling GeoJSON geometry: %v", err)
	}

	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))
	ds := C.GDALOpen(cPath, C.GDAL_OF_READONLY)
	if ds == nil {
		return nil, fmt.Errorf("gdal could not open dataset: %s", filePath)
	}
	defer C.GDALClose(ds)

	cGeom := C.CString(string(geomGeoJSON))
	defer C.free(unsafe.Pointer(cGeom))
	geom := C.OGR_G_CreateGeometryFromJson(cGeom)
	if geom == nil {
		return nil, fmt.Errorf("geometry could not be parsed")
	}

	selSRS := C.OSRNewSpatialReference(cWGS84WKT)
	defer C.OSRDestroySpatialReference(selSRS)

	C.OGR_G_AssignSpatialReference(geom, selSRS)

	// get mask
	dsDscr, err := getDrillFileDescriptor(ds, geom, 0, 0)

	if err != nil {
		return nil, err
	}

	// it is safe to assume all data bands have same data type and nodata value
	bandH := C.GDALGetRasterBand(ds, C.int(1))
	dType := C.GDALGetRasterDataType(bandH)
	noData := float64(C.GDALGetRasterNoDataValue(bandH, nil))

	dSize := C.GDALGetDataTypeSizeBytes(dType)
	if dSize == 0 {
		return nil, fmt.Errorf("gdal data type not implemented")
	}

	dTypeName := utils.GetDataType(int(dType))

	if dTypeName == "" {
		return nil, fmt.Errorf("gdal data type not implemented")
	}

	geomRasterData := &GeomRasterData{
		DataType:    dTypeName,
		ClipDescr:   *dsDscr,
		NoDataValue: noData,
	}

	// band 1
	bandsRead := []int32{1}

	// read band
	switch dTypeName {
	case "Byte":
		dataBuf := make([]uint8, dsDscr.DstBBox[2]*dsDscr.DstBBox[3]*int32(1))
		C.GDALDatasetRasterIO(ds, C.GF_Read, C.int(dsDscr.SrcBBox[0]), C.int(dsDscr.SrcBBox[1]), C.int(dsDscr.SrcBBox[2]), C.int(dsDscr.SrcBBox[3]), unsafe.Pointer(&dataBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Byte, C.int(1), (*C.int)(unsafe.Pointer(&bandsRead[0])), 0, 0, 0)
		geomRasterData.Data = dataBuf
	case "UInt16":
		dataBuf := make([]uint16, dsDscr.DstBBox[2]*dsDscr.DstBBox[3]*int32(1))
		C.GDALDatasetRasterIO(ds, C.GF_Read, C.int(dsDscr.SrcBBox[0]), C.int(dsDscr.SrcBBox[1]), C.int(dsDscr.SrcBBox[2]), C.int(dsDscr.SrcBBox[3]), unsafe.Pointer(&dataBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_UInt16, C.int(1), (*C.int)(unsafe.Pointer(&bandsRead[0])), 0, 0, 0)
		geomRasterData.Data = dataBuf
	case "Int16":
		dataBuf := make([]int16, dsDscr.DstBBox[2]*dsDscr.DstBBox[3]*int32(1))
		C.GDALDatasetRasterIO(ds, C.GF_Read, C.int(dsDscr.SrcBBox[0]), C.int(dsDscr.SrcBBox[1]), C.int(dsDscr.SrcBBox[2]), C.int(dsDscr.SrcBBox[3]), unsafe.Pointer(&dataBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Int16, C.int(1), (*C.int)(unsafe.Pointer(&bandsRead[0])), 0, 0, 0)
		geomRasterData.Data = dataBuf
	case "UInt32":
		dataBuf := make([]uint32, dsDscr.DstBBox[2]*dsDscr.DstBBox[3]*int32(1))
		C.GDALDatasetRasterIO(ds, C.GF_Read, C.int(dsDscr.SrcBBox[0]), C.int(dsDscr.SrcBBox[1]), C.int(dsDscr.SrcBBox[2]), C.int(dsDscr.SrcBBox[3]), unsafe.Pointer(&dataBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_UInt32, C.int(1), (*C.int)(unsafe.Pointer(&bandsRead[0])), 0, 0, 0)
		geomRasterData.Data = dataBuf
	case "Int32":
		dataBuf := make([]int32, dsDscr.DstBBox[2]*dsDscr.DstBBox[3]*int32(1))
		C.GDALDatasetRasterIO(ds, C.GF_Read, C.int(dsDscr.SrcBBox[0]), C.int(dsDscr.SrcBBox[1]), C.int(dsDscr.SrcBBox[2]), C.int(dsDscr.SrcBBox[3]), unsafe.Pointer(&dataBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Int32, C.int(1), (*C.int)(unsafe.Pointer(&bandsRead[0])), 0, 0, 0)
		geomRasterData.Data = dataBuf
	case "Float32":
		dataBuf := make([]float32, dsDscr.DstBBox[2]*dsDscr.DstBBox[3]*int32(1))
		C.GDALDatasetRasterIO(ds, C.GF_Read, C.int(dsDscr.SrcBBox[0]), C.int(dsDscr.SrcBBox[1]), C.int(dsDscr.SrcBBox[2]), C.int(dsDscr.SrcBBox[3]), unsafe.Pointer(&dataBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Float32, C.int(1), (*C.int)(unsafe.Pointer(&bandsRead[0])), 0, 0, 0)
		geomRasterData.Data = dataBuf
	case "Float64":
		dataBuf := make([]float64, dsDscr.DstBBox[2]*dsDscr.DstBBox[3]*int32(1))
		C.GDALDatasetRasterIO(ds, C.GF_Read, C.int(dsDscr.SrcBBox[0]), C.int(dsDscr.SrcBBox[1]), C.int(dsDscr.SrcBBox[2]), C.int(dsDscr.SrcBBox[3]), unsafe.Pointer(&dataBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Float64, C.int(1), (*C.int)(unsafe.Pointer(&bandsRead[0])), 0, 0, 0)
		geomRasterData.Data = dataBuf
	default:
		return nil, fmt.Errorf("gdal data type not implemented")
	}

	return geomRasterData, nil
}

func CreatGeomMask(feature geo.Feature, filePath string, inverse bool) (*GeomRasterData, error) {

	geomGeoJSON, err := json.Marshal(feature.Geometry)

	if err != nil {
		return nil, fmt.Errorf("problem marshaling GeoJSON geometry: %v", err)
	}

	cPath := C.CString(filePath)
	defer C.free(unsafe.Pointer(cPath))
	ds := C.GDALOpen(cPath, C.GDAL_OF_READONLY)
	if ds == nil {
		return nil, fmt.Errorf("gdal could not open dataset: %s", filePath)
	}
	defer C.GDALClose(ds)

	cGeom := C.CString(string(geomGeoJSON))
	defer C.free(unsafe.Pointer(cGeom))
	geom := C.OGR_G_CreateGeometryFromJson(cGeom)
	if geom == nil {
		return nil, fmt.Errorf("geometry could not be parsed")
	}

	selSRS := C.OSRNewSpatialReference(cWGS84WKT)
	defer C.OSRDestroySpatialReference(selSRS)

	C.OGR_G_AssignSpatialReference(geom, selSRS)

	gCopy := C.OGR_G_Buffer(geom, C.double(0.0), C.int(30))
	if C.OGR_G_IsEmpty(gCopy) == C.int(1) {
		gCopy = C.OGR_G_Clone(geom)
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

	geot := make([]float64, 6)
	gdalErr := C.GDALGetGeoTransform(ds, (*C.double)(&geot[0]))
	if gdalErr != 0 {
		return nil, fmt.Errorf("couldn't get the geotransform from the source dataset %v", gdalErr)
	}

	xSize := int32(float64(C.GDALGetRasterXSize(ds)) + 0.5)
	ySize := int32(float64(C.GDALGetRasterYSize(ds)) + 0.5)

	if err != nil {
		return nil, err
	}

	srcBBox := []int32{0, 0, xSize, ySize}

	mask, err := createMask(ds, gCopy, geot, srcBBox)

	if err != nil {
		return nil, err
	}

	dsDscr := &ClipFileDescriptor{srcBBox, srcBBox, mask, geot, []float64{}}

	geomRasterData := &GeomRasterData{
		DataType:    "Byte",
		ClipDescr:   *dsDscr,
		NoDataValue: 0,
	}

	if inverse {
		out := make([]uint8, len(mask))
		for i, m := range mask {
			if m == 255 {
				out[i] = uint8(0)
			} else {
				out[i] = uint8(255)
			}
		}
		geomRasterData.Data = out
		geomRasterData.NoDataValue = 255

		return geomRasterData, nil
	}

	geomRasterData.Data = mask

	return geomRasterData, nil
}

func CreatVisMemGeomMask(feature geo.Feature, crs string, width int32, height int32, geot []float64) error {
	filePath, err := CreatVisMem(crs, []int32{width, height}, geot)

	if err != nil {
		return err
	}

	cPath := C.CString(*filePath)
	defer C.free(unsafe.Pointer(cPath))

	ds := C.GDALOpen(cPath, C.GA_Update)
	if ds == nil {
		return fmt.Errorf("gdal could not open dataset: %s", *filePath)
	}
	defer C.GDALClose(ds)

	geomData, err := CreatGeomMask(feature, *filePath, true)

	if err != nil {
		return err
	}

	path := "/vsimem/mem_inter.tif"

	err = WriteDataToGeoTiff(geomData, path, false)

	if err != nil {
		return err
	}

	timestamp, err := time.Parse(ISOFormat, "2021-11-30T00:00:00Z")

	if err != nil {
		return err
	}

	yMax := geomData.ClipDescr.SrcGeot[3]

	xPixel := geomData.ClipDescr.SrcGeot[2]
	yPixel := geomData.ClipDescr.SrcGeot[5]

	xMin := geomData.ClipDescr.SrcGeot[0]
	xMax := xMin + float64(geomData.ClipDescr.DstBBox[2])*xPixel
	yMin := yMax + float64(geomData.ClipDescr.DstBBox[3])*yPixel

	wktPolygon := fmt.Sprintf(`POLYGON((%[1]f %[2]f, %[1]f %[4]f, %[2]f %[4]f, %[3]f %[2]f, %[1]f %[2]f))`, xMin, yMin, xMax, yMax)

	gdalDataset := GDALDataset{
		RawPath:      path,
		DSName:       path,
		NameSpace:    "pixelmask",
		ArrayType:    "Byte",
		SRS:          C.GoString(cWGS85MercatorWKT),
		GeoTransform: geomData.ClipDescr.DstGeot,
		TimeStamps:   []time.Time{timestamp},
		Axes:         nil,
		NoData:       255,
		GeoLocation:  nil,
		Polygon:      wktPolygon,
	}

	metadata := MetadataResponse{
		GDALDatasets: []*GDALDataset{
			&gdalDataset,
		},
	}

	return nil
}

func CreatVisMem(crs string, bbox []int32, geot []float64) (*string, error) {
	canvas := make([]uint8, bbox[0]*bbox[1])

	memStr := fmt.Sprintf("MEM:::DATAPOINTER=%d,PIXELS=%d,LINES=%d,DATATYPE=Byte", unsafe.Pointer(&canvas[0]), bbox[0], bbox[1])
	memStrC := C.CString(memStr)
	defer C.free(unsafe.Pointer(memStrC))
	hSrcDS := C.GDALOpen(memStrC, C.GA_Update)
	if hSrcDS == nil {
		return nil, fmt.Errorf("error creating memory dataset")
	}
	defer C.GDALClose(hSrcDS)

	hSRS := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(hSRS)
	C.OSRImportFromEPSG(hSRS, C.int(3857))
	var projWKT *C.char
	defer C.free(unsafe.Pointer(projWKT))
	C.OSRExportToWkt(hSRS, &projWKT)
	C.GDALSetProjection(hSrcDS, projWKT)

	var gdalErr C.CPLErr

	if gdalErr = C.GDALSetGeoTransform(hSrcDS, (*C.double)(&geot[0])); gdalErr != 0 {
		return nil, fmt.Errorf("couldn't set the geotransform on the destination dataset %v", gdalErr)
	}

	driverStr := C.CString("GTiff")
	defer C.free(unsafe.Pointer(driverStr))
	hDriver := C.GDALGetDriverByName(driverStr)

	outFileStr := fmt.Sprintf("/vsimem/%d_%d.tif", bbox[0], bbox[1])
	outFileC := C.CString(outFileStr)
	defer C.free(unsafe.Pointer(outFileC))

	hDstDS := C.GDALCreateCopy(hDriver, outFileC, hSrcDS, C.int(0), nil, nil, nil)

	if hDstDS != nil {
		return &outFileStr, nil
	}

	return nil, fmt.Errorf("error creating memory raster")

}

func WriteDataToGeoTiff(geomData *GeomRasterData, outPath string, applyMask bool) error {
	dsDscr := geomData.ClipDescr
	bandSize := int(dsDscr.DstBBox[2] * dsDscr.DstBBox[3])
	bandStrides := 1

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

	var hDs C.GDALDatasetH

	gerr := C.CPLErr(0)

	switch geomData.DataType {
	case "Byte":
		dataOutBuf := make([]uint8, bandSize*bandStrides)
		inData := geomData.Data.([]uint8)
		// no data
		if len(inData) < 1 {
			return fmt.Errorf("no data to write")
		}
		// reading of the non masked pixels
		for i := 0; i < bandSize; i++ {
			val := inData[i]
			if applyMask {
				if dsDscr.Mask[i] == 255 {
					val = inData[i]
				} else {
					// this is outside the geometry. Write nodata
					val = uint8(geomData.NoDataValue)
				}
			}
			dataOutBuf[i] = val
		}

		hDs = C.GDALCreate(hDriver, outFileC, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), 1, C.GDT_Byte, &driverOptions[0])
		if hDs == nil {
			os.Remove(outPath)
			return fmt.Errorf("error creating raster")
		}
		hBand := C.GDALGetRasterBand(hDs, C.int(1))
		// set no data value
		C.GDALSetRasterNoDataValue(hBand, C.double(geomData.NoDataValue))
		// write band data
		gerr = C.GDALRasterIO(hBand, C.GF_Write, 0, 0, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), unsafe.Pointer(&dataOutBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Byte, 0, 0)

		if gerr != 0 {
			C.GDALClose(hDs)
			return fmt.Errorf("error writing raster band")
		}

	case "UInt16":
		dataOutBuf := make([]uint16, bandSize*bandStrides)
		inData := geomData.Data.([]uint16)

		// no data
		if len(inData) < 1 {
			return fmt.Errorf("no data to write")
		}

		for i := 0; i < bandSize; i++ {
			val := inData[i]

			if applyMask {
				if dsDscr.Mask[i] == 255 {
					val = inData[i]
				} else {
					// this is outside the geometry. Write nodata
					val = uint16(geomData.NoDataValue)
				}
			}

			dataOutBuf[i] = val
		}

		hDs = C.GDALCreate(hDriver, outFileC, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), 1, C.GDT_UInt16, &driverOptions[0])
		if hDs == nil {
			os.Remove(outPath)
			return fmt.Errorf("error creating raster")
		}
		hBand := C.GDALGetRasterBand(hDs, C.int(1))
		// set no data value
		C.GDALSetRasterNoDataValue(hBand, C.double(geomData.NoDataValue))
		// write band data
		gerr = C.GDALRasterIO(hBand, C.GF_Write, 0, 0, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), unsafe.Pointer(&dataOutBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_UInt16, 0, 0)

		if gerr != 0 {
			C.GDALClose(hDs)
			return fmt.Errorf("error writing raster band")
		}
	case "Int16":
		dataOutBuf := make([]int16, bandSize*bandStrides)
		inData := geomData.Data.([]int16)

		// no data
		if len(inData) < 1 {
			return fmt.Errorf("no data to write")
		}

		for i := 0; i < bandSize; i++ {
			val := inData[i]

			if applyMask {
				if dsDscr.Mask[i] == 255 {
					val = inData[i]
				} else {
					// this is outside the geometry. Write nodata
					val = int16(geomData.NoDataValue)
				}
			}

			dataOutBuf[i] = val
		}

		hDs = C.GDALCreate(hDriver, outFileC, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), 1, C.GDT_Int16, &driverOptions[0])
		if hDs == nil {
			os.Remove(outPath)
			return fmt.Errorf("error creating raster")
		}
		hBand := C.GDALGetRasterBand(hDs, C.int(1))
		// set no data value
		C.GDALSetRasterNoDataValue(hBand, C.double(geomData.NoDataValue))
		// write band data
		gerr = C.GDALRasterIO(hBand, C.GF_Write, 0, 0, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), unsafe.Pointer(&dataOutBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Int16, 0, 0)

		if gerr != 0 {
			C.GDALClose(hDs)
			return fmt.Errorf("error writing raster band")
		}
	case "UInt32":
		dataOutBuf := make([]uint32, bandSize*bandStrides)
		inData := geomData.Data.([]uint32)
		// no data
		if len(inData) < 1 {
			return fmt.Errorf("no data to write")
		}

		for i := 0; i < bandSize; i++ {
			val := inData[i]

			if applyMask {
				if dsDscr.Mask[i] == 255 {
					val = inData[i]
				} else {
					// this is outside the geometry. Write nodata
					val = uint32(geomData.NoDataValue)
				}
			}

			dataOutBuf[i] = val
		}

		hDs = C.GDALCreate(hDriver, outFileC, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), 1, C.GDT_UInt32, &driverOptions[0])
		if hDs == nil {
			os.Remove(outPath)
			return fmt.Errorf("error creating raster")
		}
		hBand := C.GDALGetRasterBand(hDs, C.int(1))
		// set no data value
		C.GDALSetRasterNoDataValue(hBand, C.double(geomData.NoDataValue))
		// write band data
		gerr = C.GDALRasterIO(hBand, C.GF_Write, 0, 0, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), unsafe.Pointer(&dataOutBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_UInt32, 0, 0)

		if gerr != 0 {
			C.GDALClose(hDs)
			return fmt.Errorf("error writing raster band")
		}

	case "Int32":
		dataOutBuf := make([]int32, bandSize*bandStrides)
		inData := geomData.Data.([]int32)
		// no data
		if len(inData) < 1 {
			return fmt.Errorf("no data to write")
		}
		for i := 0; i < bandSize; i++ {
			val := inData[i]

			if applyMask {
				if dsDscr.Mask[i] == 255 {
					val = inData[i]
				} else {
					// this is outside the geometry. Write nodata
					val = int32(geomData.NoDataValue)
				}
			}

			dataOutBuf[i] = val
		}

		hDs = C.GDALCreate(hDriver, outFileC, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), 1, C.GDT_Int32, &driverOptions[0])
		if hDs == nil {
			os.Remove(outPath)
			return fmt.Errorf("error creating raster")
		}
		hBand := C.GDALGetRasterBand(hDs, C.int(1))
		// set no data value
		C.GDALSetRasterNoDataValue(hBand, C.double(geomData.NoDataValue))
		// write band data
		gerr = C.GDALRasterIO(hBand, C.GF_Write, 0, 0, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), unsafe.Pointer(&dataOutBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Int32, 0, 0)

		if gerr != 0 {
			C.GDALClose(hDs)
			return fmt.Errorf("error writing raster band")
		}

	case "Float32":
		dataOutBuf := make([]float32, bandSize*bandStrides)
		inData := geomData.Data.([]float32)
		// no data
		if len(inData) < 1 {
			return fmt.Errorf("no data to write")
		}
		for i := 0; i < bandSize; i++ {
			val := inData[i]

			if applyMask {
				if dsDscr.Mask[i] == 255 {
					val = inData[i]
				} else {
					// this is outside the geometry. Write nodata
					val = float32(geomData.NoDataValue)
				}
			}

			dataOutBuf[i] = val
		}
		hDs = C.GDALCreate(hDriver, outFileC, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), 1, C.GDT_Float32, &driverOptions[0])
		if hDs == nil {
			os.Remove(outPath)
			return fmt.Errorf("error creating raster")
		}
		hBand := C.GDALGetRasterBand(hDs, C.int(1))
		// set no data value
		C.GDALSetRasterNoDataValue(hBand, C.double(geomData.NoDataValue))
		// write band data
		gerr = C.GDALRasterIO(hBand, C.GF_Write, 0, 0, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), unsafe.Pointer(&dataOutBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Float32, 0, 0)

		if gerr != 0 {
			C.GDALClose(hDs)
			return fmt.Errorf("error writing raster band")
		}
	case "Float64":
		dataOutBuf := make([]float64, bandSize*bandStrides)
		inData := geomData.Data.([]float64)
		// no data
		if len(inData) < 1 {
			return fmt.Errorf("no data to write")
		}
		for i := 0; i < bandSize; i++ {
			val := inData[i]

			if applyMask {
				if dsDscr.Mask[i] == 255 {
					val = inData[i]
				} else {
					// this is outside the geometry. Write nodata
					val = float64(geomData.NoDataValue)
				}
			}

			dataOutBuf[i] = val
		}

		hDs = C.GDALCreate(hDriver, outFileC, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), 1, C.GDT_Float64, &driverOptions[0])
		if hDs == nil {
			os.Remove(outPath)
			return fmt.Errorf("error creating raster")
		}
		hBand := C.GDALGetRasterBand(hDs, C.int(1))
		// set no data value
		C.GDALSetRasterNoDataValue(hBand, C.double(geomData.NoDataValue))
		// write band data
		gerr = C.GDALRasterIO(hBand, C.GF_Write, 0, 0, C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), unsafe.Pointer(&dataOutBuf[0]), C.int(dsDscr.DstBBox[2]), C.int(dsDscr.DstBBox[3]), C.GDT_Float64, 0, 0)

		if gerr != 0 {
			C.GDALClose(hDs)
			return fmt.Errorf("error writing raster band")
		}
	default:
		return fmt.Errorf("unknown data type %s", geomData.DataType)
	}

	// Set projection
	hSRS := C.OSRNewSpatialReference(nil)
	defer C.OSRDestroySpatialReference(hSRS)
	C.OSRImportFromEPSG(hSRS, C.int(4326))
	var projWKT *C.char
	defer C.free(unsafe.Pointer(projWKT))
	C.OSRExportToWkt(hSRS, &projWKT)
	C.GDALSetProjection(hDs, projWKT)

	// Set geotransform
	C.GDALSetGeoTransform(hDs, (*C.double)(&geot[0]))

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

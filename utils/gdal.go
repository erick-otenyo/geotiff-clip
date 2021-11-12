package utils

// #include "gdal.h"
// #include "gdal_frmts.h"
// #cgo pkg-config: gdal
import "C"

import (
	"os"
	"path/filepath"
)

func InitGdal() {
	setDefaultEnv("GDAL_NETCDF_VERIFY_DIMS", "NO")
	setDefaultEnv("GDAL_PAM_ENABLED", "NO")
	setDefaultEnv("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
	setDefaultEnv("GDAL_VRT_ENABLE_PYTHON", "YES")
	setDefaultEnv("GDAL_MAX_DATASET_POOL_SIZE", "10")

	exeFilePath, err := os.Executable()
	if err == nil {
		setDefaultEnv("GDAL_DRIVER_PATH", filepath.Dir(exeFilePath))
	}

	registerGDALDrivers()
}

func setDefaultEnv(envVar string, defaultVal string) {
	if _, ok := os.LookupEnv(envVar); !ok {
		os.Setenv(envVar, defaultVal)
	}
}

func registerGDALDrivers() {
	// This is a bit nasty, but this is one way to work out which
	// drivers are present in the GDAL shared library. We then
	// load the drivers of interest and then load all of the
	// drivers. This places common drivers at the front of the
	// driver list.
	var haveNetCDF bool
	var haveGTiff bool

	// Find out which drivers are present
	C.GDALAllRegister()

	for i := 0; i < int(C.GDALGetDriverCount()); i++ {
		driver := C.GDALGetDriver(C.int(i))
		switch C.GoString(C.GDALGetDriverShortName(driver)) {
		case "netCDF":
			haveNetCDF = true
		case "GTiff":
			haveGTiff = true
		}
	}

	// De-register all the drivers again
	for i := 0; i < int(C.GDALGetDriverCount()); i++ {
		driver := C.GDALGetDriver(C.int(i))
		C.GDALDeregisterDriver(driver)
		switch C.GoString(C.GDALGetDriverShortName(driver)) {
		case "GSKY_netCDF":
			continue
		default:
			C.GDALDeregisterDriver(driver)
		}

	}

	// Register these drivers first for higher performance when
	// opening files (drivers are interrogated in a linear scan)
	if haveGTiff {
		C.GDALRegister_GTiff()
	}

	if haveNetCDF {
		C.GDALRegister_netCDF()
	}

	// Now register everything else
	C.GDALAllRegister()
}

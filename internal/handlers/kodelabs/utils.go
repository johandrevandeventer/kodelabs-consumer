package kodelabs_handler

import (
	"github.com/johandrevandeventer/devicesdb"
	"github.com/johandrevandeventer/devicesdb/models"
)

// Helper function to get database instance
// GetDBInstance returns the database instance or handles the error.
func getDBInstance() (*devicesdb.BMS_DB, error) {
	bmsDB, err := devicesdb.GetDB()
	if err != nil {
		return nil, err
	}
	return bmsDB, nil
}

func GetDevicesBySerialNumber(serialNumber string) ([]models.Device, error) {
	bmsDB, err := getDBInstance()
	if err != nil {
		return nil, err
	}

	var devices []models.Device
	if err := bmsDB.DB.Preload("Site.Customer").Where("device_serial_number = ?", serialNumber).Find(&devices).Error; err != nil {
		return nil, err
	}

	return devices, nil
}

package kodelabs_handler

import (
	"context"
	"os"
	"strings"

	"github.com/johandrevandeventer/kodelabs"
	"github.com/johandrevandeventer/kodelabs-consumer/internal/flags"
	"github.com/johandrevandeventer/logging"
	"go.uber.org/zap"
)

// runKodelabsHandler runs the Kodelabs handler
func RunKodelabsHandler(ctx context.Context, kodelabsChan chan kodelabs.Message, logger *zap.Logger) {
	workerPool := 100 // Number of concurrent workers
	semaphore := make(chan struct{}, workerPool)

	for {
		select {
		case <-ctx.Done(): // Handle context cancellation (e.g., Ctrl+C)
			logger.Info("Stopping Kodelabs handler due to context cancellation")
			return
		case data, ok := <-kodelabsChan:
			if !ok { // Check if the channel is closed
				logger.Info("Stopping worker because kodelabsChan is closed")
				return
			}

			// Acquire a semaphore slot
			semaphore <- struct{}{}

			// Process data in a goroutine
			go func(msg kodelabs.Message) {
				defer func() { <-semaphore }() // Release the semaphore slot
				kodelabsHandler(data)          // Handle the data
			}(data)
		}
	}
}

// kodelabsHandler sends data to Kodelabs
func kodelabsHandler(payload kodelabs.Message) {
	logger := logging.GetLogger("handlers.kodelabs")

	var kodelabsLogger *zap.Logger

	if flags.FlagKodelabsLogging {
		kodelabsLogger = logging.GetLogger("kodelabs")
	} else {
		kodelabsLogger = zap.NewNop()
	}

	devices, err := GetDevicesBySerialNumber(payload.DeviceSerialNumber)
	if err != nil {
		logger.Error("Failed to get devices by serial number", zap.Error(err))
		return
	}

	if len(devices) == 0 {
		logger.Error("No devices found for serial number", zap.String("serial_number", payload.DeviceSerialNumber))
		return
	}

	for _, device := range devices {
		var convertedPayload any

		deviceType := strings.ToLower(device.DeviceType)
		authToken := device.AuthToken
		url := device.BuildingURL

		if flags.FlagEnvironment == "development" {
			url = os.Getenv("KODELABS_BUILDING_URL")
			authToken = os.Getenv("KODELABS_AUTH_TOKEN")

			if url == "" {
				logger.Error("KODELABS_BUILDING_URL environment variable not set")
				return
			}

			if authToken == "" {
				logger.Error("KODELABS_AUTH_TOKEN environment variable not set")
				return
			}

		}

		if decoderType, exists := decoderTypes[deviceType]; !exists {
			logger.Warn("No decoder found", zap.String("deviceType", deviceType))
			return
		} else {
			switch decoderType {
			case 1:
			case 2:
				convertedPayload = kodelabs.ConvertToInverterAPIv2Payload(payload)
			default:
				convertedPayload = payload.Data
			}
		}

		_, err = kodelabs.SendPostRequest(payload, convertedPayload, url, authToken, kodelabsLogger)
		if err != nil {
			logger.Error("Error sending post request", zap.Error(err))
			return
		}
	}
}

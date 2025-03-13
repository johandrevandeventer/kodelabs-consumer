package engine

import (
	"encoding/json"
	"fmt"

	"github.com/johandrevandeventer/kafkaclient/payload"
	"github.com/johandrevandeventer/kodelabs"
	"github.com/johandrevandeventer/kodelabs-consumer/internal/flags"
	kodelabs_handler "github.com/johandrevandeventer/kodelabs-consumer/internal/handlers/kodelabs"
	"github.com/johandrevandeventer/logging"
	"go.uber.org/zap"
)

func (e *Engine) startWorker() {
	e.logger.Info("Starting Kodelabs workers")
	kodelabsChannel := make(chan kodelabs.Message, 1000)

	var kodelabsLogger *zap.Logger
	if flags.FlagKodelabsLogging {
		kodelabsLogger = logging.GetLogger("kodelabs")
	} else {
		kodelabsLogger = zap.NewNop()
	}

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		select {
		case <-e.ctx.Done():
			return
		default:
			e.logger.Debug("Starting Kodelabs handler")
			// influxdb_handler.RunInfluxDBHandler(e.ctx, influxDBChannel, e.influxDBClient, e.influxDBClient.Logger)
			kodelabs_handler.RunKodelabsHandler(e.ctx, kodelabsChannel, e.logger)

		}
	}()

	for {
		select {
		case <-e.ctx.Done(): // Handle context cancellation (e.g., Ctrl+C)
			e.logger.Info("Stopping worker due to context cancellation")
			return
		case data, ok := <-e.kafkaConsumer.GetOutputChannel():
			if !ok { // Channel is closed
				e.logger.Info("Kafka consumer output channel closed, stopping worker")
				return
			}
			// Deserialize data
			p, err := payload.Deserialize(data)
			if err != nil {
				e.logger.Error("Failed to deserialize message", zap.Error(err))
				continue
			}

			// Deserialize Kodelabs message
			m, err := DeserializeKodelabsMessage(p.Message)
			if err != nil {
				e.logger.Error("Failed to deserialize Kodelabs message", zap.Error(err))
				continue
			}

			loggerMsg := fmt.Sprintf("Kodelabs -> %s :: %s :: %s :: %s :: %s :: %s :: %s", m.State, m.CustomerName, m.SiteName, m.Controller, m.ControllerSerialNumber, m.DeviceType, m.DeviceSerialNumber)
			kodelabsLogger.Info(loggerMsg)
			kodelabsChannel <- *m
		}
	}
}

func DeserializeKodelabsMessage(data []byte) (*kodelabs.Message, error) {
	var m kodelabs.Message
	err := json.Unmarshal(data, &m)
	return &m, err
}

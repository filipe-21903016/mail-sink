package consumer

import (
	"crypto/tls"
	"errors"
	"mailsink/internal/config"
	"mailsink/internal/logger"
	"math"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

func connectWithRetry(connStrg string, useSSL bool, maxRetries int) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error

	for attempt := 0; attempt < maxRetries; attempt++ {
		if useSSL {
			tlsConfig := &tls.Config{InsecureSkipVerify: true} // TODO: add cert verification
			conn, err = amqp.DialTLS(connStrg, tlsConfig)
		} else {
			conn, err = amqp.Dial(connStrg)
		}

		if err == nil {
			logger.Log.WithField("connection", connStrg).Info("Connected to RabbitMQ")
			return conn, nil
		}

		waitTime := time.Duration(math.Pow(2, float64(attempt))) * time.Second
		logger.Log.WithFields(map[string]interface{}{
			"attempt": attempt + 1,
			"max":     maxRetries,
			"error":   err,
			"wait":    waitTime.String(),
		}).Warn("Connection failed, retrying...")
		time.Sleep(waitTime)
	}

	return nil, err
}

func StartWorker(workerID int, connStr string, queueName string, cfg *config.Config) {
	const maxRetries = 5

	conn, err := connectWithRetry(connStr, cfg.RabbitmqUseSSL, maxRetries)
	if err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"error":  err,
		}).Fatal("Could not connect after retries")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"error":  err,
		}).Fatal("Failed to open channel")
	}
	defer ch.Close()

	msgs, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"error":  err,
		}).Fatal("Failed to consume messages")
	}

	var processed uint64
	var failed uint64

	logger.Log.WithField("worker", workerID).Info("Worker started, waiting for messages")

	for msg := range msgs {
		handleMessage(workerID, msg, cfg, &processed, &failed)

		total := atomic.LoadUint64(&processed) + atomic.LoadUint64(&failed)
		if total%10 == 0 {
			logger.Log.WithFields(map[string]interface{}{
				"worker":    workerID,
				"processed": processed,
				"failed":    failed,
			}).Info("Metrics update")
		}
	}
}

func ackMessage(workerID int, msg amqp.Delivery, raw string) {
	if err := msg.Ack(false); err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker":  workerID,
			"error":   err,
			"message": raw,
		}).Error("Failed to ack message")
	}
}

func handleMessage(workerID int, msg amqp.Delivery, cfg *config.Config, processed, failed *uint64) {
	raw := string(msg.Body)

	err := ProcessMessage(workerID, raw, cfg)

	switch {
	case err == nil:
		ackMessage(workerID, msg, raw)
		atomic.AddUint64(processed, 1)
	case errors.Is(err, ErrInvalidSchema):
		ackMessage(workerID, msg, raw)
		atomic.AddUint64(failed, 1)
		logger.Log.WithFields(map[string]interface{}{
			"worker":  workerID,
			"message": raw,
		}).Error("Invalid message removed from queue (permanent failure)")
	default:
		ackMessage(workerID, msg, raw)
		atomic.AddUint64(failed, 1)
		logger.Log.WithFields(map[string]interface{}{
			"worker":  workerID,
			"message": raw,
			"error":   err,
		}).Error("Message failed and removed from queue")
	}
}

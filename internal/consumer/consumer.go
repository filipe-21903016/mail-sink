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

const MAX_RETRIES = 5

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
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"error":  err,
		}).Fatal("Failed to declare queue")
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
		handleMessage(workerID, msg, ch, cfg, &processed, &failed)

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

func nackMessage(workerID int, msg amqp.Delivery, raw string, ch *amqp.Channel, queueName string, retryCount int) {
	// Update headers
	headers := msg.Headers
	if headers == nil {
		headers = amqp.Table{}
	}
	headers["x-retry-count"] = int32(retryCount)

	// Re-publish message with updated headers
	err := ch.Publish(
		"", // default exchange
		queueName,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers:     headers,
			ContentType: msg.ContentType,
			Body:        msg.Body,
		},
	)
	if err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"error":  err,
		}).Error("Failed to republish message for retry")
	}

	// Acknowledge the old message
	msg.Ack(false)
}

func handleMessage(workerID int, msg amqp.Delivery, ch *amqp.Channel, cfg *config.Config, processed *uint64, failed *uint64) {
	raw := string(msg.Body)

	err := ProcessMessage(workerID, raw, cfg)

	retryCount := 0
	if val, ok := msg.Headers["x-retry-count"]; ok {
		if rc, ok := val.(int32); ok {
			retryCount = int(rc)
		}
	}

	switch {
	case err == nil:
		ackMessage(workerID, msg, raw)
		atomic.AddUint64(processed, 1)

	case errors.Is(err, ErrInvalidSchema):
		// Permanent Failure - remove from queue
		ackMessage(workerID, msg, raw)
		atomic.AddUint64(failed, 1)
		logger.Log.WithFields(map[string]interface{}{
			"worker":  workerID,
			"message": raw,
		}).Error("Invalid message removed from queue (permanent failure)")

	case errors.Is(err, ErrTransientFailed), errors.Is(err, ErrKeyValueStore):
		// Transient Failure -> requeue message
		if retryCount+1 > MAX_RETRIES {
			// Move to dead letter queue
			msg.Ack(false)
			logger.Log.Warn("Max retries reached, message dropped")
		} else {
			// Requeue with incremented retry count
			nackMessage(workerID, msg, raw, ch, cfg.RabbitmqQueue, retryCount+1)
			atomic.AddUint64(failed, 1)
			logger.Log.WithFields(map[string]interface{}{
				"worker":     workerID,
				"message":    raw,
				"error":      err,
				"retryCount": retryCount,
			}).Warn("Transient failure, message requeued")
		}

	default:
		if retryCount+1 > MAX_RETRIES {
			// Move to dead letter queue
			msg.Ack(false)
			logger.Log.Warn("Max retries reached, message dropped")
		} else {
			// TODO: requeue for now then move to dead letter queue
			nackMessage(workerID, msg, raw, ch, cfg.RabbitmqQueue, retryCount+1)
			atomic.AddUint64(failed, 1)
			logger.Log.WithFields(map[string]interface{}{
				"worker":  workerID,
				"message": raw,
				"error":   err,
			}).Error("Unknown failure, message requeued")
		}
	}
}

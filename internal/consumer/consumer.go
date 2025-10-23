package consumer

import (
	"crypto/tls"
	"errors"
	"fmt"
	"mailsink/internal/config"
	"mailsink/internal/logger"
	"math"
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

func setupRabbitMQ(ch *amqp.Channel, mainQueue string) error {
	mainExchangeName := fmt.Sprintf("%s_exchange", mainQueue)
	dlxExchangeName := fmt.Sprintf("%s_dlx", mainQueue)
	retryQueueName := fmt.Sprintf("%s_retry", mainQueue)
	deadQueueName := fmt.Sprintf("%s_dead", mainQueue)

	if err := ch.ExchangeDeclare(mainExchangeName, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(dlxExchangeName, "direct", true, false, false, false, nil); err != nil {
		return err
	}

	// Main queue → sends failed messages to DLX
	mainArgs := amqp.Table{
		"x-dead-letter-exchange":    dlxExchangeName,
		"x-dead-letter-routing-key": "retry",
	}
	if _, err := ch.QueueDeclare(mainQueue, true, false, false, false, mainArgs); err != nil {
		return err
	}

	// Retry queue → after delay, sends back to main exchange
	retryArgs := amqp.Table{
		"x-dead-letter-exchange":    mainExchangeName,
		"x-dead-letter-routing-key": "process",
		"x-message-ttl":             int32(15000), // 15s delay before requeue
	}
	if _, err := ch.QueueDeclare(retryQueueName, true, false, false, false, retryArgs); err != nil {
		return err
	}

	// Dead queue for permanently failed messages
	if _, err := ch.QueueDeclare(deadQueueName, true, false, false, false, nil); err != nil {
		return err
	}

	// Bind queues to their exchanges
	if err := ch.QueueBind(mainQueue, "process", mainExchangeName, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(retryQueueName, "retry", dlxExchangeName, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(deadQueueName, "dead", dlxExchangeName, false, nil); err != nil {
		return err
	}

	return nil
}

func StartWorker(workerID int, connStr string, cfg *config.Config) {
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

	err = setupRabbitMQ(ch, cfg.RabbitmqQueue)
	if err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"error":  err,
		}).Fatal("Failed to setup RabbitMQ queues/exchanges")
	}

	msgs, err := ch.Consume(cfg.RabbitmqQueue, "", false, false, false, false, nil)
	if err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"error":  err,
		}).Fatal("Failed to consume messages")
	}

	logger.Log.WithField("worker", workerID).Info("Worker started, waiting for messages")

	for msg := range msgs {
		handleMessage(workerID, msg, ch, cfg)
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

func killMessage(workerID int, msg amqp.Delivery, ch *amqp.Channel, cfg *config.Config, retryCount int) {
	err := ch.Publish(fmt.Sprintf("%s_dlx", cfg.RabbitmqQueue), "dead", false, false, amqp.Publishing{
		Headers:     amqp.Table{"x-retry-count": retryCount},
		ContentType: msg.ContentType,
		Body:        msg.Body,
	})
	if err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker":     workerID,
			"retryCount": retryCount,
			"error":      err}).Error("Failed to publish to dead queue")
	}
	msg.Ack(false)
	logger.Log.WithFields(map[string]interface{}{
		"worker":     workerID,
		"retryCount": retryCount,
	}).Warn("Max retries reached, message sent to dead queue")
}

func handleMessage(workerID int, msg amqp.Delivery, ch *amqp.Channel, cfg *config.Config) {
	raw := string(msg.Body)
	err := ProcessMessage(workerID, raw, cfg)

	retryCount := 0
	if val, ok := msg.Headers["x-death"]; ok {
		if deaths, ok := val.([]interface{}); ok && len(deaths) > 0 {
			if d, ok := deaths[0].(amqp.Table); ok {
				if count, ok := d["count"].(int64); ok {
					retryCount = int(count)
				}
			}
		}
	}

	switch {
	case err == nil:
		ackMessage(workerID, msg, raw)

	case errors.Is(err, ErrInvalidSchema):
		// Permanent Failure - remove from queue
		ackMessage(workerID, msg, raw)
		logger.Log.WithFields(map[string]interface{}{
			"worker":  workerID,
			"message": raw,
		}).Error("Invalid schema, message removed from queue (permanent failure)")

	case errors.Is(err, ErrTransientFailed), errors.Is(err, ErrKeyValueStore):
		// Transient Failure -> requeue message
		if retryCount+1 > MAX_RETRIES {
			killMessage(workerID, msg, ch, cfg, retryCount)
		} else {
			msg.Nack(false, false)
			logger.Log.WithFields(map[string]interface{}{
				"worker":     workerID,
				"retryCount": retryCount,
				"error":      err,
			}).Warn("Transient failure, message sent to retry queue")
		}

	default:
		if retryCount+1 > MAX_RETRIES {
			killMessage(workerID, msg, ch, cfg, retryCount)
		} else {
			msg.Nack(false, false)
			logger.Log.WithFields(map[string]interface{}{
				"worker":     workerID,
				"retryCount": retryCount,
				"error":      err,
			}).Warn("Unknown failure, message sent to retry queue")
		}
	}
}

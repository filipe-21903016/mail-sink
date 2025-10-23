package consumer

import (
	"encoding/json"
	"errors"
	"mailsink/internal/config"
	"mailsink/internal/db"
	"mailsink/internal/logger"
	"time"
)

var (
	ErrInvalidSchema   = errors.New("invalid message schema")
	ErrTransientFailed = errors.New("transient processing failure")
	ErrKeyValueStore   = errors.New("failed checking key-value store")
)

func ProcessMessage(workerID int, rawMessage string, cfg *config.Config) error {
	var mailSinkMessage MailSinkMessage
	if err := json.Unmarshal([]byte(rawMessage), &mailSinkMessage); err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"error":  err,
			"raw":    rawMessage,
		}).Error("Failed to parse JSON message")
		return ErrInvalidSchema
	}

	// Set key to processing
	set, err := db.Rdb.SetNX(db.Ctx, mailSinkMessage.IdempotencyKey, "processing", 5*time.Minute).Result()
	if err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"key":    mailSinkMessage.IdempotencyKey,
			"error":  err,
		}).Error("Failed to write idempotency key")
		return ErrKeyValueStore
	}
	if !set {
		// Another worker is already processing or has processed this message
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"key":    mailSinkMessage.IdempotencyKey,
		}).Info("Message already processed or being processed, skipping")
		return nil
	}

	emailMsg := mailSinkMessage.Payload
	if !emailMsg.Validate(workerID, rawMessage) {
		return ErrInvalidSchema
	}

	// At this point, message is valid
	logger.Log.WithFields(map[string]interface{}{
		"worker":  workerID,
		"to":      emailMsg.To,
		"cc":      emailMsg.CC,
		"bcc":     emailMsg.BCC,
		"subject": emailMsg.Subject,
		"isHtml":  emailMsg.IsHTML,
	}).Info("Processing email message")

	if err := SendEmail(&emailMsg, cfg); err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"error":  err,
		}).Error("Failed to send email")
		return ErrTransientFailed
	}

	// Write idempotency key to key-value store
	err = db.Rdb.Set(db.Ctx, mailSinkMessage.IdempotencyKey, "processed", 24*time.Hour).Err()
	if err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"key":    mailSinkMessage.IdempotencyKey,
			"error":  err,
		}).Error("Failed to update idempotency key to processed (email already sent)")
	}

	logger.Log.WithFields(map[string]interface{}{
		"worker": workerID,
		"key":    mailSinkMessage.IdempotencyKey,
	}).Info("Message processed and key set in Redis with 24h expiration")

	return nil
}

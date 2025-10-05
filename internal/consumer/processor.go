package consumer

import (
	"encoding/json"
	"errors"
	"mailsink/internal/config"
	"mailsink/internal/logger"
)

var (
	ErrInvalidSchema   = errors.New("invalid message schema")
	ErrTransientFailed = errors.New("transient processing failure")
)

func ProcessMessage(workerID int, rawMessage string, cfg *config.Config) error {
	var emailMsg EmailMessage
	if err := json.Unmarshal([]byte(rawMessage), &emailMsg); err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"error":  err,
			"raw":    rawMessage,
		}).Error("Failed to parse JSON message")
		return ErrInvalidSchema
	}

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

	err := SendEmail(&emailMsg, cfg)
	if err != nil {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"error":  err,
		}).Error("Failed to send email")
		return ErrTransientFailed
	}

	return nil
}

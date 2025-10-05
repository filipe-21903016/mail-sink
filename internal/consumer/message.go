package consumer

import (
	"mailsink/internal/logger"
	"strings"
)

type EmailMessage struct {
	To      []string `json:"to"`
	CC      []string `json:"cc,omitempty"`
	BCC     []string `json:"bcc,omitempty"`
	Subject string   `json:"subject"`
	Body    string   `json:"body"`
	IsHTML  bool     `json:"isHtml"`
}

func (m *EmailMessage) Validate(workerID int, rawMessage string) bool {
	valid := true

	if len(m.To) == 0 {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"raw":    rawMessage,
		}).Warn("Missing 'to' recipients")
		valid = false
	} else {
		for _, addr := range m.To {
			if strings.TrimSpace(addr) == "" {
				logger.Log.WithFields(map[string]interface{}{
					"worker": workerID,
					"raw":    rawMessage,
				}).Warn("Empty 'to' recipient found")
				valid = false
			}
		}
	}

	if strings.TrimSpace(m.Body) == "" {
		logger.Log.WithFields(map[string]interface{}{
			"worker": workerID,
			"raw":    rawMessage,
		}).Warn("Missing 'body'")
		valid = false
	}

	return valid
}

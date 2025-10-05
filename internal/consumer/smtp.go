package consumer

import (
	"mailsink/internal/config"
	"mailsink/internal/logger"

	gomail "gopkg.in/gomail.v2"
)

func SendEmail(emailMessage *EmailMessage, cfg *config.Config) error {
	m := gomail.NewMessage()

	// Set From
	m.SetHeader("From", cfg.SMTPUser)

	// Set To, CC, BCC
	if len(emailMessage.To) > 0 {
		m.SetHeader("To", emailMessage.To...)
	}
	if len(emailMessage.CC) > 0 {
		m.SetHeader("Cc", emailMessage.CC...)
	}
	if len(emailMessage.BCC) > 0 {
		m.SetHeader("Bcc", emailMessage.BCC...)
	}

	// Set Subject
	m.SetHeader("Subject", emailMessage.Subject)

	// Set Body (HTML or plain text)
	if emailMessage.IsHTML {
		m.SetBody("text/html", emailMessage.Body)
	} else {
		m.SetBody("text/plain", emailMessage.Body)
	}

	logger.Log.WithFields(map[string]interface{}{
		"to":      emailMessage.To,
		"cc":      emailMessage.CC,
		"bcc":     emailMessage.BCC,
		"subject": emailMessage.Subject,
		"isHtml":  emailMessage.IsHTML,
	}).Info("Sending email...")

	d := gomail.NewDialer(cfg.SMTPServer, cfg.SMTPPortInt(), cfg.SMTPUser, cfg.SMTPPass)
	d.TLSConfig = cfg.TLSServerConfig()

	if err := d.DialAndSend(m); err != nil {
		logger.Log.WithError(err).Error("Failed to send email")
		return err
	}

	logger.Log.Info("Email sent successfully!")
	return nil
}

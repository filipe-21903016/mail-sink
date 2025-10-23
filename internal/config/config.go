package config

import (
	"crypto/tls"
	"mailsink/internal/logger"
	"os"
	"strconv"
)

type Config struct {
	RabbitmqHost    string
	RabbitmqPort    string
	RabbitmqUser    string
	RabbitmqPass    string
	RabbitmqQueue   string
	RabbitmqUseSSL  bool   // true of false
	RabbitmqSSLCert string // optional client cert file path
	RabbitmqSSLKey  string // optional client key file path
	RabbitmqSSLCA   string // optional ca cert file path
	WorkerCount     int
	SMTPServer      string
	SMTPPort        string
	SMTPUser        string
	SMTPPass        string
	SMTPUseSSL      bool
	RedisHost       string
	RedisPort       string
	RedisPass       string
}

func (c *Config) SMTPPortInt() int {
	p, _ := strconv.Atoi(c.SMTPPort)
	return p
}

func (c *Config) TLSServerConfig() *tls.Config {
	// TODO: add cert verification
	return &tls.Config{InsecureSkipVerify: true}
}

func LoadConfig() Config {
	workerCount := 4 // default worker count
	if wcStr, ok := os.LookupEnv("WORKER_COUNT"); ok {
		if wc, err := strconv.Atoi(wcStr); err == nil {
			workerCount = wc
		} else {
			logger.Log.WithFields(map[string]interface{}{
				"invalid_value": wcStr,
				"default":       workerCount,
			}).Warn("Invalid WORKER_COUNT, using default")
		}
	}

	rabbitUseSSL := false // default to false
	if useSSLStr := os.Getenv("RABBITMQ_USE_SSL"); useSSLStr == "true" {
		rabbitUseSSL = true
	}

	smtpUseSSL := false
	if useSSLStr := os.Getenv("SMTP_USE_SSL"); useSSLStr == "true" {
		smtpUseSSL = true
	}

	return Config{
		RabbitmqHost:    os.Getenv("RABBITMQ_HOST"),
		RabbitmqPort:    os.Getenv("RABBITMQ_PORT"),
		RabbitmqUser:    os.Getenv("RABBITMQ_USER"),
		RabbitmqPass:    os.Getenv("RABBITMQ_PASS"),
		RabbitmqQueue:   os.Getenv("RABBITMQ_QUEUE"),
		RabbitmqUseSSL:  rabbitUseSSL,
		RabbitmqSSLCert: os.Getenv("RABBITMQ_SSL_CERT"),
		RabbitmqSSLKey:  os.Getenv("RABBITMQ_SSL_KEY"),
		RabbitmqSSLCA:   os.Getenv("RABBITMQ_SSL_CA"),
		WorkerCount:     workerCount,
		SMTPServer:      os.Getenv("SMTP_SERVER"),
		SMTPPort:        os.Getenv("SMTP_PORT"),
		SMTPUser:        os.Getenv("SMTP_USER"),
		SMTPPass:        os.Getenv("SMTP_PASS"),
		SMTPUseSSL:      smtpUseSSL,
		RedisHost:       os.Getenv("REDIS_HOST"),
		RedisPort:       os.Getenv("REDIS_PORT"),
		RedisPass:       os.Getenv("REDIS_PASS"),
	}
}

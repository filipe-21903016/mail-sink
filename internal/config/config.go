package config

import (
	"crypto/tls"
	"mailsink/internal/logger"
	"os"
	"strconv"
	"strings"
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
	RedisDb         int
}

func (c *Config) SMTPPortInt() int {
	p, _ := strconv.Atoi(c.SMTPPort)
	return p
}

func (c *Config) TLSServerConfig() *tls.Config {
	// TODO: add cert verification
	return &tls.Config{InsecureSkipVerify: true}
}

func getEnvInt(envKey string, defaultValue int) int {
	if strVal, ok := os.LookupEnv(envKey); ok {
		if val, err := strconv.Atoi(strVal); err == nil {
			return val
		} else {
			logger.Log.WithFields(map[string]interface{}{
				"invalid_value": strVal,
				"default":       defaultValue,
			}).Warnf("Invalid %s, using default", envKey)
		}
	}
	return defaultValue
}

func getEnvBool(envKey string, defaultValue bool) bool {
	if strVal, ok := os.LookupEnv(envKey); ok {
		lower := strings.ToLower(strVal)
		switch lower {
		case "true", "1", "yes", "y":
			return true
		case "false", "0", "no", "n":
			return false
		default:
			// Invalid value; return default
			return defaultValue
		}
	}
	return defaultValue
}

func LoadConfig() Config {
	return Config{
		RabbitmqHost:    os.Getenv("RABBITMQ_HOST"),
		RabbitmqPort:    os.Getenv("RABBITMQ_PORT"),
		RabbitmqUser:    os.Getenv("RABBITMQ_USER"),
		RabbitmqPass:    os.Getenv("RABBITMQ_PASS"),
		RabbitmqQueue:   os.Getenv("RABBITMQ_QUEUE"),
		RabbitmqUseSSL:  getEnvBool("RABBITMQ_USE_SSL", false),
		RabbitmqSSLCert: os.Getenv("RABBITMQ_SSL_CERT"),
		RabbitmqSSLKey:  os.Getenv("RABBITMQ_SSL_KEY"),
		RabbitmqSSLCA:   os.Getenv("RABBITMQ_SSL_CA"),
		WorkerCount:     getEnvInt("WORKER_COUNT", 4),
		SMTPServer:      os.Getenv("SMTP_SERVER"),
		SMTPPort:        os.Getenv("SMTP_PORT"),
		SMTPUser:        os.Getenv("SMTP_USER"),
		SMTPPass:        os.Getenv("SMTP_PASS"),
		SMTPUseSSL:      getEnvBool("SMTP_USE_SSL", false),
		RedisHost:       os.Getenv("REDIS_HOST"),
		RedisPort:       os.Getenv("REDIS_PORT"),
		RedisPass:       os.Getenv("REDIS_PASS"),
		RedisDb:         getEnvInt("REDIS_DB", 0),
	}
}

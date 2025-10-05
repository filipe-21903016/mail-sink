package main

import (
	"fmt"
	"log"
	"mailsink/internal/config"
	"mailsink/internal/consumer"
	"mailsink/internal/logger"
	"os"
	"os/signal"
	"syscall"
)

func buildAMQPConn(cfg config.Config) string {
	scheme := "amqp"
	if cfg.RabbitmqUseSSL {
		scheme = "amqps"
	}
	return fmt.Sprintf("%s://%s:%s@%s:%s/", scheme, cfg.RabbitmqUser, cfg.RabbitmqPass, cfg.RabbitmqHost, cfg.RabbitmqPort)
}

func main() {
	logger.InitLogger()
	cfg := config.LoadConfig()
	connStr := buildAMQPConn(cfg)

	logger.Log.WithFields(map[string]interface{}{
		"host":    cfg.RabbitmqHost,
		"queue":   cfg.RabbitmqQueue,
		"workers": cfg.WorkerCount,
		"ssl":     cfg.RabbitmqUseSSL,
	}).Info("Starting MailSink")

	// Start workers
	for i := 0; i < cfg.WorkerCount; i++ {
		go consumer.StartWorker(i, connStr, cfg.RabbitmqQueue, &cfg)
	}

	// Gracefully handle shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	log.Println("Shutting down MailSink...")
}

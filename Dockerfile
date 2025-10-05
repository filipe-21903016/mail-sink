# --- Build stage ---
FROM golang:1.24-alpine AS builder

# Disable CGO for static build
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# Set working directory
WORKDIR /app

# Copy Go modules files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build the binary (main package is in cmd/mail-sink)
RUN go build -o mailsink ./cmd/mail-sink

# --- Final stage ---
FROM alpine:latest

# Set working directory
WORKDIR /app

# Copy the binary
COPY --from=builder /app/mailsink .

# Add CA certificates for TLS (needed for SMTP)
RUN apk add --no-cache ca-certificates

# Environment variables can be set in Coolify
ENV SMTP_USER=""
ENV SMTP_PASS=""
ENV SMTP_SERVER=""
ENV SMTP_PORT=""
ENV SMTP_USE_SSL="false"
ENV RABBITMQ_URL=""

# Run the worker
CMD ["./mailsink"]

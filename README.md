# 📨 MailSink

MailSink is a small personal project written in Go.  
It listens to a RabbitMQ queue and processes messages — usually for sending emails via SMTP.

---

## 🏗️ Build

```bash
docker build -t mailsink:latest .
```

## 🚀 Run

```bash
docker run -d \
  --name mailsink-worker \
  -e SMTP_USER="you@example.com" \
  -e SMTP_PASS="supersecretpassword" \
  -e SMTP_SERVER="smtp.example.com" \
  -e SMTP_PORT="465" \
  -e SMTP_USE_SSL="true" \
  -e RABBITMQ_URL="amqp://rabbitmq:5672/" \
  -e RABBITMQ_USER="rabbituser" \
  -e RABBITMQ_PASS="rabbitpass" \
  mailsink:latest
```

or 

```bash
docker run -d \
  --name mailsink-worker \
  --env-file .env \
  mailsink:latest
```

## 💌 Expected Message Format

MailSink expects messages in JSON format like this:

```json
{
  "idempotencyKey": "unique-key-123",
  "payload": {
    "to": ["user@example.com"],
    "cc": ["cc@example.com"],
    "bcc": [],
    "subject": "Test Email",
    "body": "<p>Hello from MailSink!</p>",
    "isHtml": true,
    "attachments": [
      {
        "filename": "example.txt",
        "contentType": "text/plain",
        "data": "SGVsbG8gd29ybGQh" // base64 encoded
      }
    ]
  }
}
```
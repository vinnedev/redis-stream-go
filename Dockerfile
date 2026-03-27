FROM golang:1.26.1-alpine3.22 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/app ./cmd/main.go

FROM alpine:3.22

RUN apk add --no-cache ca-certificates \
    && adduser -D -g '' appuser

COPY --from=builder /bin/app /app

EXPOSE 8080

USER appuser

ENTRYPOINT ["/app"]

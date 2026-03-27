FROM golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/app ./cmd/main.go


FROM gcr.io/distroless/static-debian12

COPY --from=builder /bin/app /app

EXPOSE 8080

ENTRYPOINT ["/app"]

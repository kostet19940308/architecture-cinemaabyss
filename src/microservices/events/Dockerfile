# ---------- Build stage ----------
FROM golang:1.24-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /app

# 1) сначала только go.mod — чтобы закэшировать базовые зависимости
COPY go.mod ./
RUN go env -w GOPROXY=https://proxy.golang.org,direct \
    && go env -w GOSUMDB=sum.golang.org
RUN go mod tidy && go mod download

# 2) теперь всё остальное (в т.ч. исходники, возможный другой go.mod/go.sum)
COPY . .

# ВАЖНО: заново синхронизируем зависимости уже по финальному графу
RUN go mod tidy && go mod download

# 3) сборка
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o events-service ./main.go

# ---------- Runtime stage ----------
FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
WORKDIR /app
COPY --from=builder /app/events-service .
ENV PORT=8082
EXPOSE 8082
# HEALTHCHECK --interval=10s --timeout=2s --retries=5 \
#   CMD curl -fs http://localhost:8082/api/events/health || exit 1

CMD ["./events-service"]
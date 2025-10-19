# Build stage
FROM --platform=$BUILDPLATFORM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -a -installsuffix cgo -o proxy-service .
# Runtime stage
FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
WORKDIR /app
COPY --from=builder /app/proxy-service .

ENV PORT=8000
EXPOSE 8000
# HEALTHCHECK --interval=10s --timeout=2s --retries=5 CMD curl -fs http://localhost:8000/health || exit 1

# Command to run the executable
CMD ["./proxy-service"]
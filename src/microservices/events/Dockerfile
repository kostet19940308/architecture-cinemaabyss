FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /app

COPY go.mod ./
RUN go env -w GOPROXY=https://proxy.golang.org,direct \
    && go env -w GOSUMDB=sum.golang.org
RUN go mod tidy && go mod download

COPY . .

RUN go mod tidy && go mod download

ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -ldflags="-s -w" -o events-service ./main.go

FROM alpine:3.20
RUN apk add --no-cache ca-certificates curl
WORKDIR /app
COPY --from=builder /app/events-service .
ENV PORT=8082
EXPOSE 8082

CMD ["./events-service"]
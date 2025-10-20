package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type Config struct {
	Port         string
	KafkaBrokers []string
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic("missing env: " + key)
	}
	return v
}

func loadConfig(log *slog.Logger) Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	brokers := strings.Split(mustEnv("KAFKA_BROKERS"), ",")
	log.Info("env loaded", "PORT", port, "KAFKA_BROKERS", brokers)
	return Config{Port: port, KafkaBrokers: brokers}
}

type Event struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Timestamp string         `json:"timestamp"`
	Payload   map[string]any `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

type MovieEvent struct {
	MovieID     int      `json:"movie_id"`
	Title       string   `json:"title"`
	Action      string   `json:"action"`
	UserID      *int     `json:"user_id,omitempty"`
	Rating      *float64 `json:"rating,omitempty"`
	Genres      []string `json:"genres,omitempty"`
	Description *string  `json:"description,omitempty"`
}

type UserEvent struct {
	UserID    int     `json:"user_id"`
	Username  *string `json:"username,omitempty"`
	Email     *string `json:"email,omitempty"`
	Action    string  `json:"action"`
	Timestamp string  `json:"timestamp"`
}

type PaymentEvent struct {
	PaymentID int     `json:"payment_id"`
	UserID    int     `json:"user_id"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"`
	Timestamp string  `json:"timestamp"`
	Method    *string `json:"method_type,omitempty"`
}

type Kafka struct {
	log      *slog.Logger
	producer sarama.SyncProducer
	brokers  []string
}

func newKafka(log *slog.Logger, brokers []string) (*Kafka, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Idempotent = false
	cfg.Version = sarama.V2_7_0_0 // соответствует образу 2.7.0 в compose
	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	return &Kafka{log: log, producer: p, brokers: brokers}, nil
}

func (k *Kafka) Close() error { return k.producer.Close() }

func (k *Kafka) produceAndRead(ctx context.Context, topic string, key string, value []byte) (partition int32, offset int64, readMsg *sarama.ConsumerMessage, err error) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	partition, offset, err = k.producer.SendMessage(msg)
	if err != nil {
		return
	}
	k.log.Info("kafka produced", "topic", topic, "partition", partition, "offset", offset)

	consumer, err := sarama.NewConsumer(k.brokers, nil)
	if err != nil {
		return partition, offset, nil, err
	}
	defer consumer.Close()

	pc, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return partition, offset, nil, err
	}
	defer pc.Close()

	t := time.NewTimer(3 * time.Second)
	defer t.Stop()

	for {
		select {
		case m := <-pc.Messages():
			if m != nil && m.Offset == offset {
				k.log.Info("kafka read-back ok", "topic", topic, "partition", m.Partition, "offset", m.Offset)
				return partition, offset, m, nil
			}
		case err = <-pc.Errors():
			if err != nil {
				k.log.Error("kafka consume error", "error", err)
				return partition, offset, nil, err
			}
		case <-ctx.Done():
			return partition, offset, nil, ctx.Err()
		case <-t.C:
			return partition, offset, nil, errors.New("read-back timeout")
		}
	}
}

type Server struct {
	log   *slog.Logger
	cfg   Config
	kafka *Kafka
	mux   *http.ServeMux
}

func NewServer(log *slog.Logger, cfg Config, k *Kafka) *Server {
	s := &Server{
		log:   log,
		cfg:   cfg,
		kafka: k,
		mux:   http.NewServeMux(),
	}
	s.mux.HandleFunc("/api/events/health", s.handleHealth)

	s.mux.HandleFunc("/api/events/movie", s.handleMovieEvent)
	s.mux.HandleFunc("/api/events/user", s.handleUserEvent)
	s.mux.HandleFunc("/api/events/payment", s.handlePaymentEvent)

	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	s.mux.ServeHTTP(w, r)
	s.log.Info("request", "method", r.Method, "path", r.URL.Path, "dur", time.Since(start).String())
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": true})
}

// ---------- helpers ----------
func toEvent(t string, payload any) (Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return Event{}, err
	}
	return Event{
		ID:        t + "-" + time.Now().UTC().Format("20060102T150405.000Z0700"),
		Type:      t,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Payload:   m,
	}, nil
}

func respondProduced(w http.ResponseWriter, topic string, ev Event, p int32, o int64) {
	resp := EventResponse{
		Status:    "success",
		Partition: p,
		Offset:    o,
		Event:     ev,
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleMovieEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in MovieEvent
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return
	}
	if in.MovieID == 0 || in.Title == "" || in.Action == "" {
		http.Error(w, `{"error":"movie_id, title, action are required"}`, http.StatusBadRequest)
		return
	}
	ev, err := toEvent("movie", in)
	if err != nil {
		http.Error(w, `{"error":"encode event"}`, http.StatusInternalServerError)
		return
	}
	value, _ := json.Marshal(ev)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	partition, offset, _, err := s.kafka.produceAndRead(ctx, "movie-events", ev.ID, value)
	if err != nil {
		http.Error(w, `{"error":"kafka error: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	respondProduced(w, "movie-events", ev, partition, offset)
}

func (s *Server) handleUserEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in UserEvent
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return
	}
	if in.UserID == 0 || in.Action == "" || in.Timestamp == "" {
		http.Error(w, `{"error":"user_id, action, timestamp are required"}`, http.StatusBadRequest)
		return
	}
	ev, err := toEvent("user", in)
	if err != nil {
		http.Error(w, `{"error":"encode event"}`, http.StatusInternalServerError)
		return
	}
	value, _ := json.Marshal(ev)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	partition, offset, _, err := s.kafka.produceAndRead(ctx, "user-events", ev.ID, value)
	if err != nil {
		http.Error(w, `{"error":"kafka error: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	respondProduced(w, "user-events", ev, partition, offset)
}

func (s *Server) handlePaymentEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var in PaymentEvent
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
		return
	}
	if in.PaymentID == 0 || in.UserID == 0 || in.Status == "" || in.Timestamp == "" {
		http.Error(w, `{"error":"payment_id, user_id, status, timestamp are required"}`, http.StatusBadRequest)
		return
	}
	ev, err := toEvent("payment", in)
	if err != nil {
		http.Error(w, `{"error":"encode event"}`, http.StatusInternalServerError)
		return
	}
	value, _ := json.Marshal(ev)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	partition, offset, _, err := s.kafka.produceAndRead(ctx, "payment-events", ev.ID, value)
	if err != nil {
		http.Error(w, `{"error":"kafka error: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	respondProduced(w, "payment-events", ev, partition, offset)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("starting events-service")

	cfg := loadConfig(logger)
	k, err := newKafka(logger, cfg.KafkaBrokers)
	if err != nil {
		logger.Error("kafka connect failed", "error", err)
		os.Exit(1)
	}
	defer k.Close()

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      NewServer(logger, cfg, k),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	logger.Info("http listen", "addr", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logger.Error("server error", "error", err)
	}
}

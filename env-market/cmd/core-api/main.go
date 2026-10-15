package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Environment struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Type      string    `json:"type"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Command struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Type   string `json:"type"`
	Action string `json:"action"`
}

type Event struct {
	ID      string `json:"id"`
	Type    string `json:"type"`
	Status  string `json:"status"`
	Action  string `json:"action"`
	Message string `json:"message,omitempty"`
}

var (
	db    *pgxpool.Pool
	kafka *kgo.Client
)

func main() {
	ctx := context.Background()

	dbDSN := getenv("DB_DSN", "postgres://envuser:envpass@localhost:5432/envmarket?sslmode=disable")
	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")

	var err error
	for attempt := 1; attempt <= 30; attempt++ {
		db, err = pgxpool.New(ctx, dbDSN)
		if err == nil {
			err = db.Ping(ctx)
			if err == nil {
				break
			}
		}

		log.Printf("waiting for postgres, attempt %d: %v", attempt, err)
		time.Sleep(2 * time.Second)
	}

	if err != nil {
		log.Fatalf("cannot connect to postgres: %v", err)
	}
	defer db.Close()

	kafka, err = kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("core-api"),
		kgo.ConsumeTopics("env-events"),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.Fatalf("cannot create kafka client: %v", err)
	}
	defer kafka.Close()

	go consumeEvents(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/", serveIndex)
	mux.HandleFunc("/api/environments", handleEnvironments)
	mux.HandleFunc("/api/environments/", handleEnvironmentByID)

	addr := getenv("HTTP_ADDR", ":8080")
	log.Printf("core-api listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	http.ServeFile(w, r, "index.html")
}

func handleEnvironments(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		listEnvironments(w, r)
	case http.MethodPost:
		createEnvironment(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func listEnvironments(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	rows, err := db.Query(ctx, `
		SELECT id::text, name, type, status, created_at, updated_at
		FROM environments
		ORDER BY created_at DESC
	`)
	if err != nil {
		http.Error(w, "database error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	environments := make([]Environment, 0)

	for rows.Next() {
		var env Environment
		if err := rows.Scan(&env.ID, &env.Name, &env.Type, &env.Status, &env.CreatedAt, &env.UpdatedAt); err != nil {
			http.Error(w, "scan error: "+err.Error(), http.StatusInternalServerError)
			return
		}

		environments = append(environments, env)
	}

	if err := rows.Err(); err != nil {
		http.Error(w, "rows error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, environments)
}

func createEnvironment(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var request struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}

	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	request.Name = strings.TrimSpace(request.Name)

	if request.Name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	if !validType(request.Type) {
		http.Error(w, "type must be one of: k8s, vm, bm", http.StatusBadRequest)
		return
	}

	env := Environment{
		ID:     uuid.NewString(),
		Name:   request.Name,
		Type:   request.Type,
		Status: "CREATING",
	}

	err := db.QueryRow(ctx, `
		INSERT INTO environments (id, name, type, status)
		VALUES ($1, $2, $3, 'CREATING')
		RETURNING created_at, updated_at
	`, env.ID, env.Name, env.Type).Scan(&env.CreatedAt, &env.UpdatedAt)

	if err != nil {
		http.Error(w, "database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	command := Command{
		ID:     env.ID,
		Name:   env.Name,
		Type:   env.Type,
		Action: "CREATE",
	}

	if err := produceCommand(ctx, command); err != nil {
		log.Printf("failed to produce CREATE command for %s: %v", env.ID, err)
		http.Error(w, "kafka error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusAccepted, env)
}

func handleEnvironmentByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/environments/")
	if id == "" {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	deleteEnvironment(w, r, id)
}

func deleteEnvironment(w http.ResponseWriter, r *http.Request, id string) {
	ctx := r.Context()

	var name string
	var envType string
	var status string

	err := db.QueryRow(ctx, `
		UPDATE environments
		SET status = 'DELETING', updated_at = NOW()
		WHERE id = $1 AND status <> 'DELETED'
		RETURNING name, type, status
	`, id).Scan(&name, &envType, &status)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			http.Error(w, "environment not found", http.StatusNotFound)
			return
		}

		http.Error(w, "database error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	command := Command{
		ID:     id,
		Name:   name,
		Type:   envType,
		Action: "DELETE",
	}

	if err := produceCommand(ctx, command); err != nil {
		log.Printf("failed to produce DELETE command for %s: %v", id, err)
		http.Error(w, "kafka error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]string{
		"id":     id,
		"status": status,
	})
}

func produceCommand(ctx context.Context, command Command) error {
	payload, err := json.Marshal(command)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic: "env-commands",
		Key:   []byte(command.ID),
		Value: payload,
	}

	return kafka.ProduceSync(ctx, record).FirstErr()
}

func consumeEvents(ctx context.Context) {
	for {
		fetches := kafka.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("kafka fetch error topic=%s partition=%d: %v", topic, partition, err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			var event Event
			if err := json.Unmarshal(record.Value, &event); err != nil {
				log.Printf("cannot unmarshal env-event: %v", err)
				return
			}

			if event.ID == "" || event.Status == "" {
				return
			}

			_, err := db.Exec(ctx, `
				UPDATE environments
				SET status = $1, updated_at = NOW()
				WHERE id = $2
			`, event.Status, event.ID)

			if err != nil {
				log.Printf("cannot update environment %s to %s: %v", event.ID, event.Status, err)
				return
			}

			log.Printf("environment %s updated to %s", event.ID, event.Status)
		})
	}
}

func validType(value string) bool {
	switch value {
	case "k8s", "vm", "bm":
		return true
	default:
		return false
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func getenv(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

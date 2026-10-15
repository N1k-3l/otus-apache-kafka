package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

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

func main() {
	workerType := os.Getenv("WORKER_TYPE")
	if !validWorkerType(workerType) {
		log.Fatalf("WORKER_TYPE must be one of: k8s, vm, bm; got %q", workerType)
	}

	brokers := strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ",")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("worker-"+workerType),
		kgo.ConsumeTopics("env-commands"),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		log.Fatalf("cannot create kafka client: %v", err)
	}
	defer client.Close()

	log.Printf("worker %s started", workerType)

	ctx := context.Background()

	for {
		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("worker %s kafka fetch error topic=%s partition=%d: %v", workerType, topic, partition, err)
		})

		fetches.EachRecord(func(record *kgo.Record) {
			var command Command
			if err := json.Unmarshal(record.Value, &command); err != nil {
				log.Printf("worker %s cannot unmarshal command: %v", workerType, err)
				return
			}

			if command.Type != workerType {
				return
			}

			processCommand(ctx, client, workerType, command)
		})
	}
}

func processCommand(ctx context.Context, client *kgo.Client, workerType string, command Command) {
	log.Printf(
		"worker %s got command action=%s env=%s name=%s type=%s",
		workerType,
		command.Action,
		command.ID,
		command.Name,
		command.Type,
	)

	for progress := 20; progress <= 100; progress += 20 {
		time.Sleep(1 * time.Second)
		log.Printf("worker %s env=%s action=%s progress=%d%%", workerType, command.ID, command.Action, progress)
	}

	status := "ACTIVE"
	if command.Action == "DELETE" {
		status = "DELETED"
	}

	event := Event{
		ID:      command.ID,
		Type:    command.Type,
		Status:  status,
		Action:  command.Action,
		Message: "simulated by worker-" + workerType,
	}

	payload, err := json.Marshal(event)
	if err != nil {
		log.Printf("worker %s cannot marshal event: %v", workerType, err)
		return
	}

	result := client.ProduceSync(ctx, &kgo.Record{
		Topic: "env-events",
		Key:   []byte(command.ID),
		Value: payload,
	})

	if err := result.FirstErr(); err != nil {
		log.Printf("worker %s cannot produce event for env=%s: %v", workerType, command.ID, err)
		return
	}

	log.Printf("worker %s sent event env=%s status=%s", workerType, command.ID, status)
}

func validWorkerType(value string) bool {
	switch value {
	case "k8s", "vm", "bm":
		return true
	default:
		return false
	}
}

func getenv(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}

	return fallback
}

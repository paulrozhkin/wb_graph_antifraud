package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"

	"wb-graph/services/graph-service/internal/config"
	"wb-graph/services/graph-service/internal/entity"
	"wb-graph/services/graph-service/internal/repositories"
)

func Run(ctx context.Context, cfg config.Kafka, repo *repositories.GraphRepository) {
	for ctx.Err() == nil {
		err := consume(ctx, cfg, repo)
		if ctx.Err() != nil {
			return
		}
		log.Printf("Kafka consumer stopped: %v; retrying in %s", err, cfg.RetryInterval)
		timer := time.NewTimer(cfg.RetryInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

func consume(ctx context.Context, cfg config.Kafka, repo *repositories.GraphRepository) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.Broker}, Topic: cfg.Topic, GroupID: cfg.GroupID, StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("fetch Kafka message: %w", err)
		}
		if err := record(ctx, repo, reader, message, cfg.CommitTimeout); err != nil {
			return fmt.Errorf("process %s partition %d offset %d: %w", message.Topic, message.Partition, message.Offset, err)
		}
	}
}

func record(ctx context.Context, repo *repositories.GraphRepository, reader *kafka.Reader, message kafka.Message, commitTimeout time.Duration) error {
	var transfer entity.Transfer
	if err := json.Unmarshal(message.Value, &transfer); err != nil {
		return fmt.Errorf("decode transfer: %w", err)
	}
	if strings.TrimSpace(transfer.TransactionID) == "" ||
		strings.TrimSpace(transfer.FromAccountID) == "" ||
		strings.TrimSpace(transfer.ToAccountID) == "" ||
		transfer.Time.IsZero() || transfer.Time.Nanosecond()%int(time.Millisecond) != 0 ||
		transfer.AmountKopecks <= 0 {
		return fmt.Errorf("transfer requires IDs, a timestamp with millisecond precision or coarser, and a positive amount")
	}

	outcome, err := repo.RecordTransfer(ctx, transfer)
	if err != nil {
		return fmt.Errorf("write transfer: %w", err)
	}
	commitCtx, cancel := context.WithTimeout(ctx, commitTimeout)
	defer cancel()
	if err := reader.CommitMessages(commitCtx, message); err != nil {
		return fmt.Errorf("commit offset after %s transfer: %w", outcome, err)
	}
	log.Printf("transfer %q %s, committed partition %d offset %d", transfer.TransactionID, outcome, message.Partition, message.Offset)
	return nil
}

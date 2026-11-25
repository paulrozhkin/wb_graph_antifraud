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

type Handler func(context.Context, kafka.Message) (string, error)

func Run(ctx context.Context, cfg config.Kafka, groupID string, handle Handler) {
	for ctx.Err() == nil {
		err := consume(ctx, cfg, groupID, handle)
		if ctx.Err() != nil {
			return
		}
		log.Printf("Kafka group %s: %v; retrying in %s", groupID, err, cfg.RetryInterval)
		timer := time.NewTimer(cfg.RetryInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

func consume(ctx context.Context, cfg config.Kafka, groupID string, handle Handler) error {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{cfg.Broker}, Topic: cfg.Topic, GroupID: groupID, StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return fmt.Errorf("fetch Kafka message: %w", err)
		}
		outcome, err := handle(ctx, message)
		if err != nil {
			return fmt.Errorf("process %s partition %d offset %d: %w", message.Topic, message.Partition, message.Offset, err)
		}
		commitCtx, cancel := context.WithTimeout(ctx, cfg.CommitTimeout)
		err = reader.CommitMessages(commitCtx, message)
		cancel()
		if err != nil {
			return fmt.Errorf("commit %s partition %d offset %d after %s: %w", message.Topic, message.Partition, message.Offset, outcome, err)
		}
		log.Printf("%s, committed partition %d offset %d (group %s)", outcome, message.Partition, message.Offset, groupID)
	}
}

func RecordTransfer(ctx context.Context, repo *repositories.GraphRepository, message kafka.Message) (string, error) {
	var transfer entity.Transfer
	if err := json.Unmarshal(message.Value, &transfer); err != nil {
		return "", fmt.Errorf("decode transfer: %w", err)
	}
	if strings.TrimSpace(transfer.TransactionID) == "" ||
		strings.TrimSpace(transfer.FromAccountID) == "" ||
		strings.TrimSpace(transfer.ToAccountID) == "" ||
		transfer.Time.IsZero() || transfer.Time.Nanosecond()%int(time.Millisecond) != 0 ||
		transfer.AmountKopecks <= 0 {
		return "", fmt.Errorf("transfer requires IDs, a timestamp with millisecond precision or coarser, and a positive amount")
	}

	outcome, err := repo.RecordTransfer(ctx, transfer)
	if err != nil {
		return "", fmt.Errorf("write transfer: %w", err)
	}
	return fmt.Sprintf("transfer %q %s", transfer.TransactionID, outcome), nil
}

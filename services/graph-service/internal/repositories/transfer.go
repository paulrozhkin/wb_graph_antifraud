package repositories

import (
	"context"
	_ "embed"
	"fmt"

	"wb-graph/services/graph-service/internal/entity"
)

//go:embed gremlin/record_transfer.groovy
var recordTransferQuery string

func (r *GraphRepository) RecordTransfer(ctx context.Context, transfer entity.Transfer) (string, error) {
	bindings := map[string]any{
		"transactionId": transfer.TransactionID,
		"fromAccountId": transfer.FromAccountID,
		"toAccountId":   transfer.ToAccountID,
		"timeMillis":    transfer.Time.UnixMilli(),
		"amountKopecks": transfer.AmountKopecks,
	}
	var result []string
	if err := r.execute(ctx, recordTransferQuery, bindings, &result); err != nil {
		return "", err
	}
	if len(result) != 1 {
		return "", fmt.Errorf("unexpected Gremlin result for transfer %q", transfer.TransactionID)
	}
	switch result[0] {
	case "created", "replayed":
		return result[0], nil
	case "conflict":
		return "", fmt.Errorf("transfer %q already exists with different data", transfer.TransactionID)
	default:
		return "", fmt.Errorf("unexpected Gremlin result for transfer %q: %q", transfer.TransactionID, result[0])
	}
}

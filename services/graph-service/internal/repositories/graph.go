package repositories

import (
	"context"
	_ "embed"
	"fmt"

	"wb-graph/services/graph-service/internal/entity"
)

//go:embed gremlin/graph_snapshot.groovy
var graphSnapshotQuery string

const (
	graphAccountLimit  = 250
	graphTransferLimit = 500
)

func (r *GraphRepository) Snapshot(ctx context.Context) (entity.GraphSnapshot, error) {
	bindings := map[string]any{
		"accountScanLimit":  graphAccountLimit + 1,
		"transferScanLimit": graphTransferLimit + 1,
	}
	var rows []struct {
		Accounts  []entity.Account `json:"accounts"`
		Transfers []transferRow    `json:"transfers"`
	}
	if err := r.execute(ctx, graphSnapshotQuery, bindings, &rows); err != nil {
		return entity.GraphSnapshot{}, err
	}
	if len(rows) != 1 || rows[0].Accounts == nil || rows[0].Transfers == nil {
		return entity.GraphSnapshot{}, fmt.Errorf("unexpected Gremlin response for graph snapshot")
	}
	row := rows[0]
	if len(row.Accounts) > graphAccountLimit || len(row.Transfers) > graphTransferLimit {
		return entity.GraphSnapshot{}, entity.ErrGraphTooLarge
	}

	accountsByID := make(map[string]struct{}, len(row.Accounts))
	for _, account := range row.Accounts {
		accountsByID[account.ID] = struct{}{}
	}
	for _, transfer := range row.Transfers {
		_, fromExists := accountsByID[transfer.FromAccountID]
		_, toExists := accountsByID[transfer.ToAccountID]
		if !fromExists || !toExists {
			return entity.GraphSnapshot{}, fmt.Errorf("graph snapshot has a missing endpoint for transfer %q", transfer.TransactionID)
		}
	}
	return entity.GraphSnapshot{Accounts: row.Accounts, Transfers: decodeTransfers(row.Transfers)}, nil
}

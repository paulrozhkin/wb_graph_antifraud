package repositories

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"wb-graph/services/graph-service/internal/entity"
)

//go:embed gremlin/account_history.groovy
var historyQuery string

type GraphRepository struct {
	endpoint string
	client   *http.Client
}

func NewGraphRepository(endpoint string, client *http.Client) *GraphRepository {
	return &GraphRepository{endpoint: endpoint, client: client}
}

// Untyped GraphSON encodes java.util.Date as Unix milliseconds.
type transferRow struct {
	entity.Transfer
	Millis int64 `json:"time"`
}

func (r *GraphRepository) History(ctx context.Context, accountID string) ([]entity.Transfer, error) {
	var rows []transferRow
	if err := r.execute(ctx, historyQuery, map[string]any{"accountId": accountID}, &rows); err != nil {
		return nil, err
	}
	return decodeTransfers(rows), nil
}

func decodeTransfers(rows []transferRow) []entity.Transfer {
	transfers := make([]entity.Transfer, 0, len(rows))
	for _, row := range rows {
		row.Transfer.Time = time.UnixMilli(row.Millis).UTC()
		transfers = append(transfers, row.Transfer)
	}
	return transfers
}

func (r *GraphRepository) execute(ctx context.Context, script string, bindings map[string]any, data any) error {
	body, err := json.Marshal(map[string]any{
		"gremlin":  script,
		"bindings": bindings,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/vnd.gremlin-v1.0+json;types=false")
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Gremlin HTTP status %d", resp.StatusCode)
	}

	var result struct {
		Status struct {
			Code int
		}
		Result struct {
			Data json.RawMessage
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode Gremlin response: %w", err)
	}
	if result.Status.Code != http.StatusOK || len(result.Result.Data) == 0 || bytes.Equal(result.Result.Data, []byte("null")) {
		return fmt.Errorf("unexpected Gremlin result (status %d)", result.Status.Code)
	}
	if err := json.Unmarshal(result.Result.Data, data); err != nil {
		return fmt.Errorf("decode Gremlin result: %w", err)
	}
	return nil
}

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

type HistoryRepository struct {
	endpoint string
	client   *http.Client
}

func NewHistoryRepository(endpoint string, client *http.Client) *HistoryRepository {
	return &HistoryRepository{endpoint: endpoint, client: client}
}

func (r *HistoryRepository) History(ctx context.Context, accountID string) ([]entity.Transfer, error) {
	body, err := json.Marshal(map[string]any{
		"gremlin":  historyQuery,
		"bindings": map[string]string{"accountId": accountID},
	})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/vnd.gremlin-v1.0+json;types=false")
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Gremlin HTTP status %d", resp.StatusCode)
	}

	// Untyped GraphSON encodes java.util.Date as Unix milliseconds.
	var result struct {
		Status struct{ Code int }
		Result struct {
			Data []struct {
				entity.Transfer
				Millis int64 `json:"time"`
			}
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode Gremlin response: %w", err)
	}
	if result.Status.Code != http.StatusOK || result.Result.Data == nil {
		return nil, fmt.Errorf("unexpected Gremlin result (status %d)", result.Status.Code)
	}
	transfers := make([]entity.Transfer, 0, len(result.Result.Data))
	for _, row := range result.Result.Data {
		row.Transfer.Time = time.UnixMilli(row.Millis).UTC()
		transfers = append(transfers, row.Transfer)
	}
	return transfers, nil
}

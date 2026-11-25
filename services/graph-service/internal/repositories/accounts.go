package repositories

import (
	"context"
	_ "embed"

	"wb-graph/services/graph-service/internal/entity"
)

//go:embed gremlin/search_accounts.groovy
var searchAccountsQuery string

func (r *GraphRepository) SearchAccounts(ctx context.Context, prefix string) ([]entity.Account, error) {
	accounts := []entity.Account{}
	if err := r.execute(ctx, searchAccountsQuery, map[string]any{"prefix": prefix}, &accounts); err != nil {
		return nil, err
	}
	return accounts, nil
}

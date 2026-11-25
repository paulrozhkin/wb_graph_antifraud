package entity

import "time"

type Transfer struct {
	TransactionID string    `json:"transaction_id"`
	FromAccountID string    `json:"from_account_id"`
	ToAccountID   string    `json:"to_account_id"`
	Time          time.Time `json:"time"`
	AmountKopecks int64     `json:"amount_kopecks"`
}

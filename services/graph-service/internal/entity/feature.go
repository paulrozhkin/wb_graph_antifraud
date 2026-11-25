package entity

import (
	"errors"
	"time"
)

var ErrFeatureNotPublished = errors.New("recipient feature is not published")

type RecipientFeature struct {
	FeatureDate      string    `json:"feature_date"`
	PublishedAt      time.Time `json:"published_at"`
	UniqueRecipients *int64    `json:"unique_recipients"`
}

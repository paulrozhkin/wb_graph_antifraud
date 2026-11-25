package repositories

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"slices"
	"strings"
	"time"

	"wb-graph/services/graph-service/internal/entity"
	"wb-graph/services/graph-service/internal/webhdfs"
)

const servingFilename = "_recipients.jsonl"

type FeatureRepository struct {
	hdfs    *webhdfs.Client
	root    string
	timeout time.Duration
}

func NewFeatureRepository(hdfs *webhdfs.Client, featureRoot string, timeout time.Duration) *FeatureRepository {
	return &FeatureRepository{hdfs: hdfs, root: path.Join(featureRoot, "unique-recipients"), timeout: timeout}
}

func (r *FeatureRepository) UniqueRecipients(ctx context.Context, accountID string) (entity.RecipientFeature, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	dates, err := r.hdfs.List(ctx, r.root)
	if err != nil {
		return entity.RecipientFeature{}, err
	}
	slices.SortFunc(dates, func(left, right webhdfs.Entry) int {
		return strings.Compare(right.Name, left.Name)
	})
	for _, date := range dates {
		if date.Type != "DIRECTORY" {
			continue
		}
		if _, err := time.Parse(time.DateOnly, date.Name); err != nil {
			continue
		}
		dateDir := path.Join(r.root, date.Name)
		runs, err := r.hdfs.List(ctx, dateDir)
		if err != nil {
			return entity.RecipientFeature{}, err
		}
		slices.SortFunc(runs, func(left, right webhdfs.Entry) int {
			if byPublication := cmp.Compare(right.ModificationMillis, left.ModificationMillis); byPublication != 0 {
				return byPublication
			}
			return strings.Compare(right.Name, left.Name)
		})
		for _, run := range runs {
			if run.Type != "DIRECTORY" {
				continue
			}
			runDir := path.Join(dateDir, run.Name)
			ready, err := r.isPublished(ctx, runDir)
			if err != nil {
				return entity.RecipientFeature{}, err
			}
			if ready {
				count, err := r.readCount(ctx, path.Join(runDir, servingFilename), accountID)
				if err != nil {
					return entity.RecipientFeature{}, err
				}
				return entity.RecipientFeature{
					FeatureDate:      date.Name,
					PublishedAt:      time.UnixMilli(run.ModificationMillis).UTC(),
					UniqueRecipients: count,
				}, nil
			}
		}
	}
	return entity.RecipientFeature{}, entity.ErrFeatureNotPublished
}

func (r *FeatureRepository) isPublished(ctx context.Context, directory string) (bool, error) {
	entries, err := r.hdfs.List(ctx, directory)
	if err != nil {
		return false, err
	}
	var success, serving bool
	for _, entry := range entries {
		if entry.Type != "FILE" {
			continue
		}
		success = success || entry.Name == "_SUCCESS"
		serving = serving || entry.Name == servingFilename
	}
	return success && serving, nil
}

func (r *FeatureRepository) readCount(ctx context.Context, filename, accountID string) (*int64, error) {
	file, err := r.hdfs.Open(ctx, filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	for {
		var row struct {
			AccountID        string `json:"account_id"`
			UniqueRecipients *int64 `json:"unique_recipients"`
		}
		err := decoder.Decode(&row)
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("decode recipient features: %w", err)
		}
		if row.AccountID == accountID {
			if row.UniqueRecipients == nil || *row.UniqueRecipients < 0 {
				return nil, fmt.Errorf("invalid recipient count for account %q", accountID)
			}
			return row.UniqueRecipients, nil
		}
	}
}

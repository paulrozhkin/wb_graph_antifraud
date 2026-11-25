package repositories

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"wb-graph/services/graph-service/internal/webhdfs"
)

func TestUniqueRecipientsChoosesLatestServingRun(t *testing.T) {
	const root = "/webhdfs/v1/features/unique-recipients"
	const latest = root + "/2026-11-26"
	const published = latest + "/older-id"
	complete := []webhdfs.Entry{{Name: "_SUCCESS", Type: "FILE"}, {Name: servingFilename, Type: "FILE"}}
	folders := map[string][]webhdfs.Entry{
		root: {
			{Name: "2026-11-25", Type: "DIRECTORY"},
			{Name: "_staging", Type: "DIRECTORY"},
			{Name: "2026-11-27", Type: "DIRECTORY"},
			{Name: "2026-11-26", Type: "DIRECTORY"},
		},
		root + "/2026-11-27":        {{Name: "legacy", Type: "DIRECTORY"}},
		root + "/2026-11-27/legacy": {{Name: "_SUCCESS", Type: "FILE"}},
		latest: {
			{Name: "newer-id", Type: "DIRECTORY", ModificationMillis: 10},
			{Name: "unfinished", Type: "DIRECTORY", ModificationMillis: 30},
			{Name: "older-id", Type: "DIRECTORY", ModificationMillis: 20},
		},
		latest + "/unfinished": {{Name: servingFilename, Type: "FILE"}},
		latest + "/newer-id":   complete,
		published:              complete,
	}
	servingFile := published + "/" + servingFilename
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch op := r.URL.Query().Get("op"); {
		case op == "LISTSTATUS":
			if files, ok := folders[r.URL.Path]; ok {
				_ = json.NewEncoder(w).Encode(map[string]any{"FileStatuses": map[string]any{"FileStatus": files}})
				return
			}
		case r.URL.Path == servingFile && op == "OPEN":
			http.Redirect(w, r, servingFile+"?op=DATA", http.StatusTemporaryRedirect)
			return
		case r.URL.Path == servingFile && op == "DATA":
			_, _ = io.WriteString(w, "{\"account_id\":\"other\",\"unique_recipients\":7}\n{\"account_id\":\"demo-sender\",\"unique_recipients\":3}\n")
			return
		}
		t.Errorf("unexpected WebHDFS request: %s", r.URL)
		http.NotFound(w, r)
	}))
	defer server.Close()

	hdfs, err := webhdfs.New(server.URL, "hadoop")
	if err != nil {
		t.Fatal(err)
	}
	repo := NewFeatureRepository(hdfs, "/features", time.Second)
	got, err := repo.UniqueRecipients(context.Background(), "demo-sender")
	if err != nil {
		t.Fatal(err)
	}
	if got.FeatureDate != "2026-11-26" || got.PublishedAt.UnixMilli() != 20 ||
		got.UniqueRecipients == nil || *got.UniqueRecipients != 3 {
		t.Fatalf("unexpected feature: %+v", got)
	}
	got, err = repo.UniqueRecipients(context.Background(), "missing")
	if err != nil || got.FeatureDate != "2026-11-26" || got.UniqueRecipients != nil {
		t.Errorf("missing account: feature=%+v err=%v", got, err)
	}
}

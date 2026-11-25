package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"wb-graph/services/graph-service/internal/config"
)

func TestRecordPublishesOneFilePerOffset(t *testing.T) {
	const destination = "/archive/kafka/transfers.v1/partition-2/00000000000000000042.json"
	var mu sync.Mutex
	var staged, published []byte
	uploads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		params := r.URL.Query()
		switch params.Get("op") {
		case "GETFILESTATUS":
			if r.URL.Path != "/webhdfs/v1"+destination {
				t.Errorf("unexpected destination: %s", r.URL.Path)
			}
			if published == nil {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			io.WriteString(w, `{"FileStatus":{"type":"FILE"}}`)
		case "MKDIRS":
			io.WriteString(w, `{"boolean":true}`)
		case "CREATE":
			if params.Get("upload") == "" {
				http.Redirect(w, r, r.URL.Path+"?op=CREATE&upload=1", http.StatusTemporaryRedirect)
				return
			}
			var err error
			staged, err = io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read upload: %v", err)
			}
			uploads++
			w.WriteHeader(http.StatusCreated)
		case "RENAME":
			if params.Get("destination") != destination || staged == nil {
				t.Errorf("rename before upload or to unexpected destination: %s", params.Get("destination"))
				io.WriteString(w, `{"boolean":false}`)
				return
			}
			published, staged = staged, nil
			io.WriteString(w, `{"boolean":true}`)
		default:
			http.Error(w, "unexpected WebHDFS op", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	writer, err := New(config.HDFS{URL: server.URL, Root: "/archive/kafka", User: "hadoop", Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	msg := kafka.Message{Topic: "transfers.v1", Partition: 2, Offset: 42, Value: []byte{0, 0xff, 0x12}}
	for range 2 {
		if _, err := writer.Record(context.Background(), msg); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if uploads != 1 {
		t.Fatalf("uploaded %d files for one offset", uploads)
	}
	var archived record
	if err := json.Unmarshal(published, &archived); err != nil {
		t.Fatal(err)
	}
	if archived.Topic != msg.Topic || archived.Partition != msg.Partition ||
		archived.Offset != msg.Offset || !bytes.Equal(archived.Value, msg.Value) {
		t.Errorf("unexpected archive: %+v", archived)
	}
}

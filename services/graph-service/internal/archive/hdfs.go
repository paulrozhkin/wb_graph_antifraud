package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/segmentio/kafka-go"

	"wb-graph/services/graph-service/internal/webhdfs"
)

type Writer struct {
	client  *webhdfs.Client
	root    string
	timeout time.Duration
}

func New(client *webhdfs.Client, root string, timeout time.Duration) *Writer {
	return &Writer{client: client, root: root, timeout: timeout}
}

type record struct {
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"kafka_timestamp"`
	Key       []byte    `json:"key_base64"`
	Value     []byte    `json:"value_base64"`
}

func (w *Writer) Record(ctx context.Context, message kafka.Message) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	dir := path.Join(w.root, message.Topic, fmt.Sprintf("partition-%d", message.Partition))
	filename := fmt.Sprintf("%020d.json", message.Offset)
	final := path.Join(dir, filename)
	exists, err := w.fileExists(ctx, final)
	if err != nil {
		return "", err
	}
	if exists {
		return "archive already contains " + final, nil
	}

	document, err := json.Marshal(record{
		Topic: message.Topic, Partition: message.Partition, Offset: message.Offset,
		Timestamp: message.Time.UTC(), Key: message.Key, Value: message.Value,
	})
	if err != nil {
		return "", fmt.Errorf("encode archive record: %w", err)
	}
	if err := w.putBoolean(ctx, dir, "MKDIRS", url.Values{"permission": {"755"}}); err != nil {
		return "", err
	}
	staging := path.Join(dir, "."+filename+".pending")
	if err := w.upload(ctx, staging, append(document, '\n')); err != nil {
		return "", err
	}
	if err := w.putBoolean(ctx, staging, "RENAME", url.Values{"destination": {final}}); err != nil {
		return "", err
	}
	return "archived " + final, nil
}

func (w *Writer) fileExists(ctx context.Context, filename string) (bool, error) {
	response, err := w.client.Request(ctx, http.MethodGet, filename, "GETFILESTATUS", nil)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if response.StatusCode != http.StatusOK {
		return false, webhdfs.StatusError("GETFILESTATUS", response)
	}
	var result struct {
		FileStatus struct{ Type string }
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return false, fmt.Errorf("decode HDFS status: %w", err)
	}
	if result.FileStatus.Type != "FILE" {
		return false, fmt.Errorf("HDFS path %s is not a file", filename)
	}
	return true, nil
}

func (w *Writer) upload(ctx context.Context, staging string, document []byte) error {
	redirect, err := w.client.Request(ctx, http.MethodPut, staging, "CREATE", url.Values{"overwrite": {"true"}})
	if err != nil {
		return err
	}
	defer redirect.Body.Close()
	if redirect.StatusCode != http.StatusTemporaryRedirect {
		return webhdfs.StatusError("CREATE at NameNode", redirect)
	}
	response, err := w.client.Follow(ctx, redirect, http.MethodPut, bytes.NewReader(document))
	if err != nil {
		return fmt.Errorf("upload to DataNode: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return webhdfs.StatusError("CREATE at DataNode", response)
	}
	return nil
}

func (w *Writer) putBoolean(ctx context.Context, filename, operation string, params url.Values) error {
	response, err := w.client.Request(ctx, http.MethodPut, filename, operation, params)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return webhdfs.StatusError(operation, response)
	}
	var result struct{ Boolean bool }
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode HDFS %s response: %w", operation, err)
	}
	if !result.Boolean {
		return fmt.Errorf("HDFS %s returned false for %s", operation, filename)
	}
	return nil
}

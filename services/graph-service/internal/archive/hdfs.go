package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/segmentio/kafka-go"

	"wb-graph/services/graph-service/internal/config"
)

type Writer struct {
	cfg      config.HDFS
	endpoint *url.URL
	client   *http.Client
}

func New(cfg config.HDFS) (*Writer, error) {
	endpoint, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, err
	}
	client := &http.Client{
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return &Writer{cfg: cfg, endpoint: endpoint, client: client}, nil
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
	ctx, cancel := context.WithTimeout(ctx, w.cfg.Timeout)
	defer cancel()

	dir := path.Join(w.cfg.Root, message.Topic, fmt.Sprintf("partition-%d", message.Partition))
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
	response, err := w.request(ctx, http.MethodGet, filename, "GETFILESTATUS", url.Values{})
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if response.StatusCode != http.StatusOK {
		return false, statusError("GETFILESTATUS", response)
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
	redirect, err := w.request(ctx, http.MethodPut, staging, "CREATE", url.Values{"overwrite": {"true"}})
	if err != nil {
		return err
	}
	defer redirect.Body.Close()
	if redirect.StatusCode != http.StatusTemporaryRedirect {
		return statusError("CREATE at NameNode", redirect)
	}
	destination, err := redirect.Location()
	if err != nil {
		return fmt.Errorf("read HDFS upload location: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, destination.String(), bytes.NewReader(document))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/octet-stream")
	response, err := w.client.Do(request)
	if err != nil {
		return fmt.Errorf("upload to DataNode: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		return statusError("CREATE at DataNode", response)
	}
	return nil
}

func (w *Writer) putBoolean(ctx context.Context, filename, operation string, params url.Values) error {
	response, err := w.request(ctx, http.MethodPut, filename, operation, params)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return statusError(operation, response)
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

func (w *Writer) request(ctx context.Context, method, filename, operation string, params url.Values) (*http.Response, error) {
	target := w.endpoint.JoinPath("webhdfs/v1", filename)
	params.Set("op", operation)
	params.Set("user.name", w.cfg.User)
	target.RawQuery = params.Encode()
	request, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, err
	}
	response, err := w.client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("HDFS %s: %w", operation, err)
	}
	return response, nil
}

func statusError(operation string, response *http.Response) error {
	var result struct {
		RemoteException struct{ Message string }
	}
	_ = json.NewDecoder(io.LimitReader(response.Body, 8192)).Decode(&result)
	return fmt.Errorf("HDFS %s returned %s: %s", operation, response.Status, result.RemoteException.Message)
}

package webhdfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

type Entry struct {
	Name               string `json:"pathSuffix"`
	Type               string `json:"type"`
	ModificationMillis int64  `json:"modificationTime"`
}

func (c *Client) List(ctx context.Context, directory string) ([]Entry, error) {
	response, err := c.Request(ctx, http.MethodGet, directory, "LISTSTATUS", nil)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if response.StatusCode != http.StatusOK {
		return nil, StatusError("LISTSTATUS", response)
	}
	var result struct {
		FileStatuses struct{ FileStatus []Entry }
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode HDFS directory %s: %w", directory, err)
	}
	return result.FileStatuses.FileStatus, nil
}

func (c *Client) Open(ctx context.Context, filename string) (io.ReadCloser, error) {
	redirect, err := c.Request(ctx, http.MethodGet, filename, "OPEN", nil)
	if err != nil {
		return nil, err
	}
	defer redirect.Body.Close()
	if redirect.StatusCode != http.StatusTemporaryRedirect {
		return nil, StatusError("OPEN at NameNode", redirect)
	}
	response, err := c.Follow(ctx, redirect, http.MethodGet, nil)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		defer response.Body.Close()
		return nil, StatusError("OPEN at DataNode", response)
	}
	return response.Body, nil
}

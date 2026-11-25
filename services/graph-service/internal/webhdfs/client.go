package webhdfs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type Client struct {
	endpoint *url.URL
	user     string
	http     *http.Client
}

func New(endpoint, user string) (*Client, error) {
	base, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	return &Client{
		endpoint: base,
		user:     user,
		http: &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}, nil
}

func (c *Client) Request(ctx context.Context, method, filename, operation string, params url.Values) (*http.Response, error) {
	if params == nil {
		params = url.Values{}
	}
	target := c.endpoint.JoinPath("webhdfs/v1", filename)
	params.Set("op", operation)
	params.Set("user.name", c.user)
	target.RawQuery = params.Encode()
	request, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, err
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("HDFS %s: %w", operation, err)
	}
	return response, nil
}

func (c *Client) Follow(ctx context.Context, redirect *http.Response, method string, body io.Reader) (*http.Response, error) {
	destination, err := redirect.Location()
	if err != nil {
		return nil, fmt.Errorf("read HDFS redirect: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, method, destination.String(), body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/octet-stream")
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("request to HDFS DataNode: %w", err)
	}
	return response, nil
}

func StatusError(operation string, response *http.Response) error {
	var result struct {
		RemoteException struct{ Message string }
	}
	_ = json.NewDecoder(io.LimitReader(response.Body, 8192)).Decode(&result)
	return fmt.Errorf("HDFS %s returned %s: %s", operation, response.Status, result.RemoteException.Message)
}

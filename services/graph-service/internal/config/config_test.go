package config

import (
	"testing"
	"time"
)

func TestLoadAppliesEnvironmentOverrides(t *testing.T) {
	t.Chdir("../..")
	t.Setenv("HTTP_ADDR", ":9000")
	t.Setenv("WB_GRAPH_HTTP_ADDR", ":8080")
	t.Setenv("WB_GRAPH_KAFKA_GROUP_ID", "test-group")
	t.Setenv("WB_GRAPH_KAFKA_RETRY_INTERVAL", "2s")
	t.Setenv("WB_GRAPH_GREMLIN_URL", "")
	t.Setenv("GREMLIN_URL", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Gremlin.URL != "http://127.0.0.1:18182" {
		t.Errorf("Gremlin URL from JSON = %q", cfg.Gremlin.URL)
	}
	if cfg.HTTP.Addr != ":8080" {
		t.Errorf("HTTP address from prefixed environment = %q", cfg.HTTP.Addr)
	}
	if cfg.Kafka.GroupID != "test-group" || cfg.Kafka.RetryInterval != 2*time.Second {
		t.Errorf("Kafka overrides = %q, %s", cfg.Kafka.GroupID, cfg.Kafka.RetryInterval)
	}
}

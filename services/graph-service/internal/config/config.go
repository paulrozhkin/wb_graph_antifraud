package config

import (
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

type Config struct {
	HTTP    HTTP    `mapstructure:"http"`
	Gremlin Gremlin `mapstructure:"gremlin"`
	Kafka   Kafka   `mapstructure:"kafka"`
}

type HTTP struct {
	Addr              string        `mapstructure:"addr" validate:"required"`
	ReadHeaderTimeout time.Duration `mapstructure:"readHeaderTimeout" validate:"gt=0"`
	WriteTimeout      time.Duration `mapstructure:"writeTimeout" validate:"gt=0"`
	IdleTimeout       time.Duration `mapstructure:"idleTimeout" validate:"gt=0"`
	ShutdownTimeout   time.Duration `mapstructure:"shutdownTimeout" validate:"gt=0"`
}

type Gremlin struct {
	URL     string        `mapstructure:"url" validate:"http_url"`
	Timeout time.Duration `mapstructure:"timeout" validate:"gt=0"`
}

type Kafka struct {
	Broker        string        `mapstructure:"broker" validate:"required"`
	Topic         string        `mapstructure:"topic" validate:"required"`
	GroupID       string        `mapstructure:"groupId" validate:"required"`
	CommitTimeout time.Duration `mapstructure:"commitTimeout" validate:"gt=0"`
	RetryInterval time.Duration `mapstructure:"retryInterval" validate:"gt=0"`
}

func Load() (Config, error) {
	v := viper.New()
	v.SetConfigName("config")
	v.SetConfigType("json")
	v.AddConfigPath(".")
	v.AddConfigPath("./config")
	v.AddConfigPath("./services/graph-service/config")

	envs := map[string][]string{
		"http.addr":              {"WB_GRAPH_HTTP_ADDR", "HTTP_ADDR"},
		"http.readHeaderTimeout": {"WB_GRAPH_HTTP_READ_HEADER_TIMEOUT"},
		"http.writeTimeout":      {"WB_GRAPH_HTTP_WRITE_TIMEOUT"},
		"http.idleTimeout":       {"WB_GRAPH_HTTP_IDLE_TIMEOUT"},
		"http.shutdownTimeout":   {"WB_GRAPH_HTTP_SHUTDOWN_TIMEOUT"},
		"gremlin.url":            {"WB_GRAPH_GREMLIN_URL", "GREMLIN_URL"},
		"gremlin.timeout":        {"WB_GRAPH_GREMLIN_TIMEOUT"},
		"kafka.broker":           {"WB_GRAPH_KAFKA_BROKER", "KAFKA_BROKER"},
		"kafka.topic":            {"WB_GRAPH_KAFKA_TOPIC", "KAFKA_TOPIC"},
		"kafka.groupId":          {"WB_GRAPH_KAFKA_GROUP_ID", "KAFKA_GROUP_ID"},
		"kafka.commitTimeout":    {"WB_GRAPH_KAFKA_COMMIT_TIMEOUT"},
		"kafka.retryInterval":    {"WB_GRAPH_KAFKA_RETRY_INTERVAL"},
	}
	for key, names := range envs {
		if err := v.BindEnv(append([]string{key}, names...)...); err != nil {
			return Config{}, fmt.Errorf("bind config key %s: %w", key, err)
		}
	}

	if err := v.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("read config.json: %w", err)
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if err := validator.New().Struct(cfg); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

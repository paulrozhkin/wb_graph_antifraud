package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpserver "wb-graph/services/graph-service/internal/delivery/http"
	"wb-graph/services/graph-service/internal/repositories"
)

func main() {
	endpoint := os.Getenv("GREMLIN_URL")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:18182"
	}
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = "127.0.0.1:18080"
	}
	repo := repositories.NewHistoryRepository(endpoint, &http.Client{Timeout: 6 * time.Second})
	server := &http.Server{
		Addr: addr, Handler: httpserver.NewRouter(repo),
		ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: time.Minute,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.ListenAndServe() }()
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			log.Fatal(err)
		}
	}
}

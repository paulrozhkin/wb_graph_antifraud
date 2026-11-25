package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"

	"wb-graph/services/graph-service/internal/archive"
	"wb-graph/services/graph-service/internal/config"
	"wb-graph/services/graph-service/internal/consumer"
	httpserver "wb-graph/services/graph-service/internal/delivery/http"
	"wb-graph/services/graph-service/internal/repositories"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}
	if err := run(ctx, cfg); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, cfg config.Config) error {
	repo := repositories.NewGraphRepository(cfg.Gremlin.URL, &http.Client{Timeout: cfg.Gremlin.Timeout})
	archiveWriter, err := archive.New(cfg.HDFS)
	if err != nil {
		return fmt.Errorf("configure HDFS archive: %w", err)
	}
	server := &http.Server{
		Addr:              cfg.HTTP.Addr,
		Handler:           httpserver.NewRouter(repo),
		ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout,
		WriteTimeout:      cfg.HTTP.WriteTimeout,
		IdleTimeout:       cfg.HTTP.IdleTimeout,
	}
	group, runCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("serve HTTP: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		consumer.Run(runCtx, cfg.Kafka, cfg.Kafka.GroupID, func(ctx context.Context, message kafka.Message) (string, error) {
			return consumer.RecordTransfer(ctx, repo, message)
		})
		return nil
	})
	group.Go(func() error {
		consumer.Run(runCtx, cfg.Kafka, cfg.Kafka.ArchiveGroupID, archiveWriter.Record)
		return nil
	})
	group.Go(func() error {
		<-runCtx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), cfg.HTTP.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdown); err != nil {
			_ = server.Close()
			return fmt.Errorf("shut down HTTP server: %w", err)
		}
		return nil
	})
	return group.Wait()
}

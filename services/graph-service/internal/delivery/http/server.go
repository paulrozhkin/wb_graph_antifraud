package httpserver

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"wb-graph/services/graph-service/internal/entity"
)

type GraphReader interface {
	History(context.Context, string) ([]entity.Transfer, error)
	SearchAccounts(context.Context, string) ([]entity.Account, error)
	Snapshot(context.Context) (entity.GraphSnapshot, error)
}

func NewRouter(reader GraphReader) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())
	router.StaticFile("/", "web/index.html")
	router.Static("/assets", "web/assets")
	router.GET("/api/v1/graph", func(c *gin.Context) {
		snapshot, err := reader.Snapshot(c.Request.Context())
		if errors.Is(err, entity.ErrGraphTooLarge) {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "graph exceeds visualization limits"})
			return
		}
		if err != nil {
			writeGraphError(c, err, "graph unavailable")
			return
		}
		c.JSON(http.StatusOK, snapshot)
	})
	router.GET("/api/v1/accounts", func(c *gin.Context) {
		prefix := strings.TrimSpace(c.Query("q"))
		length := utf8.RuneCountInString(prefix)
		if length < 2 || length > 100 || strings.ContainsFunc(prefix, unicode.IsSpace) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "q must be a single prefix of 2 to 100 characters"})
			return
		}
		accounts, err := reader.SearchAccounts(c.Request.Context(), prefix)
		if err != nil {
			writeGraphError(c, err, "account search unavailable")
			return
		}
		c.JSON(http.StatusOK, accounts)
	})
	router.GET("/api/v1/accounts/:account_id/transfers", func(c *gin.Context) {
		transfers, err := reader.History(c.Request.Context(), c.Param("account_id"))
		if err != nil {
			writeGraphError(c, err, "transfer history unavailable")
			return
		}
		c.JSON(http.StatusOK, transfers)
	})
	return router
}

func writeGraphError(c *gin.Context, err error, message string) {
	log.Printf("%s: %v", message, err)
	status := http.StatusBadGateway
	if errors.Is(err, context.DeadlineExceeded) {
		status = http.StatusGatewayTimeout
	}
	c.JSON(status, gin.H{"error": message})
}

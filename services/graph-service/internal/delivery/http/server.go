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
}

func NewRouter(reader GraphReader) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())
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

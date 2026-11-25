package httpserver

import (
	"context"
	"errors"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"wb-graph/services/graph-service/internal/entity"
)

type HistoryReader interface {
	History(context.Context, string) ([]entity.Transfer, error)
}

func NewRouter(reader HistoryReader) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/api/v1/accounts/:account_id/transfers", func(c *gin.Context) {
		transfers, err := reader.History(c.Request.Context(), c.Param("account_id"))
		if err != nil {
			log.Printf("read transfer history: %v", err)
			status := http.StatusBadGateway
			if errors.Is(err, context.DeadlineExceeded) {
				status = http.StatusGatewayTimeout
			}
			c.JSON(status, gin.H{"error": "transfer history unavailable"})
			return
		}
		c.JSON(http.StatusOK, transfers)
	})
	return router
}

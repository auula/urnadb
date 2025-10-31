package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func Error404Handler(ctx *gin.Context) {
	ctx.JSON(http.StatusNotFound, gin.H{
		"message": "Oops! 404 Not Found!",
	})
}

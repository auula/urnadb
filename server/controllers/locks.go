package controllers

import (
	"errors"
	"net/http"

	"github.com/auula/urnadb/server/response"
	"github.com/auula/urnadb/server/services"
	"github.com/auula/urnadb/utils"
	"github.com/gin-gonic/gin"
)

type AcquireLockRequest struct {
	TTLSeconds int64 `json:"ttl" binding:"required"`
}

type LeaseLockRequest struct {
	Token string `json:"token" binding:"required"`
}

func NewLockController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	var req AcquireLockRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	slock, err := ls.AcquireLock(name, req.TTLSeconds)
	if err != nil {
		handlerLocksError(ctx, err)
		return
	}

	defer slock.ReleaseToPool()

	ctx.IndentedJSON(http.StatusCreated, response.Ok(gin.H{
		"token": slock.Token,
	}))
}

func DeleteLockController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	var req LeaseLockRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	err = ls.ReleaseLock(name, req.Token)
	if err != nil {
		handlerLocksError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok("deleted lock successfully."))
}

func DoLeaseLockController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	var req LeaseLockRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	slock, err := ls.DoLeaseLock(name, req.Token)
	if err != nil {
		handlerLocksError(ctx, err)
		return
	}

	defer slock.ReleaseToPool()

	ctx.IndentedJSON(http.StatusCreated, response.Ok(gin.H{
		"token": slock.Token,
	}))
}

func handlerLocksError(ctx *gin.Context, err error) {
	switch {
	case errors.Is(err, services.ErrInvalidToken):
		ctx.IndentedJSON(http.StatusForbidden, response.Fail(err.Error()))
	case errors.Is(err, services.ErrLockNotFound):
		ctx.IndentedJSON(http.StatusNotFound, response.Fail(err.Error()))
	case errors.Is(err, services.ErrAlreadyLocked):
		ctx.IndentedJSON(http.StatusLocked, response.Fail(err.Error()))
	default:
		ctx.IndentedJSON(http.StatusInternalServerError, response.Fail(err.Error()))
	}
}

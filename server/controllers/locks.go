package controllers

import (
	"errors"
	"net/http"

	"github.com/auula/urnadb/server/response"
	"github.com/auula/urnadb/server/services"
	"github.com/auula/urnadb/utils"
	"github.com/gin-gonic/gin"
)

func NewLockController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail("empty key param."))
		return
	}

	type RequestBody struct {
		TTL int64 `json:"ttl" binding:"required"`
	}

	var req RequestBody
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	slock, err := ls.AcquireLock(name, req.TTL)
	if err != nil {
		if errors.Is(err, services.ErrInvalidToken) {
			ctx.IndentedJSON(http.StatusForbidden, response.Fail(err.Error()))
		} else if errors.Is(err, services.ErrLockNotFound) {
			ctx.IndentedJSON(http.StatusNotFound, response.Fail(err.Error()))
		} else if errors.Is(err, services.ErrAlreadyLocked) {
			ctx.IndentedJSON(http.StatusLocked, response.Fail(err.Error()))
		} else {
			ctx.IndentedJSON(http.StatusInternalServerError, response.Fail(err.Error()))
		}
		return
	}

	ctx.IndentedJSON(http.StatusCreated, response.Ok(gin.H{
		"token": slock.Token,
	}))
}

func DeleteLockController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail("empty key param."))
		return
	}

	type RequestBody struct {
		Token string `json:"token" binding:"required"`
	}

	var req RequestBody
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	err = ls.ReleaseLock(name, req.Token)
	if err != nil {
		ctx.IndentedJSON(http.StatusInternalServerError, response.Fail(err.Error()))
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok("deleted lock successfully."))
}

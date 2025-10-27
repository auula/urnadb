package controllers

import (
	"errors"
	"net/http"

	"github.com/auula/urnadb/server/response"
	"github.com/auula/urnadb/server/services"
	"github.com/auula/urnadb/utils"
	"github.com/gin-gonic/gin"
)

func NewLeaseController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.JSON(http.StatusBadRequest, response.Fail("empty key param."))
		return
	}

	slock, err := ls.AcquireLock(name, 0)
	if err != nil {
		if errors.Is(err, services.ErrInvalidToken) {
			ctx.JSON(http.StatusForbidden, response.Fail(err.Error()))
		} else if errors.Is(err, services.ErrLockNotFound) {
			ctx.JSON(http.StatusNotFound, response.Fail(err.Error()))
		} else if errors.Is(err, services.ErrAlreadyLocked) {
			ctx.JSON(http.StatusLocked, response.Fail(err.Error()))
		} else {
			ctx.JSON(http.StatusInternalServerError, response.Fail(err.Error()))
		}
		return
	}

	ctx.IndentedJSON(http.StatusCreated, response.Ok(gin.H{
		"token": slock.Token,
	}))
}

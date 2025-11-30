// Copyright 2022 Leon Ding <ding_ms@outlook.com> https://urnadb.github.io

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
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

	ctx.IndentedJSON(http.StatusCreated, response.Ok("lock created successfully", gin.H{
		"token": slock.Token,
	}))
}

func DeleteLockController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
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

	ctx.IndentedJSON(http.StatusOK, response.Ok("lock deleted successfully", nil))
}

func DoLeaseLockController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
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

	ctx.IndentedJSON(http.StatusCreated, response.Ok("lease acquired successfully", gin.H{
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

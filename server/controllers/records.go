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
	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/utils"
	"github.com/gin-gonic/gin"
)

func GetRecordsController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	rd, err := rs.GetRecord(name)
	if err != nil {
		ctx.IndentedJSON(http.StatusInternalServerError, response.Fail(
			err.Error(),
		))
	}

	defer rd.ReleaseToPool()

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"record": rd.Record,
	}))
}

type CreateRecordRequest struct {
	Record     map[string]any `json:"record" binding:"required"`
	TTLSeconds int64          `json:"ttl" binding:"omitempty"`
}

func PutRecordsController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	var req CreateRecordRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		handlerRecordsError(ctx, err)
		return
	}

	rd := types.AcquireRecord()
	rd.Record = req.Record

	defer rd.ReleaseToPool()

	err = rs.CreateRecord(name, rd, req.TTLSeconds)
	if err != nil {
		handlerRecordsError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"message": "record created successfully.",
	}))
}

func DeleteRecordsController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	err := rs.DeleteRecord(name)
	if err != nil {
		handlerRecordsError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"message": "record deleted successfully.",
	}))
}

type SearchRecordRequest struct {
	Column string `json:"column" binding:"required"`
}

func SearchRecordsController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	var req SearchRecordRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		handlerRecordsError(ctx, err)
		return
	}

	res, err := rs.SearchRows(name, req.Column)
	if err != nil {
		handlerRecordsError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"column": res,
	}))
}

func handlerRecordsError(ctx *gin.Context, err error) {
	switch {
	case errors.Is(err, services.ErrRecordUpdateFailed):
		ctx.IndentedJSON(http.StatusConflict, response.Fail(err.Error()))
	case errors.Is(err, services.ErrRecordNotFound):
		ctx.IndentedJSON(http.StatusNotFound, response.Fail(err.Error()))
	case errors.Is(err, services.ErrRecordExpired):
		ctx.IndentedJSON(http.StatusGone, response.Fail(err.Error()))
	default:
		// 所有其他错误都统一返回 500 内部服务器错误
		ctx.IndentedJSON(http.StatusInternalServerError, response.Fail(err.Error()))
	}
}

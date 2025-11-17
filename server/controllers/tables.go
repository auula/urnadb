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
	"io"
	"net/http"

	"github.com/auula/urnadb/server/response"
	"github.com/auula/urnadb/server/services"
	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/utils"
	"github.com/gin-gonic/gin"
)

type CreateTableRequest struct {
	TTLSeconds int64 `json:"ttl" binding:"omitempty"`
}

func CreateTableController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
		return
	}

	var req CreateTableRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil && !errors.Is(err, io.EOF) {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	if req.TTLSeconds < 0 {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail("ttl cannot be negative."))
		return
	}

	err = ts.CreateTable(name, types.AcquireTable(), req.TTLSeconds)
	if err != nil {
		handlerTablesError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"message": "table created successfully.",
	}))
}

func DeleteTableController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
		return
	}

	err := ts.DeleteTable(name)
	if err != nil {
		handlerTablesError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"message": "table deleted successfully.",
	}))
}

func QueryTableController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
		return
	}

	tab, err := ts.GetTable(name)
	if err != nil {
		handlerTablesError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"table": tab.Table,
	}))
}

type PatchRowsRequest struct {
	Wheres map[string]any `json:"wheres" binding:"required"`
	Sets   map[string]any `json:"sets" binding:"required"`
}

func PatchRowsTableController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
		return
	}

	var req PatchRowsRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	err = ts.PatchRows(name, req.Wheres, req.Sets)
	if err != nil {
		handlerTablesError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"message": "table rows updated successfully.",
	}))
}

type QueryRowsRequest struct {
	Wheres map[string]any `json:"wheres" binding:"required"`
}

func QueryRowsTableController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
		return
	}

	var req QueryRowsRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	rows, err := ts.QueryRows(name, req.Wheres)
	if err != nil {
		handlerTablesError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"rows": rows,
	}))
}

func RemoveRowsTabelController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
		return
	}

	var req QueryRowsRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	err = ts.RemoveRows(name, req.Wheres)
	if err != nil {
		handlerTablesError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"message": "table rows remove successfully.",
	}))
}

type InsertRowsRequest struct {
	Rows map[string]any `json:"rows" binding:"required"`
}

func InsertRowsTableController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
		return
	}

	var req InsertRowsRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	id, err := ts.InsertRows(name, req.Rows)
	if err != nil {
		handlerTablesError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"t_id":    id,
		"message": "table rows insert successfully.",
	}))
}

func handlerTablesError(ctx *gin.Context, err error) {
	switch {
	case errors.Is(err, services.ErrTableAlreadyExists):
		ctx.IndentedJSON(http.StatusConflict, response.Fail(err.Error()))
	case errors.Is(err, services.ErrTableNotFound):
		ctx.IndentedJSON(http.StatusNotFound, response.Fail(err.Error()))
	case errors.Is(err, services.ErrTableExpired):
		ctx.IndentedJSON(http.StatusGone, response.Fail(err.Error()))
	default:
		// 所有其他错误都统一返回 500 内部服务器错误
		ctx.IndentedJSON(http.StatusInternalServerError, response.Fail(err.Error()))
	}
}

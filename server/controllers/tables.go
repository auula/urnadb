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

type CreateTableRequest struct {
	TTLSeconds int64 `json:"ttl" binding:"omitempty"`
}

func CreateTableController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	var req CreateTableRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	if req.TTLSeconds < 0 {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail("ttl cannot be negative."))
		return
	}

	err = ts.CreateTable(name, types.NewTable(), req.TTLSeconds)
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
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
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
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	tab, err := ts.GetTable(name)
	if err != nil {
		handlerTablesError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"table": tab,
	}))
}

type PatchRowsRequest struct {
	Wheres map[string]any `json:"wheres" binding:"required"`
	Sets   map[string]any `json:"sets" binding:"required"`
}

func PatchRowsTableController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
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

type RowsConditionsRequest struct {
	Wheres map[string]any `json:"wheres" binding:"required"`
}

func RemoveRowsTabelController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	var req RowsConditionsRequest
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
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
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

func QueryRowsTableController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	var req RowsConditionsRequest
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

func handlerTablesError(ctx *gin.Context, err error) {
	switch {
	case errors.Is(err, services.ErrTableAlreadyExists):
		ctx.IndentedJSON(http.StatusConflict, response.Fail(err.Error()))
	case errors.Is(err, services.ErrTableNotFound):
		ctx.IndentedJSON(http.StatusNotFound, response.Fail(err.Error()))
	case errors.Is(err, services.ErrTableExpired):
		ctx.IndentedJSON(http.StatusGone, response.Fail(err.Error()))
	default:
		// 所有其他错误（包括 TableCreateFailed, TableDropFailed, TableUpdateFailed 等）都统一返回 500 内部服务器错误
		ctx.IndentedJSON(http.StatusInternalServerError, response.Fail(err.Error()))
	}
}

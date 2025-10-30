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

func GetTableController(ctx *gin.Context) {
	tab, err := ts.QueryTable(ctx.Param("key"))
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	defer tab.ReleaseToPool()

	ctx.IndentedJSON(http.StatusOK, gin.H{
		"table": tab.Table,
	})
}

type CreateTableRequest struct {
	TTLSeconds int64 `json:"ttl"`
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

	ctx.IndentedJSON(http.StatusOK, nil)
}

func DeleteTableController(ctx *gin.Context) {

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

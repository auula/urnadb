package controllers

import (
	"net/http"

	"github.com/auula/urnadb/server/response"
	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/utils"
	"github.com/gin-gonic/gin"
)

func GetVariantController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	variant, err := vs.GetVariant(name)
	if err != nil {
		ctx.IndentedJSON(http.StatusInternalServerError, response.Fail(err.Error()))
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"variant": variant.Value,
	}))
}

type CreateVariantRequest struct {
	Value      any   `json:"variant" binding:"required"`
	TTLSeconds int64 `json:"ttl" binding:"omitempty"`
}

func CreateVariantController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	var req CreateVariantRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	new_variant := types.NewVariant(req.Value)

	if new_variant.IsVariant() {
		ctx.IndentedJSON(http.StatusInternalServerError, response.Fail(
			"only allow string, int, and float types.",
		))
		return
	}

	err = vs.SetVariant(name, new_variant, req.TTLSeconds)
	if err != nil {
		ctx.IndentedJSON(http.StatusOK, response.Fail(err.Error()))
		return
	}

	// 成功响应
	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"variant": new_variant.Value,
	}))
}

type MathVarianrRequest struct {
	Delta string `json:"delta" bingding:"required"`
}

// increment += -=
func MathVariantController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	var req MathVarianrRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	res_num, err := vs.Increment(name, req.Delta)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok(gin.H{
		"variant": res_num,
	}))
}

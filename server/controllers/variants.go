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

func DeleteVariantController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
		return
	}

	err := vs.DeleteVariant(name)
	if err != nil {
		handlerVariantsError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok("variant deleted successfully", nil))
}

func GetVariantController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
		return
	}

	variant, err := vs.GetVariant(name)
	if err != nil {
		handlerVariantsError(ctx, err)
		return
	}

	defer variant.ReleaseToPool()

	ctx.IndentedJSON(http.StatusOK, response.Ok("variant queried successfully", gin.H{
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
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
		return
	}

	var req CreateVariantRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail(err.Error()))
		return
	}

	new_variant := types.AcquireVariant()
	new_variant.Value = req.Value

	if !new_variant.IsVariant() {
		ctx.IndentedJSON(http.StatusInternalServerError, response.Fail(
			"only allow string, int, and float types",
		))
		return
	}

	defer new_variant.ReleaseToPool()

	err = vs.SetVariant(name, new_variant, req.TTLSeconds)
	if err != nil {
		handlerVariantsError(ctx, err)
		return
	}

	// 成功响应
	ctx.IndentedJSON(http.StatusOK, response.Ok("variant created successfully", gin.H{
		"variant": new_variant.Value,
	}))
}

type MathVariantRequest struct {
	Delta float64 `json:"delta" bingding:"required"`
}

// increment += -=
func MathVariantController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missKey)
		return
	}

	var req MathVariantRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		ctx.IndentedJSON(http.StatusBadRequest, response.Fail("delta must be a float or int type"))
		return
	}

	res_num, err := vs.Increment(name, req.Delta)
	if err != nil {
		handlerVariantsError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.Ok("variant incremented successfully", gin.H{
		"variant": res_num,
	}))
}

func handlerVariantsError(ctx *gin.Context, err error) {
	switch {
	case errors.Is(err, services.ErrVariantNotFound):
		ctx.IndentedJSON(http.StatusNotFound, response.Fail(err.Error()))
	case errors.Is(err, services.ErrVariantExpired):
		ctx.IndentedJSON(http.StatusGone, response.Fail(err.Error()))
	default:
		// 所有其他错误都统一返回 500 内部服务器错误
		ctx.IndentedJSON(http.StatusInternalServerError, response.Fail(err.Error()))
	}
}

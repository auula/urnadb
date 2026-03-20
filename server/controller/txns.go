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

package controller

import (
	"errors"
	"net/http"

	"github.com/auula/urnadb/server/response"
	"github.com/auula/urnadb/server/service"
	"github.com/gin-gonic/gin"
)

func TransactionController(ctx *gin.Context) {

}

func handlerTxnsError(ctx *gin.Context, err error) {
	switch {
	case errors.Is(err, service.ErrTableAlreadyExists):
		ctx.IndentedJSON(http.StatusConflict, response.FailJSON(err.Error()))
	case errors.Is(err, service.ErrTableNotFound):
		ctx.IndentedJSON(http.StatusNotFound, response.FailJSON(err.Error()))
	case errors.Is(err, service.ErrTableExpired):
		ctx.IndentedJSON(http.StatusGone, response.FailJSON(err.Error()))
	default:
		// 所有其他错误都统一返回 500 内部服务器错误
		ctx.IndentedJSON(http.StatusInternalServerError, response.FailJSON(err.Error()))
	}
}

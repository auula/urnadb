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
	"fmt"
	"net/http"

	"github.com/auula/urnadb/server/response"
	"github.com/auula/urnadb/server/service"
	"github.com/gin-gonic/gin"
)

var (
	operationTypeMap = map[string]int{
		"INSERT": 0,
		"UPDATE": 1,
		"DELETE": 2,
	}
)

type Mutation struct {
	Name      string         `json:"name" binding:"required"`
	Operation string         `json:"operation" binding:"required,oneof=INSERT UPDATE DELETE"`
	Where     map[string]any `json:"where,omitempty"`
	Values    map[string]any `json:"values,omitempty"`
}

type MutationsRequest struct {
	Mutations []*Mutation `json:"mutations" binding:"required"`
	// 开启这个就和 MySQL 中的事物隔离 Isolation 最高级别一样
	// 如果值是 false 就是类似于 PGSQL 的 MVCC 提高并发效率
	Serialization bool `json:"serialization"`
}

func (m *Mutation) Validated() error {
	switch m.Operation {
	case "INSERT":
		if m.Values == nil {
			return errors.New("INSERT requires values")
		}
	case "UPDATE":
		if m.Where == nil || m.Values == nil {
			return errors.New("UPDATE requires where contiton and values")
		}
	case "DELETE":
		if m.Where == nil {
			return errors.New("DELETE requires where contiton")
		}
	default:
		return fmt.Errorf("unsupported operation type: %s", m.Operation)
	}
	return nil
}

func TransactionController(ctx *gin.Context) {
	var req MutationsRequest
	err := ctx.ShouldBindJSON(&req)
	if err != nil {
		inner := fmt.Errorf("invalid or empty request body: %w", err)
		ctx.IndentedJSON(http.StatusBadRequest, response.FailJSON(inner.Error()))
		return
	}

	for _, mutaction := range req.Mutations {
		err := mutaction.Validated()
		if err != nil {
			ctx.IndentedJSON(http.StatusBadRequest, response.FailJSON(err.Error()))
			return
		}
	}

	err = ts.Transaction(req.buildTableMutation(), req.Serialization)
	if err != nil {
		handlerTxnsError(ctx, err)
		return
	}

	ctx.IndentedJSON(http.StatusOK, response.OkJSON("transaction execute successfully", nil))
}

func (req *MutationsRequest) buildTableMutation() []*service.TableMutation {
	var result []*service.TableMutation
	for _, mutaction := range req.Mutations {
		result = append(result, &service.TableMutation{
			Name:       mutaction.Name,
			Operation:  service.OperationType(operationTypeMap[mutaction.Operation]),
			Conditions: mutaction.Where,
			Data:       mutaction.Values,
		})
	}
	return result
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

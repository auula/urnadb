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
	"net/http"

	"github.com/auula/urnadb/server/response"
	"github.com/auula/urnadb/utils"
	"github.com/gin-gonic/gin"
)

func QueryController(ctx *gin.Context) {
	name := ctx.Param("key")
	if !utils.NotNullString(name) {
		ctx.IndentedJSON(http.StatusBadRequest, missingKeyParam)
		return
	}

	version, seg, err := qs.GetSegment(name)
	if err != nil {
		ctx.IndentedJSON(http.StatusNotFound, response.Fail(err.Error()))
		return
	}

	defer utils.ReleaseToPool(seg)
	ttl, _ := seg.ExpiresIn()

	ctx.IndentedJSON(http.StatusOK, &gin.H{
		"type":  seg.GetTypeString(),
		"key":   seg.GetKeyString(),
		"value": seg.Value,
		"ttl":   ttl,
		"mvcc":  version,
	})
}

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

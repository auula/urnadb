package controllers

import (
	"net/http"

	"github.com/auula/urnadb/utils"
	"github.com/gin-gonic/gin"
)

func QueryController(ctx *gin.Context) {
	version, seg, err := qs.GetSegment(ctx.Param("key"))
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": err.Error(),
		})
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

package controllers

import (
	"net/http"

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

func PutTableController(ctx *gin.Context) {
	// key := ctx.Param("key")

	// tab := types.AcquireTable()
	// err := ctx.ShouldBindJSON(tab)
	// if err != nil {
	// 	utils.ReleaseToPool(tab)
	// 	ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
	// 	return
	// }

	// seg, err := vfs.AcquirePoolSegment(key, tab, tab.TTL)
	// if err != nil {
	// 	utils.ReleaseToPool(tab)
	// 	ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
	// 	return
	// }

	// defer utils.ReleaseToPool(tab, seg)
	// err = storage.PutSegment(key, seg)
	// if err != nil {
	// 	ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
	// 	return
	// }

	// ctx.JSON(http.StatusCreated, gin.H{
	// 	"message": "request processed succeed.",
	// })
}

func DeleteTableController(ctx *gin.Context) {
	// key := ctx.Param("key")

	// err := storage.DeleteSegment(key)
	// if err != nil {
	// 	ctx.JSON(http.StatusInternalServerError, gin.H{
	// 		"message": err.Error(),
	// 	})
	// 	return
	// }

	// ctx.JSON(http.StatusNoContent, gin.H{
	// 	"message": "delete data succeed.",
	// })
}

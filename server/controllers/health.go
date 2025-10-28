package controllers

import (
	"fmt"
	"net/http"

	"github.com/auula/urnadb/utils"
	"github.com/gin-gonic/gin"
)

type SystemInfo struct {
	KeyCount       uint64 `json:"key_count"`
	GCState        uint8  `json:"gc_state"`
	DiskFree       string `json:"disk_free"`
	DiskUsed       string `json:"disk_used"`
	DiskTotal      string `json:"disk_total"`
	MemoryFree     string `json:"mem_free"`
	MemoryTotal    string `json:"mem_total"`
	DiskPercent    string `json:"disk_percent"`
	SpaceTotalUsed string `json:"space_total"`
}

func GetHealthController(ctx *gin.Context) {
	ctx.IndentedJSON(http.StatusOK, SystemInfo{
		GCState:        hs.RegionCompactStatus(),
		KeyCount:       hs.RegionInodeCount(),
		DiskFree:       fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetFreeDisk())),
		DiskUsed:       fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetUsedDisk())),
		DiskTotal:      fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetTotalDisk())),
		MemoryFree:     fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetFreeMemory())),
		MemoryTotal:    fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetTotalMemory())),
		SpaceTotalUsed: fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetTotalSpaceUsed())),
		DiskPercent:    fmt.Sprintf("%.2f%%", hs.GetDiskPercent()),
	})
}

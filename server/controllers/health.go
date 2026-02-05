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
	"fmt"
	"net/http"

	"github.com/auula/urnadb/server/response"
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

func HealthController(ctx *gin.Context) {
	ctx.IndentedJSON(http.StatusOK, response.OkJSON("server is healthy", SystemInfo{
		GCState:        hs.RegionCompactStatus(),
		KeyCount:       hs.RegionInodeCount(),
		DiskFree:       fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetFreeDisk())),
		DiskUsed:       fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetUsedDisk())),
		DiskTotal:      fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetTotalDisk())),
		MemoryFree:     fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetFreeMemory())),
		MemoryTotal:    fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetTotalMemory())),
		SpaceTotalUsed: fmt.Sprintf("%.2fGB", utils.BytesToGB(hs.GetTotalSpaceUsed())),
		DiskPercent:    fmt.Sprintf("%.2f%%", hs.GetDiskPercent()),
	}))
}

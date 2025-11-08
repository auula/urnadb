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

package services

import (
	"github.com/auula/urnadb/vfs"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

type HealthService struct {
	mem     *mem.VirtualMemoryStat
	disk    *disk.UsageStat
	storage *vfs.LogStructuredFS
}

func NewHealthService(storage *vfs.LogStructuredFS) *HealthService {
	mem, _ := mem.VirtualMemory()
	var diskUsage *disk.UsageStat
	if storage != nil {
		diskUsage, _ = disk.Usage(storage.GetDirectory())
	}
	return &HealthService{mem: mem, disk: diskUsage, storage: storage}
}

func (h *HealthService) RegionCompactStatus() uint8 {
	return h.storage.GCState()
}

func (h *HealthService) RegionInodeCount() uint64 {
	return h.storage.RefreshInodeCount()
}
func (h *HealthService) GetTotalSpaceUsed() uint64 {
	return h.storage.GetTotalSpaceUsed()
}

// GetTotalMemory returns the total system memory in bytes.
func (h *HealthService) GetTotalMemory() uint64 {
	return h.mem.Total
}

// GetFreeMemory returns the available system memory in bytes.
func (h *HealthService) GetFreeMemory() uint64 {
	return h.mem.Available
}

func (h *HealthService) GetUsedDisk() uint64 {
	return h.disk.Used
}

func (h *HealthService) GetFreeDisk() uint64 {
	return h.disk.Free
}

func (h *HealthService) GetTotalDisk() uint64 {
	return h.disk.Total
}

func (h *HealthService) GetDiskPercent() float64 {
	return h.disk.UsedPercent
}

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
	disk, _ := disk.Usage(storage.GetDirectory())
	return &HealthService{mem: mem, disk: disk, storage: storage}
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

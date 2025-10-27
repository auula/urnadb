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

package server

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/utils"
	"github.com/auula/urnadb/vfs"
	"github.com/gin-gonic/gin"
)

var (
	storage *vfs.LogStructuredFS
	// 每个租期锁的 Key 也有一把锁，这样降低并发获取锁阻塞的设计，
	// 这个设计类似于 JVM 中的对象锁，每个对象头上有一把锁。
	atomicLeaseLocks = new(sync.Map)
)

func acquireLeaseLock(key string) *sync.Mutex {
	actual, _ := atomicLeaseLocks.LoadOrStore(key, new(sync.Mutex))
	return actual.(*sync.Mutex)
}

func Error404Handler(ctx *gin.Context) {
	ctx.JSON(http.StatusNotFound, gin.H{
		"message": "Oops! 404 Not Found!",
	})
}

func GetCollectionController(ctx *gin.Context) {
	_, seg, err := storage.FetchSegment(ctx.Param("key"))
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": "key data not found.",
		})
		return
	}

	collection, err := seg.ToCollection()
	if err != nil {
		utils.ReleaseToPool(seg)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.IndentedJSON(http.StatusOK, gin.H{
		"collection": collection.Collection,
	})

	utils.ReleaseToPool(seg, collection)
}

func PutCollectionController(ctx *gin.Context) {
	key := ctx.Param("key")

	collection := types.AcquireCollection()
	err := ctx.ShouldBindJSON(collection)
	if err != nil {
		utils.ReleaseToPool(collection)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	seg, err := vfs.AcquirePoolSegment(key, collection, collection.TTL)
	if err != nil {
		utils.ReleaseToPool(collection)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	defer utils.ReleaseToPool(collection, seg)
	err = storage.PutSegment(key, seg)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	ctx.JSON(http.StatusCreated, gin.H{
		"message": "request processed succeed.",
	})
}

func DeleteCollectionController(ctx *gin.Context) {
	key := ctx.Param("key")

	err := storage.DeleteSegment(key)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusNoContent, gin.H{
		"message": "delete data succeed.",
	})
}

func GetTableController(ctx *gin.Context) {
	_, seg, err := storage.FetchSegment(ctx.Param("key"))
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": "key data not found.",
		})
		return
	}

	tab, err := seg.ToTable()
	if err != nil {
		utils.ReleaseToPool(seg)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.IndentedJSON(http.StatusOK, gin.H{
		"table": tab.Table,
	})

	utils.ReleaseToPool(seg, tab)
}

func PutTableController(ctx *gin.Context) {
	key := ctx.Param("key")

	tab := types.AcquireTable()
	err := ctx.ShouldBindJSON(tab)
	if err != nil {
		utils.ReleaseToPool(tab)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	seg, err := vfs.AcquirePoolSegment(key, tab, tab.TTL)
	if err != nil {
		utils.ReleaseToPool(tab)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	defer utils.ReleaseToPool(tab, seg)
	err = storage.PutSegment(key, seg)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	ctx.JSON(http.StatusCreated, gin.H{
		"message": "request processed succeed.",
	})
}

func DeleteTableController(ctx *gin.Context) {
	key := ctx.Param("key")

	err := storage.DeleteSegment(key)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusNoContent, gin.H{
		"message": "delete data succeed.",
	})
}

func GetZsetController(ctx *gin.Context) {
	_, seg, err := storage.FetchSegment(ctx.Param("key"))
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": "key data not found.",
		})
		return
	}

	zset, err := seg.ToZSet()
	if err != nil {
		utils.ReleaseToPool(seg)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.IndentedJSON(http.StatusOK, gin.H{
		"list": zset.ZSet,
	})

	utils.ReleaseToPool(seg, zset)
}

func PutZsetController(ctx *gin.Context) {
	key := ctx.Param("key")

	zset := types.AcquireZSet()
	err := ctx.ShouldBindJSON(zset)
	if err != nil {
		utils.ReleaseToPool(zset)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	seg, err := vfs.AcquirePoolSegment(key, zset, zset.TTL)
	if err != nil {
		utils.ReleaseToPool(zset)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	defer utils.ReleaseToPool(zset, seg)
	err = storage.PutSegment(key, seg)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	ctx.JSON(http.StatusCreated, gin.H{
		"message": "request processed succeed.",
	})
}

func DeleteZsetController(ctx *gin.Context) {
	key := ctx.Param("key")

	err := storage.DeleteSegment(key)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusNoContent, gin.H{
		"message": "delete data succeed.",
	})
}

func GetTextController(ctx *gin.Context) {
	_, seg, err := storage.FetchSegment(ctx.Param("key"))
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": "key data not found.",
		})
		return
	}

	text, err := seg.ToText()
	if err != nil {
		utils.ReleaseToPool(text)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.IndentedJSON(http.StatusOK, gin.H{
		"text": text.Content,
	})

	utils.ReleaseToPool(seg, text)
}

func PutTextController(ctx *gin.Context) {
	key := ctx.Param("key")

	text := types.AcquireText()
	err := ctx.ShouldBindJSON(text)
	if err != nil {
		utils.ReleaseToPool(text)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	seg, err := vfs.AcquirePoolSegment(key, text, text.TTL)

	if err != nil {
		utils.ReleaseToPool(text)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	// 到此才能完全放心的使用 defer 来释放资源
	defer utils.ReleaseToPool(seg, text)
	err = storage.PutSegment(key, seg)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	ctx.JSON(http.StatusCreated, gin.H{
		"message": "request processed succeed.",
	})
}

func DeleteTextController(ctx *gin.Context) {
	key := ctx.Param("key")

	err := storage.DeleteSegment(key)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusNoContent, gin.H{
		"message": "delete data succeed.",
	})
}

func GetNumberController(ctx *gin.Context) {
	_, seg, err := storage.FetchSegment(ctx.Param("key"))
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": "key data not found.",
		})
		return
	}

	number, err := seg.ToNumber()
	if err != nil {
		utils.ReleaseToPool(seg)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.IndentedJSON(http.StatusOK, gin.H{
		"number": number.Value,
	})

	utils.ReleaseToPool(seg, number)
}

func PutNumberController(ctx *gin.Context) {
	key := ctx.Param("key")

	number := types.AcquireNumber()
	err := ctx.ShouldBindJSON(number)
	if err != nil {
		utils.ReleaseToPool(number)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	seg, err := vfs.AcquirePoolSegment(key, number, number.TTL)
	if err != nil {
		utils.ReleaseToPool(number)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	defer utils.ReleaseToPool(number, seg)
	err = storage.PutSegment(key, seg)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	ctx.JSON(http.StatusCreated, gin.H{
		"message": "request processed succeed.",
	})

}

func DeleteNumberController(ctx *gin.Context) {
	key := ctx.Param("key")

	err := storage.DeleteSegment(key)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusNoContent, gin.H{
		"message": "delete data succeed.",
	})
}

func GetSetController(ctx *gin.Context) {
	_, seg, err := storage.FetchSegment(ctx.Param("key"))
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": "key data not found.",
		})
		return
	}

	set, err := seg.ToSet()
	if err != nil {
		utils.ReleaseToPool(seg)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusOK, gin.H{
		"set": set.Set,
	})

	utils.ReleaseToPool(seg, set)
}

func PutSetController(ctx *gin.Context) {
	key := ctx.Param("key")

	set := types.AcquireSet()
	err := ctx.ShouldBindJSON(set)
	if err != nil {
		utils.ReleaseToPool(set)
		ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		return
	}

	seg, err := vfs.AcquirePoolSegment(key, set, set.TTL)
	if err != nil {
		utils.ReleaseToPool(set)
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	defer utils.ReleaseToPool(set, seg)
	err = storage.PutSegment(key, seg)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"message": err.Error()})
		return
	}

	ctx.JSON(http.StatusCreated, gin.H{
		"message": "request processed succeed.",
	})
}

func DeleteSetController(ctx *gin.Context) {
	key := ctx.Param("key")

	err := storage.DeleteSegment(key)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.JSON(http.StatusNoContent, gin.H{
		"message": "delete data succeed.",
	})
}

func QueryController(ctx *gin.Context) {
	version, seg, err := storage.FetchSegment(ctx.Param("key"))
	if err != nil {
		ctx.JSON(http.StatusNotFound, gin.H{
			"message": err.Error(),
		})
		return
	}

	defer utils.ReleaseToPool(seg)
	ttl, _ := seg.ExpiresIn()

	ctx.IndentedJSON(http.StatusOK, gin.H{
		"type":  seg.GetTypeString(),
		"key":   seg.GetKeyString(),
		"value": seg.Value,
		"ttl":   ttl,
		"mvcc":  version,
	})
}

func NewLeaseController(ctx *gin.Context) {
	key := ctx.Param("key")
	if !utils.NotNullString(key) {
		ctx.JSON(http.StatusBadRequest, gin.H{
			"message": "missing or empty 'key' parameter.",
		})
		return
	}

	// 获取对应 key 的锁，颗粒度较细的锁
	slock := acquireLeaseLock(key)
	slock.Lock()
	defer slock.Unlock()

	// 存在则表示 key 锁已经存在，意味着同一把锁还没有过期，同一资源还未过期。
	if storage.HasSegment(key) {
		ctx.JSON(http.StatusLocked, gin.H{
			"message": fmt.Sprintf("resource '%s' is already locked.", key),
		})
		return
	}

	// 创建一把新租期锁并且设置锁的租期
	lease := types.AcquireLeaseLock()
	// 尝试创建 segment
	seg, err := vfs.AcquirePoolSegment(key, lease, vfs.ImmortalTTL)
	if err != nil {
		utils.ReleaseToPool(lease)
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	defer utils.ReleaseToPool(lease, seg)
	err = storage.PutSegment(key, seg)
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.IndentedJSON(http.StatusCreated, gin.H{
		"token": lease.Token,
	})
}

func GetHealthController(ctx *gin.Context) {
	health, err := newHealth(storage.GetDirectory())
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{
			"message": err.Error(),
		})
		return
	}

	ctx.IndentedJSON(http.StatusOK, SystemInfo{
		Version:        version,
		GCState:        storage.GCState(),
		KeyCount:       storage.RefreshInodeCount(),
		DiskFree:       fmt.Sprintf("%.2fGB", utils.BytesToGB(health.GetFreeDisk())),
		DiskUsed:       fmt.Sprintf("%.2fGB", utils.BytesToGB(health.GetUsedDisk())),
		DiskTotal:      fmt.Sprintf("%.2fGB", utils.BytesToGB(health.GetTotalDisk())),
		MemoryFree:     fmt.Sprintf("%.2fGB", utils.BytesToGB(health.GetFreeMemory())),
		MemoryTotal:    fmt.Sprintf("%.2fGB", utils.BytesToGB(health.GetTotalMemory())),
		SpaceTotalUsed: fmt.Sprintf("%.2fGB", utils.BytesToGB(storage.GetTotalSpaceUsed())),
		DiskPercent:    fmt.Sprintf("%.2f%%", health.GetDiskPercent()),
	})
}

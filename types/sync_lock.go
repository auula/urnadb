package types

import (
	"sync"

	"github.com/auula/urnadb/utils"
)

const (
	nullString = ""
	lenght     = 65
)

// 创建一个对象池
var syncLockPools = sync.Pool{
	New: func() any {
		return new(SyncLock)
	},
}

func init() {
	// 预先填充池中的对象，把对象放入池中
	for i := 0; i < 10; i++ {
		syncLockPools.Put(new(SyncLock))
	}
}

// SyncLock 定义了一个同步锁结构体
type SyncLock struct {
	// LockID 是锁的唯一标识，解锁的时候客户端需要提供相同的 LockID 才能解锁，除非锁已经过期。
	LockID string `json:"lock_id" msgpack:"lock_id"`
}

// NewSyncLock 创建一个新的 SyncLock 实例带有唯一的 LockID
func NewSyncLock() *SyncLock {
	return &SyncLock{
		LockID: utils.RandomString(lenght),
	}
}

// 从对象池获取一个 SyncLock ，内存被复用但是锁 ID 不会被复用
func AcquireSyncLock() *SyncLock {
	sl := syncLockPools.Get().(*SyncLock)
	sl.LockID = utils.RandomString(lenght)
	return sl
}

// 放回对象池，清理数据
func (sl *SyncLock) Clear() {
	sl.LockID = nullString
	syncLockPools.Put(sl)
}

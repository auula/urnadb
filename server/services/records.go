package services

import (
	"errors"
	"sync"

	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/vfs"
)

var (
	ErrRecordUpdateFailed = errors.New("failed to update record")
	ErrRecordNotFound     = errors.New("record not found")
)

// Record 通常直接映射编程语言中的 class 的一条记录，
// OOP 面向对象编程中的对象可以直接影响为 Record 记录，
// Record 和 Tables 区别，Record 是一条整体记录，Tables 是一组 Record 组成集合，
// 另外 Tables 随着不断插入新的数据，会导致 Tables 越来越大并且有锁的开销，多条记录共享一把 Tables 锁，
// 而 Record 一条记录对应一把锁，Record 一段创建就不能改了，提高并发性能。

// 定型的应用场景就是更新不频繁的数据，如果更新直接设置一条新的 Record 映射就可以。
type RecordsService interface {
	// 删除一条名为 name 的记录
	DeleteRecord(name string) error
	// 根据记录名获取到这条记录
	QueryRecord(name string) (*types.Record, error)
	// Record 一段创建就不可以更改其内容，要更改直接 PUT 新 Record 和 RUW 操作
	// // 更新记录中的某个字段
	// PatchRows(name string, data map[string]any) error
	// // 插入数据到一条记录里面
	// InsertRows(name string, data map[string]any) error
	// 创建一条名为 name 的记录
	CreateRecord(name string, record *types.Record, ttl int64) error
	// 根据字段搜索一条记录下的某个字段
	SearchRows(name string, column string) (map[string]any, error)
}

type RecordsServiceImpl struct {
	storage *vfs.LogStructuredFS
	rlock   sync.Map
}

// 获取或创建一个锁
func (rs *RecordsServiceImpl) acquireRecordLock(name string) *sync.RWMutex {
	val, _ := rs.rlock.LoadOrStore(name, new(sync.RWMutex))
	return val.(*sync.RWMutex)
}

// 创建记录
func (rs *RecordsServiceImpl) CreateRecord(name string, record *types.Record, ttl int64) error {
	return nil
}

// 查询记录
func (rs *RecordsServiceImpl) QueryRecord(name string) (*types.Record, error) {
	return nil, nil
}

// 删除记录
func (rs *RecordsServiceImpl) DeleteRecord(name string) error {
	return nil
}

// 根据条件查询字段（简单示例，只支持一层 map）
func (rs *RecordsServiceImpl) SearchRows(name string, column string) (map[string]any, error) {
	rs.acquireRecordLock(name).RLock()
	defer rs.acquireRecordLock(name).RUnlock()

	return nil, nil
}

func NewRecordsService(storage *vfs.LogStructuredFS) RecordsService {
	return &RecordsServiceImpl{
		storage: storage,
	}
}

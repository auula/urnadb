package services

import (
	"errors"
	"sync"

	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/vfs"
)

var ErrRecordUpdateFailed = errors.New("failed to update record")
var ErrRecordNotFound = errors.New("record not found")

type RecordsService interface {
	// 删除一条名为 name 的记录
	DeleteRecord(name string) error
	// 根据记录名获取到这条记录
	QueryRecord(name string) (*types.Record, error)
	// 删除一张表名为 name 的表的某个字段
	RemoveColumn(name string, column string) error
	// 更新记录中的某个字段
	PatchRows(name string, data map[string]any) error
	// 插入数据到一条记录里面
	InsertRows(name string, data map[string]any) error
	// 创建一条名为 name 的记录
	CreateRecord(name string, record *types.Record, ttl int64) error
	// 根据表名和条件查询搜索一条记录下的某个字段
	SelectRows(name string, wheres map[string]any) (map[string]any, error)
}

type RecordsServiceImpl struct {
	storage *vfs.LogStructuredFS
	rlock   sync.Map
}

// 获取或创建一个锁
func (s *RecordsServiceImpl) acquireRecordLock(name string) *sync.Mutex {
	val, _ := s.rlock.LoadOrStore(name, &sync.Mutex{})
	return val.(*sync.Mutex)
}

// 创建记录
func (s *RecordsServiceImpl) CreateRecord(name string, record *types.Record, ttl int64) error {
	return nil
}

// 查询记录
func (s *RecordsServiceImpl) QueryRecord(name string) (*types.Record, error) {
	return nil, nil
}

// 删除记录
func (s *RecordsServiceImpl) DeleteRecord(name string) error {
	return nil
}

// 删除字段
func (s *RecordsServiceImpl) RemoveColumn(name string, column string) error {
	return nil
}

// 更新字段（Patch）
func (s *RecordsServiceImpl) PatchRecordRows(name string, data map[string]any) error {
	return nil
}

// 插入一条新数据（Add）
func (s *RecordsServiceImpl) InsertRecordRows(name string, data map[string]any) error {
	return nil
}

// 根据条件查询字段（简单示例，只支持一层 map）
func (s *RecordsServiceImpl) SelectRecordRows(name string, wheres map[string]any) (map[string]any, error) {
	return nil, nil
}

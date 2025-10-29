package services

import (
	"errors"
	"sync"

	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/vfs"
)

var (
	// 表未找到
	ErrTableNotFound = errors.New("table not found.")
	// 创建表失败
	ErrTableCreateFailed = errors.New("failed to create table.")
	// 表已存在
	ErrTableAlreadyExists = errors.New("table already exists.")
	// 删除表失败
	ErrTableDropFailed = errors.New("failed to delete table.")
	// 更新表失败
	ErrTableUpdateFailed = errors.New("failed to update table.")
)

type TableService interface {
	// 返回存储层所有的表
	AllTables() []*types.Table
	// 根据表名获取到这种表
	QueryTable(name string) (*types.Table, error)
	// 删除一张表名为 name 的表
	DeleteTable(name string) error
	// 删除一张表名为 name 的表的某个字段
	RemoveColumn(name string, column string) error
	// 创建一张表名为 name 的表
	CreateTable(name string, table *types.Table, ttl int64) error
	// 更新表中的某个字段
	PatchRows(name string, data map[string]interface{}) error
	// 插入表数据到一张表里面
	InsertRows(name string, data map[string]interface{}) error
	// 根据表名和子查询条件搜索表
	SelectTableRows(name string, wheres map[string]interface{}) (map[string]interface{}, error)
}

type TableLFSServiceImpl struct {
	tlock   sync.Map
	storage *vfs.LogStructuredFS
}

func (t *TableLFSServiceImpl) AllTables() []*types.Table {
	return nil
}

func (t *TableLFSServiceImpl) QueryTable(name string) (*types.Table, error) {
	t.acquireTablesLock(name).Lock()
	defer t.acquireTablesLock(name).Unlock()

	_, seg, err := t.storage.FetchSegment(name)
	if err != nil {
		return nil, ErrTableNotFound
	}

	return seg.ToTable()
}

func (t *TableLFSServiceImpl) DeleteTable(name string) error {
	t.acquireTablesLock(name).Lock()
	defer t.acquireTablesLock(name).Unlock()

	err := t.storage.DeleteSegment(name)
	if err != nil {
		return ErrTableDropFailed
	}

	return nil
}

func (s *TableLFSServiceImpl) RemoveColumn(tableName string, column string) error {
	return nil
}

func (s *TableLFSServiceImpl) CreateTable(name string, table *types.Table, ttl int64) error {
	s.acquireTablesLock(name).Lock()
	defer s.acquireTablesLock(name).Unlock()

	if s.storage.HasSegment(name) {
		return ErrTableAlreadyExists
	}

	seg, err := vfs.AcquirePoolSegment(name, table, ttl)
	if err != nil {
		return ErrTableCreateFailed
	}

	defer seg.ReleaseToPool()

	return s.storage.PutSegment(name, seg)
}

func (s *TableLFSServiceImpl) InsertRows(name string, data map[string]interface{}) error {
	return nil
}

func (s *TableLFSServiceImpl) PatchRows(name string, data map[string]interface{}) error {
	return ErrTableUpdateFailed
}

func (s *TableLFSServiceImpl) SelectTableRows(name string, wheres map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
}

func NewTableLFSServiceImpl(storage *vfs.LogStructuredFS) TableService {
	return &TableLFSServiceImpl{
		storage: storage,
	}
}

func (s *TableLFSServiceImpl) acquireTablesLock(key string) *sync.RWMutex {
	actual, _ := s.tlock.LoadOrStore(key, new(sync.RWMutex))
	return actual.(*sync.RWMutex)
}

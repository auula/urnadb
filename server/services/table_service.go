package services

import (
	"errors"

	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/vfs"
)

var (
	ErrorTableNotFound = errors.New("table not found")
)

type TableService interface {
	// 返回存储层所有的表
	AllTables() []*types.Table
	// 根据表名获取到这种表
	QueryTable(name string) (*types.Table, error)
	// 删除一张表名为 name 的表
	DeleteTable(name string) error
	// 创建一张表名为 name 的表
	CreateTable(name string, table *types.Table) error
	// 删除一张表名为 name 的表的某个字段
	RemoveColumn(name string, column string) error
	// 插入表数据到一张表里面
	InsertRows(name string, data map[string]interface{}) error
	// 根据表名和子查询条件搜索表
	SelectTableRows(name string, wheres map[string]interface{}) (map[string]interface{}, error)
}

type TableLFSServiceImpl struct {
	storage *vfs.LogStructuredFS
}

func (t *TableLFSServiceImpl) AllTables() []*types.Table {
	return nil
}

func (t *TableLFSServiceImpl) QueryTable(name string) (*types.Table, error) {
	_, seg, err := t.storage.FetchSegment(name)
	if err != nil {
		return nil, ErrorTableNotFound
	}
	return seg.ToTable()
}

func (t *TableLFSServiceImpl) DeleteTable(name string) error {
	return t.storage.DeleteSegment(name)
}

func (s *TableLFSServiceImpl) RemoveColumn(tableName string, column string) error {
	return nil
}

func (s *TableLFSServiceImpl) CreateTable(name string, table *types.Table) error {
	return nil
}

func (s *TableLFSServiceImpl) InsertRows(name string, data map[string]interface{}) error {
	return nil
}

func (s *TableLFSServiceImpl) SelectTableRows(name string, wheres map[string]interface{}) (map[string]interface{}, error) {
	return nil, nil
}

func NewTableLFSServiceImpl(storage *vfs.LogStructuredFS) TableService {
	return &TableLFSServiceImpl{
		storage: storage,
	}
}

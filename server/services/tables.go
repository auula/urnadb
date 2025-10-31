package services

import (
	"errors"
	"sync"

	"github.com/auula/urnadb/clog"
	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/utils"
	"github.com/auula/urnadb/vfs"
)

var (
	// 操作过程中出现 Table 已经过期了
	ErrTableExpired = errors.New("table ttl is invalid or expired")
	// 表未找到
	ErrTableNotFound = errors.New("table not found")
	// 表已存在
	ErrTableAlreadyExists = errors.New("table already exists")
)

type TablesService interface {
	// 返回存储层所有的表
	AllTables() []*types.Table
	// 根据表名获取到这种表
	GetTable(name string) (*types.Table, error)
	// 删除一张表名为 name 的表
	DeleteTable(name string) error
	// 删除一行记录，有条件的删除
	RemoveRows(name string, condtitons map[string]any) error
	// 创建一张表名为 name 的表
	CreateTable(name string, table *types.Table, ttl int64) error
	// 更新表中的某个记录，有条件的更新
	PatchRows(name string, wheres, data map[string]any) error
	// 插入一行数据到一张表里面
	InsertRows(name string, rows map[string]any) (uint32, error)
	// 根据表名和子查询条件搜索表
	QueryRows(name string, wheres map[string]any) ([]map[string]any, error)
}

type TableLFSServiceImpl struct {
	tlock   sync.Map
	storage *vfs.LogStructuredFS
}

func (t *TableLFSServiceImpl) AllTables() []*types.Table {
	return nil
}

func (t *TableLFSServiceImpl) GetTable(name string) (*types.Table, error) {
	t.acquireTablesLock(name).RLock()
	defer t.acquireTablesLock(name).RUnlock()

	_, seg, err := t.storage.FetchSegment(name)
	if err != nil {
		clog.Errorf("Tables service get: %#v", err)
		return nil, ErrTableNotFound
	}

	return seg.ToTable()
}

func (t *TableLFSServiceImpl) DeleteTable(name string) error {
	t.acquireTablesLock(name).Lock()

	err := t.storage.DeleteSegment(name)
	if err != nil {
		t.acquireTablesLock(name).Unlock()
		clog.Errorf("Tables service delete: %#v", err)
		return err
	}

	t.acquireTablesLock(name).Unlock()
	t.tlock.Delete(name)

	return nil
}

func (s *TableLFSServiceImpl) RemoveRows(name string, condtitons map[string]any) error {
	s.acquireTablesLock(name).Lock()
	defer s.acquireTablesLock(name).Unlock()

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		return err
	}

	tab, err := seg.ToTable()
	if err != nil {
		clog.Errorf("Tables service remove rows: %#v", err)
		return err
	}

	defer utils.ReleaseToPool(tab, seg)

	// 从表里面删除一条记录
	tab.RemoveRows(condtitons)

	ttl, ok := seg.ExpiresIn()
	if !ok {
		return ErrTableExpired
	}

	seg, err = vfs.AcquirePoolSegment(name, tab, ttl)
	if err != nil {
		clog.Errorf("Tables service remove rows: %#v", err)
		return err
	}

	return s.storage.PutSegment(name, seg)
}

func (s *TableLFSServiceImpl) CreateTable(name string, table *types.Table, ttl int64) error {
	s.acquireTablesLock(name).Lock()
	defer s.acquireTablesLock(name).Unlock()

	if s.storage.HasSegment(name) {
		return ErrTableAlreadyExists
	}

	seg, err := vfs.AcquirePoolSegment(name, table, ttl)
	if err != nil {
		clog.Errorf("Tables service create: %#v", err)
		return err
	}

	defer utils.ReleaseToPool(table, seg)

	return s.storage.PutSegment(name, seg)
}

func (s *TableLFSServiceImpl) InsertRows(name string, rows map[string]any) (uint32, error) {
	s.acquireTablesLock(name).Lock()
	defer s.acquireTablesLock(name).Unlock()

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		return 0, err
	}

	tab, err := seg.ToTable()
	if err != nil {
		clog.Errorf("Tables service insert rows: %#v", err)
		return 0, err
	}

	defer utils.ReleaseToPool(tab, seg)

	// 插入数据到表里面返回一个数据 ID
	id := tab.AddRows(rows)

	ttl, ok := seg.ExpiresIn()
	if !ok {
		return 0, ErrTableExpired
	}

	seg, err = vfs.AcquirePoolSegment(name, tab, ttl)
	if err != nil {
		clog.Errorf("Tables service insert rows: %#v", err)
		return 0, err
	}

	err = s.storage.PutSegment(name, seg)
	if err != nil {
		clog.Errorf("Tables service insert rows: %#v", err)
		return 0, err
	}

	return id, nil
}

func (s *TableLFSServiceImpl) PatchRows(name string, condttions, data map[string]any) error {
	s.acquireTablesLock(name).Lock()
	defer s.acquireTablesLock(name).Unlock()

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		return err
	}

	tab, err := seg.ToTable()
	if err != nil {
		clog.Errorf("Tables service patch rows: %#v", err)
		return err
	}

	defer utils.ReleaseToPool(tab, seg)

	// 根据条件来更新，可以是基于默认的 t_id 和类似于 SQL 条件的
	err = tab.UpdateRows(condttions, data)
	if err != nil {
		return err
	}

	ttl, ok := seg.ExpiresIn()
	if !ok {
		return ErrTableExpired
	}

	seg, err = vfs.AcquirePoolSegment(name, tab, ttl)
	if err != nil {
		clog.Errorf("Tables service patch rows: %#v", err)
		return err
	}

	return s.storage.PutSegment(name, seg)
}

func (s *TableLFSServiceImpl) QueryRows(name string, wheres map[string]any) ([]map[string]any, error) {
	s.acquireTablesLock(name).RLock()
	defer s.acquireTablesLock(name).RUnlock()

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		clog.Errorf("Tables service query rows: %#v", err)
		return nil, err
	}

	tab, err := seg.ToTable()
	if err != nil {
		clog.Errorf("Tables service query rows: %#v", err)
		return nil, err
	}

	defer utils.ReleaseToPool(tab, seg)

	// 类似于 SQL 的 AND 多条件查询一样
	result := tab.SelectRowsAll(wheres)

	return result, nil
}

func NewTableLFSServiceImpl(storage *vfs.LogStructuredFS) TablesService {
	return &TableLFSServiceImpl{
		storage: storage,
	}
}

func (s *TableLFSServiceImpl) acquireTablesLock(key string) *sync.RWMutex {
	actual, _ := s.tlock.LoadOrStore(key, new(sync.RWMutex))
	return actual.(*sync.RWMutex)
}

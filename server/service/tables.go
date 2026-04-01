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

package service

import (
	"errors"
	"fmt"
	"sync"

	"github.com/auula/urnadb/clog"
	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/utils"
	"github.com/auula/urnadb/vfs"
)

type OperationType int8

const (
	_INSERT OperationType = iota
	_UPDATE
	_REMOVE
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
	// 事务接口，暂时不支持
	Transaction(mts []*TableMutation, serialization bool) error
}

type TablesServiceImpl struct {
	tlock   sync.Map
	storage *vfs.LogStructuredFS
}

func (*TablesServiceImpl) AllTables() []*types.Table {
	return nil
}

func (t *TablesServiceImpl) GetTable(name string) (*types.Table, error) {
	t.acquireTablesLock(name).RLock()
	defer t.acquireTablesLock(name).RUnlock()

	_, seg, err := t.storage.FetchSegment(name)
	if err != nil {
		clog.Errorf("[TablesService.GetTable] %v", err)
		return nil, ErrTableNotFound
	}

	defer seg.ReleaseToPool()

	return seg.ToTable()
}

func (t *TablesServiceImpl) DeleteTable(name string) error {
	t.acquireTablesLock(name).Lock()

	err := t.storage.DeleteSegment(name)
	if err != nil {
		t.acquireTablesLock(name).Unlock()
		clog.Errorf("[TablesService.DeleteTable] %v", err)
		return err
	}

	t.acquireTablesLock(name).Unlock()
	t.tlock.Delete(name)

	return nil
}

func (s *TablesServiceImpl) RemoveRows(name string, condtitons map[string]any) error {
	s.acquireTablesLock(name).Lock()
	defer s.acquireTablesLock(name).Unlock()

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		return err
	}

	tab, err := seg.ToTable()
	if err != nil {
		clog.Errorf("[TablesService.RemoveRows] %v", err)
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
		clog.Errorf("[TablesService.RemoveRows] %v", err)
		return err
	}

	return s.storage.PutSegment(name, seg)
}

func (s *TablesServiceImpl) CreateTable(name string, table *types.Table, ttl int64) error {
	if s.storage.IsActive(name) {
		return ErrTableAlreadyExists
	}

	s.acquireTablesLock(name).Lock()
	defer s.acquireTablesLock(name).Unlock()

	seg, err := vfs.AcquirePoolSegment(name, table, ttl)
	if err != nil {
		clog.Errorf("[TablesService.CreateTable] %v", err)
		return err
	}

	defer utils.ReleaseToPool(table, seg)

	return s.storage.PutSegment(name, seg)
}

func (s *TablesServiceImpl) InsertRows(name string, rows map[string]any) (uint32, error) {
	s.acquireTablesLock(name).Lock()
	defer s.acquireTablesLock(name).Unlock()

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		return 0, err
	}

	tab, err := seg.ToTable()
	if err != nil {
		clog.Errorf("[TablesService.InsertRows] %v", err)
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
		clog.Errorf("[TablesService.InsertRows] %v", err)
		return 0, err
	}

	err = s.storage.PutSegment(name, seg)
	if err != nil {
		clog.Errorf("[TablesService.InsertRows] %v", err)
		return 0, err
	}

	return id, nil
}

func (s *TablesServiceImpl) PatchRows(name string, conditions, data map[string]any) error {
	s.acquireTablesLock(name).Lock()
	defer s.acquireTablesLock(name).Unlock()

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		return err
	}

	tab, err := seg.ToTable()
	if err != nil {
		clog.Errorf("[TablesService.PatchRows] %v", err)
		return err
	}

	defer utils.ReleaseToPool(tab, seg)

	// 根据条件来更新，可以是基于默认的 t_id 和类似于 SQL 条件的
	err = tab.UpdateRows(conditions, data)
	if err != nil {
		clog.Errorf("[TablesService.PatchRows] %v", err)
		return err
	}

	ttl, ok := seg.ExpiresIn()
	if !ok {
		return ErrTableExpired
	}

	seg, err = vfs.AcquirePoolSegment(name, tab, ttl)
	if err != nil {
		clog.Errorf("[TablesService.PatchRows] %v", err)
		return err
	}

	return s.storage.PutSegment(name, seg)
}

func (s *TablesServiceImpl) QueryRows(name string, wheres map[string]any) ([]map[string]any, error) {
	s.acquireTablesLock(name).RLock()
	defer s.acquireTablesLock(name).RUnlock()

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		clog.Errorf("[TablesService.QueryRows] %v", err)
		return nil, err
	}

	tab, err := seg.ToTable()
	if err != nil {
		clog.Errorf("[TablesService.QueryRows] %v", err)
		return nil, err
	}

	defer utils.ReleaseToPool(tab, seg)

	// 类似于 SQL 的 AND 多条件查询一样
	return tab.SelectRowsAll(wheres), nil
}

type TableMutation struct {
	Name       string         // 事务涉及的表名列表
	Operation  OperationType  // 操作类型，类似于 SQL 的 INSERT、UPDATE、DELETE
	Conditions map[string]any // 操作条件针对 UPDATE 和 DELETE 操作
	Data       map[string]any // 操作数据针对 INSERT 和 UPDATE 操作
}

func (ts *TablesServiceImpl) Transaction(mutations []*TableMutation, serialization bool) error {
	// 去重 key 不需要拿到重复的快照
	keySet := make(map[string]struct{})
	for _, mutation := range mutations {
		keySet[mutation.Name] = struct{}{}
	}

	var keys []string
	for key := range keySet {
		keys = append(keys, key)
	}

	// 2PL 类似于关系数据中事物中的 serialization 隔离级别
	if serialization {
		for _, name := range keys {
			// 排序保证锁顺序一致
			ts.acquireTablesLock(name).Lock()
			defer ts.acquireTablesLock(name).Unlock()
		}
	}

	txn, err := ts.storage.NewTransaction()
	if err != nil {
		clog.Errorf("[TablesService.Transaction] %v", err)
		return err
	}

	txn.AtomicBatch(func(txns *vfs.TxnState) error {
		snapshots, err := txns.Begin(keys)
		if err != nil {
			clog.Errorf("[TablesService.Transaction] %v", err)
			return err
		}

		// 使用 working 保存中间状态
		working, tab := make(map[string]*types.Table), new(types.Table)
		for _, mutation := range mutations {
			// 优先使用 working 中的中间结果
			if pending, ok := working[mutation.Name]; ok {
				tab = pending
			} else {
				snap := snapshots[mutation.Name]
				tab, err = snap.ToTable()
				if err != nil {
					return err
				}
			}

			switch mutation.Operation {
			case _INSERT:
				if tab.AddRows(mutation.Data) <= 0 {
					return fmt.Errorf("failed to insert table %s rows", mutation.Name)
				}
			case _UPDATE:
				err := tab.UpdateRows(mutation.Conditions, mutation.Data)
				if err != nil {
					return fmt.Errorf("failed to update table %s rows: %w", mutation.Name, err)
				}
			case _REMOVE:
				tab.RemoveRows(mutation.Conditions)
			default:
				return fmt.Errorf("unsupported operation type: %s", mutation.Operation.String())
			}

			// 保存中间结果
			working[mutation.Name] = tab
		}

		// 最后构建所有 snapshot
		results := make(map[string]*vfs.Snapshot, len(working))
		for name, tab := range working {
			snap := snapshots[name]
			snapshot, err := buildSnapshot(snap, tab)
			if err != nil {
				return err
			}
			results[name] = snapshot
		}

		return txns.Save(results)
	})

	err = txn.Commit()
	if err != nil {
		inner := txn.Rollback()
		if inner != nil && !errors.Is(inner, vfs.ErrEmptyBeginSnapshot) {
			clog.Errorf("[TablesService.Transaction] %v", inner)
			return errors.Join(err, inner)
		}
		// 能到这里来说明是版本冲突了，直接返回错误就好了
		clog.Errorf("[TablesService.Transaction] %v", err)
		return err
	}

	return nil
}

func NewTablesServiceImpl(storage *vfs.LogStructuredFS) TablesService {
	return &TablesServiceImpl{
		storage: storage,
	}
}

func (s *TablesServiceImpl) acquireTablesLock(key string) *sync.RWMutex {
	actual, _ := s.tlock.LoadOrStore(key, new(sync.RWMutex))
	return actual.(*sync.RWMutex)
}

func buildSnapshot(snap *vfs.Snapshot, tab *types.Table) (*vfs.Snapshot, error) {
	ttl, ok := snap.ExpiresIn()
	if !ok {
		return nil, ErrTableExpired
	}

	seg, err := vfs.AcquirePoolSegment(snap.KeyString(), tab, ttl)
	if err != nil {
		return nil, err
	}

	return vfs.NewSnapshot(seg, snap.Version()), nil
}

func (opt OperationType) String() string {
	switch opt {
	case _INSERT:
		return "INSERT"
	case _UPDATE:
		return "UPDATE"
	case _REMOVE:
		return "REMOVE"
	default:
		return "UNKNOWN"
	}
}

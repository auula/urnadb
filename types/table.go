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

package types

import (
	"encoding/json"
	"errors"
	"reflect"
	"sync"

	"github.com/auula/urnadb/utils"
	"github.com/vmihailenco/msgpack/v5"
)

type Table struct {
	Table  map[uint32]map[string]any `json:"table" msgpack:"table" binding:"required"`
	NextID uint32                    `json:"t_id,omitempty"`
}

var tablePools = sync.Pool{
	New: func() any {
		return NewTable()
	},
}

func init() {
	// 预先填充池中的对象，把对象放入池中
	for i := 0; i < 10; i++ {
		tablePools.Put(NewTable())
	}
}

// 从对象池获取一个 Table
func AcquireTable() *Table {
	return tablePools.Get().(*Table)
}

// 释放 Table 归还到对象池
func (tab *Table) ReleaseToPool() {
	// 清理数据，避免脏数据影响复用
	tab.Clear()
	tablePools.Put(tab)
}

// 新建一个 Table
func NewTable() *Table {
	return &Table{
		NextID: 0,
		Table:  make(map[uint32]map[string]any),
	}
}

// Clear 清空 Table 和 TTL
func (tab *Table) Clear() {
	tab.NextID = 0
	tab.Table = make(map[uint32]map[string]any)
}

// 向 Table 中添加一个项
func (tab *Table) AddRows(rows map[string]any) uint32 {
	tab.NextID += 1
	tab.Table[tab.NextID] = rows
	return tab.NextID
}

// 从 Table 中删除一个项
func (tab *Table) RemoveRows(id uint32) {
	delete(tab.Table, id)
}

// 从 Table 中获取一个项
func (tab *Table) GetRows(key uint32) any {
	return tab.Table[key]
}

func (tab *Table) SelectRowsAll(wheres map[string]any) []map[string]any {
	var results []map[string]any

	for _, row := range tab.Table {
		match := true
		for key, value := range wheres {
			v, ok := row[key]
			if !ok {
				match = false
				break
			}
			if !reflect.DeepEqual(v, value) {
				match = false
				break
			}
		}

		if match {
			results = append(results, row)
		}
	}

	return results
}

func (tab *Table) UpdateRows(wheres, data map[string]any) error {
	// 优先处理按 t_id 更新
	if idVal, ok := wheres["t_id"]; ok {
		id, ok := idVal.(uint32)
		if !ok {
			return errors.New("t_id must be unsigned 32-bit integer.")
		}
		if row, exists := tab.Table[id]; exists {
			for k, v := range data {
				row[k] = v
			}
			tab.Table[id] = row
		} else {
			return errors.New("t_id is invalid.")
		}
	} else {
		// 原来的遍历逻辑
		for rowID, row := range tab.Table {
			match := true
			for key, value := range wheres {
				if rowVal, ok := row[key]; !ok || rowVal != value {
					match = false
					break
				}
			}
			if match {
				for k, v := range data {
					row[k] = v
				}
				tab.Table[rowID] = row
			}
		}
	}

	return nil
}

// 获取 Table 中的元素个数
func (tab *Table) Size() int {
	return len(tab.Table)
}

func (tab *Table) ToBytes() ([]byte, error) {
	return msgpack.Marshal(&tab.Table)
}

func (tab *Table) ToJSON() ([]byte, error) {
	return json.Marshal(&tab.Table)
}

func (tab *Table) DeepMerge(id uint32, news map[string]any) {
	utils.DeepMergeMaps(tab.Table[id], news)
}

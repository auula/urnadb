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
	"reflect"
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

type Table struct {
	Rows []map[string]any `json:"table" msgpack:"table"`
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
		Rows: make([]map[string]any, 0),
	}
}

// Clear 清空 Table 和 TTL
func (tab *Table) Clear() {
	tab.Rows = make([]map[string]any, 0)
}

// 向 Table 中添加一个项
func (tab *Table) AddRows(rows map[string]any) uint32 {
	tab.Rows = append(tab.Rows, rows)
	return uint32(len(tab.Rows))
}

// 从 Table 中删除一个项
func (tab *Table) RemoveRows(wheres map[string]any) {

	for i := len(tab.Rows) - 1; i >= 0; i-- {

		row := tab.Rows[i]

		match := true
		for key, value := range wheres {
			if v, ok := row[key]; !ok || !reflect.DeepEqual(v, value) {
				match = false
				break
			}
		}

		if match {
			tab.Rows = append(tab.Rows[:i], tab.Rows[i+1:]...)
		}
	}
}

// 从 Table 中获取一个项
func (tab *Table) GetRows(index uint32) any {
	return tab.Rows[index]
}

func (tab *Table) SelectRowsAll(wheres map[string]any) []map[string]any {
	var results []map[string]any

	for _, row := range tab.Rows {
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
	// 原来的遍历逻辑
	for index, row := range tab.Rows {
		match := true
		for key, value := range wheres {
			if val, ok := row[key]; !ok || !reflect.DeepEqual(val, value) {
				match = false
				break
			}
		}
		if match {
			for k, v := range data {
				row[k] = v
			}
			tab.Rows[index] = row
		}
	}

	return nil
}

// 获取 Table 中的元素个数
func (tab *Table) Size() int {
	return len(tab.Rows)
}

func (tab *Table) ToBytes() ([]byte, error) {
	return msgpack.Marshal(&tab)
}

func (tab *Table) ToJSON() ([]byte, error) {
	return json.Marshal(&tab.Rows)
}

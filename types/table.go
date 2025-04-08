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
	"sync"

	"github.com/vmihailenco/msgpack/v5"
)

type Table struct {
	Table map[string]any `json:"table" msgpack:"table" binding:"required"`
	TTL   uint64         `json:"ttl,omitempty"`
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
		Table: make(map[string]any),
	}
}

// Clear 清空 Table 和 TTL
func (tab *Table) Clear() {
	tab.TTL = 0
	tab.Table = make(map[string]any)
}

// 向 Table 中添加一个项
func (tab *Table) AddItem(key string, value any) {
	tab.Table[key] = value
}

// 从 Table 中删除一个项
func (tab *Table) RemoveItem(key string) {
	delete(tab.Table, key)
}

// 检查 Table 中是否包含指定的键
func (tab *Table) ContainsKey(key string) bool {
	_, exists := tab.Table[key]
	return exists
}

// 从 Table 中获取一个项
func (tab *Table) GetItem(key string) any {
	if tab.ContainsKey(key) {
		return tab.Table[key]
	}
	return nil
}

// 从 Tables 查找出键为目标 key 的值，包括所有值中值
func (tab *Table) SearchItem(key string) any {
	var results []any
	if items, exists := tab.Table[key]; exists {
		results = append(results, items)
	}

	for _, item := range tab.Table {
		if innerMap, ok := item.(map[string]any); ok {
			results = append(results, searchInMap(innerMap, key)...)
		}
	}

	return results
}

func searchInMap(m map[string]any, key string) []any {
	var results []any
	if item, exists := m[key]; exists {
		results = append(results, item)
	}

	// 遍历 map，查找是否有嵌套的 map 类型
	for _, value := range m {
		if nestedMap, ok := value.(map[string]any); ok {
			// 递归查找嵌套的 map
			results = append(results, searchInMap(nestedMap, key)...)
		}
	}

	return results
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

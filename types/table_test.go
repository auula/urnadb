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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNewTables(t *testing.T) {
	tables := NewTable()
	assert.NotNil(t, tables)
	assert.Empty(t, tables.Table) // 确保新建的表是空的
}

func TestTables_AddItem(t *testing.T) {
	tables := NewTable()
	tables.AddItem("key1", "value1")
	tables.AddItem("key2", 42)

	assert.Equal(t, 2, tables.Size()) // 确保添加成功
	assert.Equal(t, "value1", tables.GetItem("key1"))
	assert.Equal(t, 42, tables.GetItem("key2"))
}

func TestTables_RemoveItem(t *testing.T) {
	tables := NewTable()
	tables.AddItem("key1", "value1")
	tables.AddItem("key2", "value2")

	tables.RemoveItem("key1")
	assert.False(t, tables.ContainsKey("key1")) // 确保 key1 被删除
	assert.True(t, tables.ContainsKey("key2"))  // 确保 key2 仍然存在
	assert.Equal(t, 1, tables.Size())           // 确保大小正确
}

func TestTables_ContainsKey(t *testing.T) {
	tables := NewTable()
	tables.AddItem("testKey", "testValue")

	assert.True(t, tables.ContainsKey("testKey"))
	assert.False(t, tables.ContainsKey("nonExistentKey"))
}

func TestTables_GetItem(t *testing.T) {
	tables := NewTable()
	tables.AddItem("key1", "value1")

	assert.Equal(t, "value1", tables.GetItem("key1"))
	assert.Nil(t, tables.GetItem("nonExistentKey")) // 确保不存在的 key 返回 nil
}

func TestTables_SearchItem(t *testing.T) {
	tables := NewTable()
	tables.AddItem("key1", "value1")
	tables.AddItem("key2", map[string]any{
		"key1": "nested value1",
		"key3": "nested value3",
	})
	tables.AddItem("key3", map[string]any{
		"key1": "deep nested value1",
	})

	results := tables.SearchItem("key1")
	expectedResults := []any{"value1", "nested value1", "deep nested value1"}

	assert.ElementsMatch(t, expectedResults, results) // 确保所有匹配的值都被找到
}

func TestTables_Size(t *testing.T) {
	tables := NewTable()
	assert.Equal(t, 0, tables.Size()) // 确保初始大小为 0

	tables.AddItem("one", 1)
	tables.AddItem("two", 2)
	assert.Equal(t, 2, tables.Size()) // 添加两个元素

	tables.RemoveItem("one")
	assert.Equal(t, 1, tables.Size()) // 删除一个元素
}

func TestTables_Clear(t *testing.T) {
	tables := NewTable()
	tables.AddItem("a", "apple")
	tables.AddItem("b", "banana")
	tables.TTL = 100

	tables.Clear()
	assert.Equal(t, 0, tables.Size())     // 确保清空后大小为 0
	assert.Equal(t, int64(0), tables.TTL) // 确保 TTL 也被重置
}

func TestTables_ToBytes(t *testing.T) {
	tables := NewTable()
	tables.AddItem("x", "value")
	tables.AddItem("y", int8(123))

	data, err := tables.ToBytes()
	assert.NoError(t, err)
	assert.NotEmpty(t, data) // 确保序列化后的数据不为空

	// 反序列化回 Tables 进行验证
	var decodedTables Table
	err = msgpack.Unmarshal(data, &decodedTables.Table)
	assert.NoError(t, err)
	assert.Equal(t, tables.Table, decodedTables.Table) // 确保反序列化后的数据与原始数据一致
}

func TestTablesDeepMerge(t *testing.T) {
	tests := []struct {
		name     string
		base     map[string]any
		news     map[string]any
		expected map[string]any
	}{
		{
			name: "simple overwrite",
			base: map[string]any{"a": 1, "b": 2},
			news: map[string]any{"b": 20, "c": 30},
			expected: map[string]any{
				"a": 1,
				"b": 20,
				"c": 30,
			},
		},
		{
			name: "nested merge",
			base: map[string]any{
				"a": map[string]any{"x": 1, "y": 2},
				"b": 10,
			},
			news: map[string]any{
				"a": map[string]any{"y": 20, "z": 30},
				"b": 20,
				"c": 30,
			},
			expected: map[string]any{
				"a": map[string]any{"x": 1, "y": 20, "z": 30},
				"b": 20,
				"c": 30,
			},
		},
		{
			name: "type conflict",
			base: map[string]any{
				"a": map[string]any{"x": 1},
			},
			news: map[string]any{
				"a": 100, // 类型冲突，覆盖
			},
			expected: map[string]any{
				"a": 100,
			},
		},
		{
			name: "nested empty map",
			base: map[string]any{
				"a": map[string]any{"x": 1},
			},
			news: map[string]any{
				"a": map[string]any{}, // 空 map，不影响 base
			},
			expected: map[string]any{
				"a": map[string]any{"x": 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := &Table{
				Table: tt.base,
			}
			table.DeepMerge(tt.news)

			if !reflect.DeepEqual(table.Table, tt.expected) {
				t.Errorf("DeepMerge() = %v, want %v", table.Table, tt.expected)
			}
		})
	}
}

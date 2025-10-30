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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewTables(t *testing.T) {
	tables := NewTable()
	assert.NotNil(t, tables)
	assert.Empty(t, tables.Table)
	assert.Equal(t, uint32(0), tables.NextID)
}

func TestTable_AddRows(t *testing.T) {
	table := NewTable()
	
	row1 := map[string]any{"name": "test1", "age": 25}
	id1 := table.AddRows(row1)
	assert.Equal(t, uint32(1), id1)
	assert.Equal(t, 1, table.Size())
	
	row2 := map[string]any{"name": "test2", "age": 30}
	id2 := table.AddRows(row2)
	assert.Equal(t, uint32(2), id2)
	assert.Equal(t, 2, table.Size())
}

func TestTable_GetRows(t *testing.T) {
	table := NewTable()
	row := map[string]any{"name": "test", "age": 25}
	id := table.AddRows(row)
	
	result := table.GetRows(id)
	assert.Equal(t, row, result)
	
	result = table.GetRows(999)
	assert.Nil(t, result)
}

func TestTable_RemoveRows(t *testing.T) {
	table := NewTable()
	table.AddRows(map[string]any{"name": "test1", "age": 25})
	table.AddRows(map[string]any{"name": "test2", "age": 30})
	
	table.RemoveRows(map[string]any{"name": "test1"})
	assert.Equal(t, 1, table.Size())
	
	// 测试不匹配的条件
	table.RemoveRows(map[string]any{"name": "nonexistent"})
	assert.Equal(t, 1, table.Size())
	
	// 测试部分匹配
	table.RemoveRows(map[string]any{"name": "test2", "age": 25})
	assert.Equal(t, 1, table.Size())
}

func TestTable_SelectRowsAll(t *testing.T) {
	table := NewTable()
	table.AddRows(map[string]any{"name": "test1", "age": 25})
	table.AddRows(map[string]any{"name": "test2", "age": 25})
	table.AddRows(map[string]any{"name": "test3", "age": 30})
	
	results := table.SelectRowsAll(map[string]any{"age": 25})
	assert.Equal(t, 2, len(results))
	
	// 测试不存在的字段
	results = table.SelectRowsAll(map[string]any{"nonexistent": "value"})
	assert.Equal(t, 0, len(results))
	
	// 测试复杂对象匹配
	table.AddRows(map[string]any{"data": map[string]any{"nested": "value"}})
	results = table.SelectRowsAll(map[string]any{"data": map[string]any{"nested": "value"}})
	assert.Equal(t, 1, len(results))
}

func TestTable_UpdateRows(t *testing.T) {
	table := NewTable()
	id := table.AddRows(map[string]any{"name": "test", "age": 25})
	
	// 测试通过 t_id 更新
	err := table.UpdateRows(map[string]any{"t_id": id}, map[string]any{"age": 30})
	assert.NoError(t, err)
	
	row := table.GetRows(id).(map[string]any)
	assert.Equal(t, 30, row["age"])
	
	// 测试无效的 t_id
	err = table.UpdateRows(map[string]any{"t_id": uint32(999)}, map[string]any{"age": 35})
	assert.Error(t, err)
	
	// 测试错误的 t_id 类型
	err = table.UpdateRows(map[string]any{"t_id": "invalid"}, map[string]any{"age": 35})
	assert.Error(t, err)
	
	// 测试通过其他条件更新
	table.AddRows(map[string]any{"name": "test2", "age": 20})
	err = table.UpdateRows(map[string]any{"name": "test2"}, map[string]any{"age": 21})
	assert.NoError(t, err)
}

func TestTable_Clear(t *testing.T) {
	table := NewTable()
	table.AddRows(map[string]any{"name": "test", "age": 25})
	
	table.Clear()
	assert.Equal(t, 0, table.Size())
	assert.Equal(t, uint32(0), table.NextID)
}

func TestTable_ToBytes(t *testing.T) {
	table := NewTable()
	table.AddRows(map[string]any{"name": "test", "age": 25})
	
	bytes, err := table.ToBytes()
	assert.NoError(t, err)
	assert.NotEmpty(t, bytes)
}

func TestTable_ToJSON(t *testing.T) {
	table := NewTable()
	table.AddRows(map[string]any{"name": "test", "age": 25})
	
	json, err := table.ToJSON()
	assert.NoError(t, err)
	assert.NotEmpty(t, json)
}

func TestAcquireTable(t *testing.T) {
	table := AcquireTable()
	assert.NotNil(t, table)
	assert.Equal(t, uint32(0), table.NextID)
}

func TestTable_ReleaseToPool(t *testing.T) {
	table := AcquireTable()
	table.AddRows(map[string]any{"name": "test", "age": 25})
	
	table.ReleaseToPool()
	
	newTable := AcquireTable()
	assert.Equal(t, 0, newTable.Size())
}

func TestTable_DeepMerge(t *testing.T) {
	table := NewTable()
	id := table.AddRows(map[string]any{
		"user": map[string]any{
			"name": "test",
			"age":  25,
		},
	})
	
	table.DeepMerge(id, map[string]any{
		"user": map[string]any{
			"email": "test@example.com",
		},
	})
	
	row := table.GetRows(id).(map[string]any)
	user := row["user"].(map[string]any)
	assert.Equal(t, "test", user["name"])
	assert.Equal(t, 25, user["age"])
	assert.Equal(t, "test@example.com", user["email"])
}

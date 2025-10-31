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

package vfs

import (
	"testing"
	"time"

	"github.com/auula/urnadb/types"
	"github.com/stretchr/testify/assert"
)

func TestNewTombstoneSegment(t *testing.T) {
	// Create a Tombstone segment
	segment := NewTombstoneSegment("mock-key")

	// Ensure the segment is of Tombstone type and has expected fields
	assert.Equal(t, unknown, segment.Type)                   // Tombstone should have Unknown type
	assert.Equal(t, int8(1), segment.Tombstone)              // Tombstone should be marked as 1
	assert.Equal(t, "mock-key", string(segment.Key))         // Ensure the key is set correctly
	assert.Equal(t, int32(len("mock-key")), segment.KeySize) // Ensure the key size is correct
}

func TestSegmentSize(t *testing.T) {
	// Create a Record type data for testing
	record := types.NewRecord()
	record.AddRecord("item1", "value1")
	record.AddRecord("item2", "value2")

	// Create a segment for the Record type
	segment, err := NewSegment("mock-key", record, 1000)
	assert.NoError(t, err)

	// Ensure the size is calculated correctly (size will vary based on data)
	assert.True(t, segment.Size() > 0)
}

func TestToRecord(t *testing.T) {
	// Create a Record type Segment
	recordData := types.NewRecord()
	recordData.AddRecord("item1", "value1")
	recordData.AddRecord("item2", "value2")

	segment, err := NewSegment("mock-key", recordData, 1000)
	assert.NoError(t, err)

	// Convert the segment to Record
	record, err := segment.ToRecord()
	assert.NoError(t, err)                            // Ensure no error
	assert.Equal(t, recordData.Size(), record.Size()) // Ensure the Record size matches
}

func TestTTL(t *testing.T) {
	// Create a Segment with TTL
	record := types.NewRecord()
	record.AddRecord("item1", "value1")

	segment, err := NewSegment("mock-key", record, 1) // TTL = 1 second
	assert.NoError(t, err)

	// Wait 1 second
	time.Sleep(time.Second)

	// Test TTL, it should return a value close to 0
	ttl, _ := segment.ExpiresIn()
	assert.True(t, ttl <= 0) // Ensure TTL is <= 0 after expiration
}

// TestToTable 测试 ToTable 方法
func TestToTable(t *testing.T) {
	// 创建 Table 数据
	tablesData := types.NewTable()
	tablesData.AddRows(map[string]any{"key1": "value1"})
	tablesData.AddRows(map[string]any{"key2": 42})

	segment, err := NewSegment("test-key-01", tablesData, 0)
	assert.NoError(t, err)

	// 测试 ToTable 方法
	result, err := segment.ToTable()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, tablesData.Size(), result.Size())
}

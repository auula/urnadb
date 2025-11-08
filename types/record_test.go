package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRecord(t *testing.T) {
	record := NewRecord()
	assert.NotNil(t, record)
	assert.NotNil(t, record.Record)
	assert.Equal(t, 0, len(record.Record))
}

func TestAcquireRecord(t *testing.T) {
	record := AcquireRecord()
	assert.NotNil(t, record)
	record.ReleaseToPool()
}

func TestRecord_AddRecord(t *testing.T) {
	record := NewRecord()
	record.AddRecord("key1", "value1")
	record.AddRecord("key2", 123)
	
	assert.Equal(t, "value1", record.Record["key1"])
	assert.Equal(t, 123, record.Record["key2"])
}

func TestRecord_Size(t *testing.T) {
	record := NewRecord()
	assert.Equal(t, 0, record.Size())
	
	record.AddRecord("key1", "value1")
	assert.Equal(t, 1, record.Size())
	
	record.AddRecord("key2", "value2")
	assert.Equal(t, 2, record.Size())
}

func TestRecord_Clear(t *testing.T) {
	record := NewRecord()
	record.AddRecord("key1", "value1")
	record.AddRecord("key2", "value2")
	
	record.Clear()
	assert.Equal(t, 0, record.Size())
}

func TestRecord_ToJSON(t *testing.T) {
	record := NewRecord()
	record.AddRecord("name", "John")
	record.AddRecord("age", 30)
	
	data, err := record.ToJSON()
	assert.NoError(t, err)
	
	var result map[string]any
	err = json.Unmarshal(data, &result)
	assert.NoError(t, err)
	assert.Equal(t, "John", result["name"])
	assert.Equal(t, float64(30), result["age"])
}

func TestRecord_ToBytes(t *testing.T) {
	record := NewRecord()
	record.AddRecord("name", "John")
	record.AddRecord("age", 30)
	
	data, err := record.ToBytes()
	assert.NoError(t, err)
	assert.NotEmpty(t, data)
}

func TestRecord_DeepMerge(t *testing.T) {
	record := NewRecord()
	record.AddRecord("user", map[string]interface{}{
		"name": "John",
		"age":  30,
	})
	
	newData := map[string]interface{}{
		"user": map[string]interface{}{
			"age":   31,
			"email": "john@example.com",
		},
		"status": "active",
	}
	
	record.DeepMerge(newData)
	
	user := record.Record["user"].(map[string]interface{})
	assert.Equal(t, "John", user["name"])
	assert.Equal(t, 31, user["age"])
	assert.Equal(t, "john@example.com", user["email"])
	assert.Equal(t, "active", record.Record["status"])
}

func TestRecord_SearchItem(t *testing.T) {
	record := NewRecord()
	
	record.AddRecord("name", "John")
	record.AddRecord("age", 30)
	
	result := record.SearchItem("name")
	results := result.([]any)
	assert.Len(t, results, 1)
	assert.Equal(t, "John", results[0])
	
	record.AddRecord("user", map[string]any{
		"name": "Alice",
		"profile": map[string]any{
			"name": "Alice Profile",
			"age":  25,
		},
	})
	
	result = record.SearchItem("name")
	results = result.([]any)
	assert.Len(t, results, 3)
	
	result = record.SearchItem("nonexistent")
	results = result.([]any)
	assert.Empty(t, results)
	
	emptyRecord := NewRecord()
	result = emptyRecord.SearchItem("any")
	results = result.([]any)
	assert.Empty(t, results)
}

func TestRecord_ReleaseToPool(t *testing.T) {
	record := AcquireRecord()
	record.AddRecord("test", "value")
	
	record.ReleaseToPool()
	assert.Equal(t, 0, record.Size())
}

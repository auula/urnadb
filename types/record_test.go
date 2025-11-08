package types

import (
	"encoding/json"
	"testing"
)

func TestNewRecord(t *testing.T) {
	record := NewRecord()
	if record == nil {
		t.Error("NewRecord should not return nil")
	}
	if record.Record == nil {
		t.Error("Record.Record should be initialized")
	}
	if len(record.Record) != 0 {
		t.Error("New record should be empty")
	}
}

func TestAcquireRecord(t *testing.T) {
	record := AcquireRecord()
	if record == nil {
		t.Error("AcquireRecord should not return nil")
	}
	record.ReleaseToPool()
}

func TestRecord_AddRecord(t *testing.T) {
	record := NewRecord()
	record.AddRecord("key1", "value1")
	record.AddRecord("key2", 123)
	
	if record.Record["key1"] != "value1" {
		t.Errorf("Expected value1, got %v", record.Record["key1"])
	}
	if record.Record["key2"] != 123 {
		t.Errorf("Expected 123, got %v", record.Record["key2"])
	}
}

func TestRecord_Size(t *testing.T) {
	record := NewRecord()
	if record.Size() != 0 {
		t.Errorf("Expected size 0, got %d", record.Size())
	}
	
	record.AddRecord("key1", "value1")
	if record.Size() != 1 {
		t.Errorf("Expected size 1, got %d", record.Size())
	}
	
	record.AddRecord("key2", "value2")
	if record.Size() != 2 {
		t.Errorf("Expected size 2, got %d", record.Size())
	}
}

func TestRecord_Clear(t *testing.T) {
	record := NewRecord()
	record.AddRecord("key1", "value1")
	record.AddRecord("key2", "value2")
	
	record.Clear()
	if record.Size() != 0 {
		t.Errorf("Expected size 0 after clear, got %d", record.Size())
	}
}

func TestRecord_ToJSON(t *testing.T) {
	record := NewRecord()
	record.AddRecord("name", "John")
	record.AddRecord("age", 30)
	
	data, err := record.ToJSON()
	if err != nil {
		t.Errorf("ToJSON failed: %v", err)
	}
	
	var result map[string]any
	err = json.Unmarshal(data, &result)
	if err != nil {
		t.Errorf("JSON unmarshal failed: %v", err)
	}
	
	if result["name"] != "John" || result["age"].(float64) != 30 {
		t.Errorf("JSON data mismatch: %v", result)
	}
}

func TestRecord_ToBytes(t *testing.T) {
	record := NewRecord()
	record.AddRecord("name", "John")
	record.AddRecord("age", 30)
	
	data, err := record.ToBytes()
	if err != nil {
		t.Errorf("ToBytes failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("ToBytes should return non-empty data")
	}
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
	if user["name"] != "John" {
		t.Errorf("Expected name John, got %v", user["name"])
	}
	if user["age"] != 31 {
		t.Errorf("Expected age 31, got %v", user["age"])
	}
	if user["email"] != "john@example.com" {
		t.Errorf("Expected email john@example.com, got %v", user["email"])
	}
	if record.Record["status"] != "active" {
		t.Errorf("Expected status active, got %v", record.Record["status"])
	}
}

func TestRecord_SearchItem(t *testing.T) {
	record := NewRecord()
	
	record.AddRecord("name", "John")
	record.AddRecord("age", 30)
	
	result := record.SearchItem("name")
	if len(result.([]any)) != 1 || result.([]any)[0] != "John" {
		t.Errorf("Expected [John], got %v", result)
	}
	
	record.AddRecord("user", map[string]any{
		"name": "Alice",
		"profile": map[string]any{
			"name": "Alice Profile",
			"age":  25,
		},
	})
	
	result = record.SearchItem("name")
	results := result.([]any)
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
	
	result = record.SearchItem("nonexistent")
	if len(result.([]any)) != 0 {
		t.Errorf("Expected empty result, got %v", result)
	}
	
	emptyRecord := NewRecord()
	result = emptyRecord.SearchItem("any")
	if len(result.([]any)) != 0 {
		t.Errorf("Expected empty result for empty record, got %v", result)
	}
}

func TestRecord_ReleaseToPool(t *testing.T) {
	record := AcquireRecord()
	record.AddRecord("test", "value")
	
	record.ReleaseToPool()
	
	if record.Size() != 0 {
		t.Error("Record should be cleared after release to pool")
	}
}

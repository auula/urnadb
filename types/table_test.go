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
	"testing"
)

func TestNewTable(t *testing.T) {
	tab := NewTable()

	if tab == nil {
		t.Fatal("NewTable returned nil")
	}

	if tab.Rows == nil {
		t.Fatal("Rows should not be nil")
	}

	if len(tab.Rows) != 0 {
		t.Fatalf("expected empty table, got %d rows", len(tab.Rows))
	}
}

func TestTableAddRows(t *testing.T) {
	tab := NewTable()

	id1 := tab.AddRows(map[string]any{
		"name": "Leon",
		"age":  26,
	})

	id2 := tab.AddRows(map[string]any{
		"name": "Alice",
		"age":  30,
	})

	if id1 != 0 {
		t.Fatalf("expected first row id 0, got %d", id1)
	}

	if id2 != 1 {
		t.Fatalf("expected second row id 1, got %d", id2)
	}

	if tab.Size() != 2 {
		t.Fatalf("expected size 2, got %d", tab.Size())
	}
}

func TestTableGetRows(t *testing.T) {
	tab := NewTable()

	row := map[string]any{
		"name": "Leon",
		"age":  26,
	}

	tab.AddRows(row)

	result := tab.GetRows(0)

	got, ok := result.(map[string]any)

	if !ok {
		t.Fatal("expected map[string]any")
	}

	if !reflect.DeepEqual(got, row) {
		t.Fatalf(
			"expected %+v, got %+v",
			row,
			got,
		)
	}
}

func TestTableSelectRowsAll(t *testing.T) {
	tab := NewTable()

	tab.AddRows(map[string]any{
		"name": "Leon",
		"age":  26,
	})

	tab.AddRows(map[string]any{
		"name": "Alice",
		"age":  30,
	})

	results := tab.SelectRowsAll(
		map[string]any{
			"name": "Leon",
		},
	)

	if len(results) != 1 {
		t.Fatalf(
			"expected 1 result, got %d",
			len(results),
		)
	}

	if results[0]["name"] != "Leon" {
		t.Fatalf(
			"unexpected result %+v",
			results[0],
		)
	}
}

func TestTableUpdateRows(t *testing.T) {
	tab := NewTable()

	tab.AddRows(map[string]any{
		"name": "Leon",
		"age":  26,
	})

	err := tab.UpdateRows(
		map[string]any{
			"name": "Leon",
		},
		map[string]any{
			"age": 27,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	row := tab.Rows[0]

	if row["age"] != 27 {
		t.Fatalf(
			"expected age 27, got %v",
			row["age"],
		)
	}
}

func TestTableRemoveRows(t *testing.T) {
	tab := NewTable()

	tab.AddRows(map[string]any{
		"name": "Leon",
	})

	tab.AddRows(map[string]any{
		"name": "Alice",
	})

	tab.RemoveRows(
		map[string]any{
			"name": "Leon",
		},
	)

	if tab.Size() != 1 {
		t.Fatalf(
			"expected size 1, got %d",
			tab.Size(),
		)
	}

	if tab.Rows[0]["name"] != "Alice" {
		t.Fatalf(
			"expected Alice remain",
		)
	}
}

func TestTableClear(t *testing.T) {
	tab := NewTable()

	tab.AddRows(map[string]any{
		"name": "Leon",
	})

	tab.Clear()

	if tab.Size() != 0 {
		t.Fatalf(
			"expected empty table",
		)
	}
}

func TestTableSize(t *testing.T) {
	tab := NewTable()

	if tab.Size() != 0 {
		t.Fatal("new table should size 0")
	}

	tab.AddRows(map[string]any{
		"a": 1,
	})

	if tab.Size() != 1 {
		t.Fatal("table size should be 1")
	}
}

func TestTableToBytes(t *testing.T) {

	tab := NewTable()

	tab.AddRows(map[string]any{
		"name": "Leon",
		"age":  26,
	})

	data, err := tab.ToBytes()

	if err != nil {
		t.Fatal(err)
	}

	if len(data) == 0 {
		t.Fatal("msgpack bytes should not be empty")
	}
}

func TestTableToJSON(t *testing.T) {

	tab := NewTable()

	tab.AddRows(map[string]any{
		"name": "Leon",
		"age":  26,
	})

	data, err := tab.ToJSON()

	if err != nil {
		t.Fatal(err)
	}

	var rows []map[string]any

	err = json.Unmarshal(
		data,
		&rows,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(rows) != 1 {
		t.Fatalf(
			"expected 1 row, got %d",
			len(rows),
		)
	}

	if rows[0]["name"] != "Leon" {
		t.Fatalf(
			"unexpected json result",
		)
	}
}

func TestTablePool(t *testing.T) {

	tab := AcquireTable()

	if tab == nil {
		t.Fatal("AcquireTable returned nil")
	}

	tab.AddRows(
		map[string]any{
			"name": "Leon",
		},
	)

	tab.ReleaseToPool()

	tab2 := AcquireTable()

	if tab2.Size() != 0 {
		t.Fatalf(
			"released table should be empty",
		)
	}

	tab2.ReleaseToPool()
}

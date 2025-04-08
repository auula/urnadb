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

func TestCollection_AddItem(t *testing.T) {
	cle := NewCollection()

	// Test adding an item
	item := "test item"
	cle.AddItem(item)

	// Assert the item is added to the cle
	assert.Equal(t, 1, cle.Size())
	assert.Contains(t, cle.Collection, item)
}

func TestCollection_Remove(t *testing.T) {
	cle := NewCollection()
	item := "test item"
	cle.AddItem(item)

	// Test removing an existing item
	err := cle.Remove(item)
	assert.NoError(t, err)
	assert.NotContains(t, cle.Collection, item)

	// Test removing a non-existing item
	err = cle.Remove("non-existing item")
	assert.Error(t, err)
}

func TestCollection_GetItem(t *testing.T) {
	cle := NewCollection()
	item := "test item"
	cle.AddItem(item)

	// Test getting an item by index
	gotItem, err := cle.GetItem(0)
	assert.NoError(t, err)
	assert.Equal(t, item, gotItem)

	// Test out-of-bounds index
	_, err = cle.GetItem(1)
	assert.Error(t, err)
}

func TestCollection_Range(t *testing.T) {
	cle := NewCollection()
	cle.AddItem("item 1")
	cle.AddItem("item 2")
	cle.AddItem("item 3")

	// Test range function
	rangeItems, err := cle.Rnage(0, 1)
	assert.NoError(t, err)
	assert.Equal(t, []any{"item 1", "item 2"}, rangeItems)

	// Test out-of-bounds range
	rangeItems, err = cle.Rnage(2, 5)
	assert.NoError(t, err)
	assert.Equal(t, []any{"item 3"}, rangeItems)
}

func TestCollection_LPush(t *testing.T) {
	cle := NewCollection()
	cle.AddItem("item 1")
	cle.LPush("new item")

	// Test LPush functionality
	assert.Equal(t, 2, cle.Size())
	assert.Equal(t, "new item", cle.Collection[0])
}

func TestCollection_RPush(t *testing.T) {
	cle := NewCollection()
	cle.AddItem("item 1")
	cle.RPush("new item")

	// Test RPush functionality
	assert.Equal(t, 2, cle.Size())
	assert.Equal(t, "new item", cle.Collection[1])
}

func TestCollection_Clear(t *testing.T) {
	cle := NewCollection()
	cle.AddItem("item 1")
	cle.Clear()

	// Test clear functionality
	assert.Equal(t, 0, cle.Size())
	assert.Equal(t, uint64(0), cle.TTL)
}

func TestCollection_ToBytes(t *testing.T) {
	cle := NewCollection()
	cle.AddItem("item 1")

	// Test ToBytes
	_, err := cle.ToBytes()
	assert.NoError(t, err)
}

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
	"sync/atomic"

	"github.com/vmihailenco/msgpack/v5"
)

// Number 结构体，表示带有数值的类型，支持原子操作
type Number struct {
	Value int64 `json:"number" msgpack:"number" binding:"required"`
	TTL   int64 `json:"ttl,omitempty"`
}

// 创建一个对象池
var numberPools = sync.Pool{
	New: func() any {
		return NewNumber(0)
	},
}

func init() {
	// 预先填充池中的对象，把对象放入池中
	for i := 0; i < 10; i++ {
		numberPools.Put(NewNumber(0))
	}
}

// 从对象池获取一个 Number
func AcquireNumber() *Number {
	return numberPools.Get().(*Number)
}

// 释放 Number 归还到对象池
func (num *Number) ReleaseToPool() {
	num.Clear()
	numberPools.Put(num)
}

func NewNumber(num int64) *Number {
	return &Number{Value: num}
}

// ToBSON 将 Number 序列化为 msgpack
func (num *Number) ToBytes() ([]byte, error) {
	return msgpack.Marshal(&num.Value)
}

func (num *Number) ToJSON() ([]byte, error) {
	return json.Marshal(&num.Value)
}

// Add 以原子方式增加值
func (num *Number) Add(delta int64) int64 {
	return atomic.AddInt64(&num.Value, delta)
}

// Sub 以原子方式减少值
func (num *Number) Sub(delta int64) int64 {
	return atomic.AddInt64(&num.Value, -delta)
}

// Increment 自增（+1）
func (num *Number) Increment() int64 {
	return num.Add(1)
}

// Decrement 自减（-1）
func (num *Number) Decrement() int64 {
	return num.Sub(1)
}

// Set 以原子方式设置值
func (num *Number) Set(newValue int64) {
	atomic.StoreInt64(&num.Value, newValue)
}

// Get 以原子方式获取值
func (num *Number) Get() int64 {
	return atomic.LoadInt64(&num.Value)
}

// CompareAndSwap (CAS 操作) 仅当当前值等于 old 时，才设置为 new
func (num *Number) CompareAndSwap(old, new int64) bool {
	return atomic.CompareAndSwapInt64(&num.Value, old, new)
}

func (num *Number) Clear() {
	num.TTL = 0
	num.Value = 0
}

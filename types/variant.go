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

var variantPools = sync.Pool{
	New: func() any {
		return new(Variant)
	},
}

func init() {
	// 预先填充池中的对象，把对象放入池中
	for i := 0; i < 10; i++ {
		variantPools.Put(new(Variant))
	}
}

// 从对象池获取一个 Variant
func AcquireVariant() *Variant {
	return variantPools.Get().(*Variant)
}

// 释放 Variant 归还到对象池
func (v *Variant) ReleaseToPool() {
	// 清理数据，避免脏数据影响复用
	v.Clear()
	variantPools.Put(v)
}

func (v *Variant) Clear() {
	// 对于特定类型，可以更细致地清理
	switch v.Value.(type) {
	case string:
		// 设置为零值而不是nil
		v.Value = nullString
	case int64:
		v.Value = int64(0)
	case float64:
		v.Value = 0.0
	case bool:
		v.Value = false
	default:
		v.Value = nil
	}
}

type Variant struct {
	Value any `json:"variant" msgpack:"variant"`
}

func NewVariant(v any) *Variant {
	return &Variant{
		Value: v,
	}
}

// 类型转换
func (v *Variant) String() string {
	if v.Value != nil {
		return v.Value.(string)
	}
	return nullString
}

func (v *Variant) IsNumber() bool {
	if v.Value == nil {
		return false
	}
	_, iok := v.Value.(int64)
	_, fok := v.Value.(float64)
	return iok || fok
}

func (v *Variant) AddInt64(delta int64) int64 {
	if v.Value != nil {
		v.Value = v.Value.(int64) + delta
		return v.Value.(int64)
	}
	return 0
}

func (v *Variant) AddFloat64(delta float64) float64 {
	if v.Value != nil {
		v.Value = v.Value.(float64) + delta
		return v.Value.(float64)
	}
	return 0
}

func (v *Variant) Bool() bool {
	if v != nil {
		return v.Value.(bool)
	}
	return false
}

func (v *Variant) ToBytes() ([]byte, error) {
	return msgpack.Marshal(&v.Value)
}

func (v *Variant) ToJSON() ([]byte, error) {
	return json.Marshal(&v.Value)
}

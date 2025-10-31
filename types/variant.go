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

	"github.com/vmihailenco/msgpack/v5"
)

type Number interface {
	int64 | float64
}

type Variant interface {
	string | bool | Number
}

type Value[T Variant] struct {
	Value T
}

func NewValue[T Variant](v T) *Value[T] {
	return &Value[T]{
		Value: v,
	}
}

// 类型转换
func (v *Value[T]) String() string {
	return string(any(v.Value).(string))
}

func (v *Value[T]) Int64() int64 {
	return any(v.Value).(int64)
}

func (v *Value[T]) Float64() float64 {
	return any(v.Value).(float64)
}

func (v *Value[T]) Bool() bool {
	return any(v.Value).(bool)
}

func (v *Value[T]) ToBytes() ([]byte, error) {
	return msgpack.Marshal(&v.Value)
}

func (v *Value[T]) ToJSON() ([]byte, error) {
	return json.Marshal(&v.Value)
}

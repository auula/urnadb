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

	"github.com/auula/urnadb/utils"
	"github.com/vmihailenco/msgpack/v5"
)

type Record struct {
	Record map[string]any `json:"record" msgpack:"record"`
}

var recordPools = sync.Pool{
	New: func() any {
		return NewRecord()
	},
}

func init() {
	// 预先填充池中的对象，把对象放入池中
	for i := 0; i < 10; i++ {
		recordPools.Put(NewRecord())
	}
}

// 从对象池获取一个 Record
func AcquireRecord() *Record {
	return recordPools.Get().(*Record)
}

// 释放 Record 归还到对象池
func (rc *Record) ReleaseToPool() {
	// 清理数据，避免脏数据影响复用
	rc.Clear()
	recordPools.Put(rc)
}

// 新建一个 Record
func NewRecord() *Record {
	return &Record{
		Record: make(map[string]any),
	}
}

// Clear 清空 Record 和 TTL
func (rc *Record) Clear() {
	rc.Record = make(map[string]any)
}

// 向 Record 中添加一个项
func (rc *Record) AddRecord(name string, record any) {
	rc.Record[name] = record
}

// 获取 Record 中的元素个数
func (rc *Record) Size() int {
	return len(rc.Record)
}

func (rc *Record) ToBytes() ([]byte, error) {
	return msgpack.Marshal(&rc.Record)
}

func (rc *Record) ToJSON() ([]byte, error) {
	return json.Marshal(&rc.Record)
}

// DeepMerge 合并新的数据到 Record 中
func (rc *Record) DeepMerge(news map[string]interface{}) {
	utils.DeepMergeMaps(rc.Record, news)
}

// 从 Tables 查找出键为目标 key 的值，包括所有值中值
func (rc *Record) SearchItem(key string) any {
	var results []any
	if items, exists := rc.Record[key]; exists {
		results = append(results, items)
	}

	for _, item := range rc.Record {
		if innerMap, ok := item.(map[string]any); ok {
			results = append(results, utils.SearchInMap(innerMap, key)...)
		}
	}

	return results
}

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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/auula/urnadb/types"
	"github.com/vmihailenco/msgpack/v5"
)

type Kind int8

const (
	Set Kind = iota
	ZSet
	Text
	Table
	Number
	Unknown
	Collection
)

var KindToString = map[Kind]string{
	Set:        "set",
	ZSet:       "zset",
	Text:       "text",
	Table:      "table",
	Number:     "number",
	Unknown:    "unknown",
	Collection: "collection",
}

// | DEL 1 | KIND 1 | EAT 8 | CAT 8 | KLEN 4 | VLEN 4 | KEY ? | VALUE ? | CRC32 4 |
type Segment struct {
	Tombstone int8
	Type      Kind
	ExpiredAt uint64
	CreatedAt uint64
	KeySize   uint32
	ValueSize uint32
	Key       []byte
	Value     []byte
}

// Available segment in the pool
var segmentPool = sync.Pool{
	New: func() any {
		return new(Segment)
	},
}

func init() {
	// 预先填充池中的对象
	for i := 0; i < 100; i++ {
		// 把对象放入池中
		segmentPool.Put(new(Segment))
	}
}

type Serializable interface {
	ToBytes() ([]byte, error)
}

func AcquirePoolSegment(key string, data Serializable, ttl uint64) (*Segment, error) {
	seg := segmentPool.Get().(*Segment)
	timestamp, expiredAt := uint64(time.Now().UnixNano()), uint64(0)
	if ttl > 0 {
		expiredAt = uint64(time.Now().Add(time.Second * time.Duration(ttl)).UnixNano())
	}

	bytes, err := data.ToBytes()
	if err != nil {
		seg.ReleaseToPool()
		return nil, err
	}

	encodedata, err := transformer.Encode(bytes)
	if err != nil {
		seg.ReleaseToPool()
		return nil, fmt.Errorf("transformer encode: %w", err)
	}

	// 只能这样初始化复用 segment 结构
	seg.Type = toKind(data)
	seg.Tombstone = 0
	seg.CreatedAt = timestamp
	seg.ExpiredAt = expiredAt
	seg.KeySize = uint32(len(key))
	seg.ValueSize = uint32(len(encodedata))
	seg.Key = []byte(key)
	seg.Value = encodedata

	return seg, nil
}

func (seg *Segment) ReleaseToPool() {
	seg.Clear()
	segmentPool.Put(seg)
}

func (s *Segment) Clear() {
	s.Key = nil
	s.Value = nil
	s.KeySize = 0
	s.CreatedAt = 0
	s.ExpiredAt = 0
	s.ValueSize = 0
	s.Tombstone = 0
}

// NewSegment 使用数据类型初始化并返回对应的 Segment
func NewSegment(key string, data Serializable, ttl uint64) (*Segment, error) {
	timestamp, expiredAt := uint64(time.Now().UnixNano()), uint64(0)
	if ttl > 0 {
		expiredAt = uint64(time.Now().Add(time.Second * time.Duration(ttl)).UnixNano())
	}

	bytes, err := data.ToBytes()
	if err != nil {
		return nil, err
	}

	// 这个是通过 transformer 编码之后的
	encodedata, err := transformer.Encode(bytes)
	if err != nil {
		return nil, fmt.Errorf("transformer encode: %w", err)
	}

	// 如果类型不匹配，则返回错误
	return &Segment{
		Type:      toKind(data),
		Tombstone: 0,
		CreatedAt: timestamp,
		ExpiredAt: expiredAt,
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(encodedata)),
		Key:       []byte(key),
		Value:     encodedata,
	}, nil

}

func NewTombstoneSegment(key string) *Segment {
	timestamp, expiredAt := uint64(time.Now().UnixNano()), uint64(0)
	return &Segment{
		Type:      Unknown,
		Tombstone: 1,
		CreatedAt: timestamp,
		ExpiredAt: expiredAt,
		KeySize:   uint32(len(key)),
		ValueSize: 0,
		Key:       []byte(key),
		Value:     []byte{},
	}
}

func (s *Segment) IsTombstone() bool {
	return s.Tombstone == 1
}

func (s *Segment) GetTypeString() string {
	return KindToString[s.Type]
}

func (s *Segment) GetKeyString() string {
	return string(s.Key)
}

func (s *Segment) Size() uint32 {
	// 计算一整块记录的大小，+4 CRC 校验码占用 4 个字节
	return SEGMENT_PADDING + s.KeySize + s.ValueSize + 4
}

func (s *Segment) ToSet() (*types.Set, error) {
	if s.Type != Set {
		return nil, fmt.Errorf("not support conversion to set type")
	}
	set := types.AcquireSet()
	err := msgpack.Unmarshal(s.Value, &set.Set)
	if err != nil {
		set.ReleaseToPool()
		return nil, err
	}
	return set, nil
}

func (s *Segment) ToZSet() (*types.ZSet, error) {
	if s.Type != ZSet {
		return nil, fmt.Errorf("not support conversion to zset type")
	}
	zset := types.AcquireZSet()
	err := msgpack.Unmarshal(s.Value, &zset.ZSet)
	if err != nil {
		zset.ReleaseToPool()
		return nil, err
	}
	return zset, nil
}

func (s *Segment) ToText() (*types.Text, error) {
	if s.Type != Text {
		return nil, fmt.Errorf("not support conversion to text type")
	}
	text := types.AcquireText()
	err := msgpack.Unmarshal(s.Value, &text.Content)
	if err != nil {
		text.ReleaseToPool()
		return nil, err
	}
	return text, nil
}

func (s *Segment) ToCollection() (*types.Collection, error) {
	if s.Type != Collection {
		return nil, fmt.Errorf("not support conversion to collection type")
	}
	collection := types.AcquireCollection()
	err := msgpack.Unmarshal(s.Value, &collection.Collection)
	if err != nil {
		collection.ReleaseToPool()
		return nil, err
	}
	return collection, nil
}

func (s *Segment) ToTable() (*types.Table, error) {
	if s.Type != Table {
		return nil, fmt.Errorf("not support conversion to table type")
	}
	table := types.AcquireTable()
	err := msgpack.Unmarshal(s.Value, &table.Table)
	if err != nil {
		table.ReleaseToPool()
		return nil, err
	}
	return table, nil
}

func (s *Segment) ToNumber() (*types.Number, error) {
	if s.Type != Number {
		return nil, fmt.Errorf("not support conversion to number type")
	}
	number := types.AcquireNumber()
	err := msgpack.Unmarshal(s.Value, &number.Value)
	if err != nil {
		number.ReleaseToPool()
		return nil, err
	}
	return number, nil
}

func (s *Segment) TTL() int64 {
	now := uint64(time.Now().UnixNano())
	if s.ExpiredAt > 0 && s.ExpiredAt > now {
		return int64(s.ExpiredAt-now) / int64(time.Second)
	}
	return -1
}

// 将类型映射为 Kind 的辅助函数
func toKind(data Serializable) Kind {
	switch data.(type) {
	case *types.Set:
		return Set
	case *types.ZSet:
		return ZSet
	case *types.Text:
		return Text
	case *types.Table:
		return Table
	case *types.Number:
		return Number
	case *types.Collection:
		return Collection
	}
	return Unknown
}

func (s *Segment) ToBytes() []byte {
	return s.Value
}

func (s *Segment) ToJSON() ([]byte, error) {
	switch s.Type {
	case Set:
		set, err := s.ToSet()
		if err != nil {
			return nil, err
		}
		return set.ToJSON()
	case ZSet:
		zset, err := s.ToZSet()
		if err != nil {
			return nil, err
		}
		return zset.ToJSON()
	case Text:
		text, err := s.ToText()
		if err != nil {
			return nil, err
		}
		return text.ToJSON()
	case Number:
		num, err := s.ToNumber()
		if err != nil {
			return nil, err
		}
		return num.ToJSON()
	case Table:
		tab, err := s.ToTable()
		if err != nil {
			return nil, err
		}
		return tab.ToJSON()
	case Collection:
		collection, err := s.ToCollection()
		if err != nil {
			return nil, err
		}
		return collection.ToJSON()
	}

	return nil, errors.New("unknown data type")
}

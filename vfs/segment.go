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

type kind int8

const (
	set kind = iota
	zset
	text
	table
	number
	unknown
	collection
)

var kindToString = map[kind]string{
	set:        "set",
	zset:       "zset",
	text:       "text",
	table:      "table",
	number:     "number",
	unknown:    "unknown",
	collection: "collection",
}

// | DEL 1 | KIND 1 | EAT 8 | CAT 8 | KLEN 4 | VLEN 4 | KEY ? | VALUE ? | CRC32 4 |
type Segment struct {
	Tombstone int8
	Type      kind
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

// NewSegmentWithExpiry 使用数据类型和元信息初始化并返回对应的 Segment，适用于基于已有过期时间的 segment 的更新操作
func NewSegmentWithExpiry[T Serializable](data T, timestamp, expiredAt uint64) (*Segment, error) {
	return nil, nil
}

// GetExpiryMeta 返回 Segment 的元信息，包括创建时间和过期时间，适用于基于已有过期时间的 segment 的更新操作
func (seg *Segment) GetExpiryMeta() (uint64, uint64) {
	return seg.CreatedAt, seg.ExpiredAt
}

// NewSegment 使用数据类型初始化并返回对应的 Segment
func NewSegment[T Serializable](key string, data T, ttl uint64) (*Segment, error) {
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
		Type:      unknown,
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
	return kindToString[s.Type]
}

func (s *Segment) GetKeyString() string {
	return string(s.Key)
}

func (s *Segment) Size() uint32 {
	// 计算一整块记录的大小，+4 CRC 校验码占用 4 个字节
	return _SEGMENT_PADDING + s.KeySize + s.ValueSize + 4
}

func (s *Segment) ToSet() (*types.Set, error) {
	if s.Type != set {
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
	if s.Type != zset {
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
	if s.Type != text {
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
	if s.Type != collection {
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
	if s.Type != table {
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
	if s.Type != number {
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

// ExpiresIn 返回剩下的存活时间，一般在基于原有的 segment 更新时使用，
// 如果返回 -1，表示这个 segment 永不过期，并且返回 ok = true 表示这个 segment 没有过期。
// 如果返回 0，表示这个 segment 已经过期，ok = false 表示这个 segment 已经过期。
// 剩下的情况是返回剩下的存活时间，并且 ok = true 表示这个 segment 没有过期。
func (s *Segment) ExpiresIn() (int64, bool) {
	now := uint64(time.Now().UnixNano())
	if s.ExpiredAt > 0 && s.ExpiredAt > now {
		aliveTTL := int64(s.ExpiredAt-now) / int64(time.Second)
		if aliveTTL > 0 {
			return aliveTTL, true
		} else {
			return 0, false
		}
	}
	return -1, true
}

// 将类型映射为 kind 的辅助函数
func toKind(data Serializable) kind {
	switch data.(type) {
	case *types.Set:
		return set
	case *types.ZSet:
		return zset
	case *types.Text:
		return text
	case *types.Table:
		return table
	case *types.Number:
		return number
	case *types.Collection:
		return collection
	}
	return unknown
}

// Payload 返回 Segment 的值和长度
// 注意：这里的长度是 Value 的实际字节长度，不包括 padding 和其他字段
func (s *Segment) Payload() ([]byte, uint32) {
	return s.Value, uint32(len(s.Value))
}

func (s *Segment) ToJSON() ([]byte, error) {
	switch s.Type {
	case set:
		set, err := s.ToSet()
		if err != nil {
			return nil, err
		}
		return set.ToJSON()
	case zset:
		zset, err := s.ToZSet()
		if err != nil {
			return nil, err
		}
		return zset.ToJSON()
	case text:
		text, err := s.ToText()
		if err != nil {
			return nil, err
		}
		return text.ToJSON()
	case number:
		num, err := s.ToNumber()
		if err != nil {
			return nil, err
		}
		return num.ToJSON()
	case table:
		tab, err := s.ToTable()
		if err != nil {
			return nil, err
		}
		return tab.ToJSON()
	case collection:
		collection, err := s.ToCollection()
		if err != nil {
			return nil, err
		}
		return collection.ToJSON()
	}

	return nil, errors.New("unknown data type")
}

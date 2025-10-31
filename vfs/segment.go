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
	table
	record
	unknown
	leaselock
)

const ImmortalTTL = -1

var kindToString = map[kind]string{
	set:       "set",
	zset:      "zset",
	table:     "table",
	record:    "record",
	unknown:   "unknown",
	leaselock: "leaselock",
}

// | DEL 1 | KIND 1 | EAT 8 | CAT 8 | KLEN 4 | VLEN 4 | KEY ? | VALUE ? | CRC32 4 |
type Segment struct {
	Tombstone int8
	Type      kind
	ExpiredAt int64
	CreatedAt int64
	KeySize   int32
	ValueSize int32
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

func AcquirePoolSegment(key string, data Serializable, ttl int64) (*Segment, error) {
	seg := segmentPool.Get().(*Segment)
	createdAt, expiredAt := int64(time.Now().UnixMicro()), int64(ImmortalTTL)
	if ttl > 0 {
		expiredAt = time.Now().Add(time.Second * time.Duration(ttl)).UnixMicro()
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
	seg.CreatedAt = createdAt
	seg.ExpiredAt = expiredAt
	seg.KeySize = int32(len(key))
	seg.ValueSize = int32(len(encodedata))
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
func NewSegmentWithExpiry[T Serializable](data T, createdAt, expiredAt int64) (*Segment, error) {
	return nil, nil
}

// GetExpiryMeta 返回 Segment 的元信息，包括创建时间和过期时间，适用于基于已有过期时间的 segment 的更新操作
func (seg *Segment) GetExpiryMeta() (int64, int64) {
	return seg.CreatedAt, seg.ExpiredAt
}

// NewSegment 使用数据类型初始化并返回对应的 Segment
func NewSegment[T Serializable](key string, data T, ttl int64) (*Segment, error) {
	createdAt, expiredAt := int64(time.Now().UnixMicro()), int64(ImmortalTTL)
	if ttl > 0 {
		expiredAt = time.Now().Add(time.Second * time.Duration(ttl)).UnixMicro()
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
		CreatedAt: createdAt,
		ExpiredAt: expiredAt,
		KeySize:   int32(len(key)),
		ValueSize: int32(len(encodedata)),
		Key:       []byte(key),
		Value:     encodedata,
	}, nil

}

func NewTombstoneSegment(key string) *Segment {
	createdAt, expiredAt := int64(time.Now().UnixMicro()), int64(0)
	return &Segment{
		Type:      unknown,
		Tombstone: 1,
		CreatedAt: createdAt,
		ExpiredAt: expiredAt,
		KeySize:   int32(len(key)),
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

func (s *Segment) Size() int32 {
	// 计算一整块记录的大小，+4 CRC 校验码占用 4 个字节
	return _SEGMENT_PADDING + s.KeySize + s.ValueSize + 4
}

func (s *Segment) ToRecord() (*types.Record, error) {
	if s.Type != record {
		return nil, fmt.Errorf("not support conversion to record type")
	}
	
	// 先通过 transformer 解码
	decodedData, err := transformer.Decode(s.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode segment value: %w", err)
	}
	
	record := types.AcquireRecord()
	err = msgpack.Unmarshal(decodedData, &record.Record)
	if err != nil {
		record.ReleaseToPool()
		return nil, err
	}
	return record, nil
}

func (s *Segment) ToTable() (*types.Table, error) {
	if s.Type != table {
		return nil, fmt.Errorf("not support conversion to table type")
	}
	
	// 先通过 transformer 解码
	decodedData, err := transformer.Decode(s.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode segment value: %w", err)
	}
	
	table := types.AcquireTable()
	err = msgpack.Unmarshal(decodedData, table)
	if err != nil {
		table.ReleaseToPool()
		return nil, err
	}
	return table, nil
}

func (s *Segment) ToLeaseLock() (*types.LeaseLock, error) {
	if s.Type != leaselock {
		return nil, fmt.Errorf("not support conversion to lease lock type")
	}
	
	// 先通过 transformer 解码
	decodedData, err := transformer.Decode(s.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to decode segment value: %w", err)
	}
	
	leaseLock := types.AcquireLeaseLock()
	err = msgpack.Unmarshal(decodedData, &leaseLock.Token)
	if err != nil {
		leaseLock.ReleaseToPool()
		return nil, err
	}
	return leaseLock, nil
}

// ExpiresIn 返回剩下的存活时间，一般在基于原有的 segment 更新时使用，
// 如果返回 -1，表示这个 segment 永不过期，并且返回 ok = true 表示这个 segment 没有过期。
// 如果返回 0，表示这个 segment 已经过期，ok = false 表示这个 segment 已经过期。
// 剩下的情况是返回剩下的存活时间，并且 ok = true 表示这个 segment 没有过期。
func (s *Segment) ExpiresIn() (int64, bool) {
	now := time.Now().UnixMicro()
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
	case *types.Table:
		return table
	case *types.Record:
		return record
	case *types.LeaseLock:
		return leaselock
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
	case record:
		num, err := s.ToRecord()
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
	case leaselock:
		leaseLock, err := s.ToLeaseLock()
		if err != nil {
			return nil, err
		}
		return leaseLock.ToJSON()
	}

	return nil, errors.New("unknown data type")
}

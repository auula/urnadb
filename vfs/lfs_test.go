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
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/auula/urnadb/conf"
	"github.com/auula/urnadb/types"
	"github.com/stretchr/testify/assert"
)

// TestSerializedIndex 测试 serializedIndex 函数
func TestSerializedIndex(t *testing.T) {
	// 创建一个测试的 inode
	in := &inode{
		RegionID:  1234,
		Position:  5678,
		ExpiredAt: 1617181723,
		CreatedAt: 1617181623,
		Length:    100,
	}

	buf := new(bytes.Buffer)

	// 调用 serializeIndex
	result, err := serializedIndex(buf, 1001, in)
	if err != nil {
		t.Fatalf("serialized index failed: %v", err)
	}

	// 检查返回的字节切片长度
	assert.Equal(t, len(result), 48)

	// 验证内容字段进行反序列化并检查
	inum, dnode, err := deserializedIndex(result)
	if err != nil {
		t.Errorf("failed to deserialized: %v", err)
	}

	// 验证字段是否一致
	if inum != 1001 {
		t.Errorf("expected inum %d, got %d", 1001, inum)
	}
	if dnode.RegionID != in.RegionID {
		t.Errorf("expected RegionID %d, got %d", in.RegionID, dnode.RegionID)
	}
	if dnode.Position != in.Position {
		t.Errorf("expected Offset %d, got %d", in.Position, dnode.RegionID)
	}
	if dnode.ExpiredAt != in.ExpiredAt {
		t.Errorf("expected ExpiredAt %d, got %d", in.ExpiredAt, dnode.ExpiredAt)
	}
	if dnode.CreatedAt != in.CreatedAt {
		t.Errorf("expected CreatedAt %d, got %d", in.CreatedAt, dnode.CreatedAt)
	}
	if dnode.Length != in.Length {
		t.Errorf("expected Length %d, got %d", in.Length, dnode.Length)
	}

}

// 测试 readSegment 函数
func TestReadSegment(t *testing.T) {
	// 构造测试数据
	seg := &Segment{
		Tombstone: 0,
		Type:      1,
		ExpiredAt: 123456789,
		CreatedAt: 987654321,
		KeySize:   3,
		ValueSize: 5,
		Key:       []byte("key"),
		Value:     []byte("value"),
	}

	// 将 Segment 数据转化为字节数组
	bytes, err := serializedSegment(seg)
	if err != nil {
		t.Fatalf("failed to serialized segment:%v", err)
	}

	// 创建临时文件
	tmpFile, err := os.CreateTemp("", "testfile")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// 写入测试数据
	_, err = tmpFile.Write(bytes)
	if err != nil {
		t.Fatalf("failed to write test data to temp file: %v", err)
	}

	// 使用 readSegment 读取并测试数据
	offset := int64(0)
	inum, segment, err := readSegment(tmpFile, offset, 26)
	if err != nil {
		t.Fatalf("expected no error, but got: %v", err)
	}

	// 校验 Segment 数据
	if segment.Tombstone != seg.Tombstone {
		t.Errorf("expected Tombstone to be %d, but got: %d", seg.Tombstone, segment.Tombstone)
	}
	if segment.Type != seg.Type {
		t.Errorf("expected Type to be %d, but got: %d", seg.Type, segment.Type)
	}
	if segment.ExpiredAt != seg.ExpiredAt {
		t.Errorf("expected ExpiredAt to be %d, but got: %d", seg.ExpiredAt, segment.ExpiredAt)
	}
	if segment.CreatedAt != seg.CreatedAt {
		t.Errorf("expected CreatedAt to be %d, but got: %d", seg.CreatedAt, segment.CreatedAt)
	}
	if segment.KeySize != seg.KeySize {
		t.Errorf("expected KeySize to be %d, but got: %d", seg.KeySize, segment.KeySize)
	}
	if segment.ValueSize != seg.ValueSize {
		t.Errorf("expected ValueSize to be %d, but got: %d", seg.ValueSize, segment.ValueSize)
	}
	if string(segment.Key) != string(seg.Key) {
		t.Errorf("expected Key to be %s, but got: %s", string(seg.Key), string(segment.Key))
	}
	if string(segment.Value) != string(seg.Value) {
		t.Errorf("expected Value to be %s, but got: %s", string(seg.Value), string(segment.Value))
	}

	// 校验返回的 inode number (inodeNum)
	if inum != inodeNum(string(seg.Key)) {
		t.Errorf("expected inodeNum to be '%s', but got: %d", seg.Key, inum)
	}
}

func TestVFSWrite(t *testing.T) {
	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		t.Fatal(err)
	}

	data := `
{
  "table": {
    "1": {
      "active": true,
      "age": 25,
      "name": "Alice",
      "score": 95.5,
      "tags": [
        "admin",
        "user"
      ]
    },
    "2": {
      "active": false,
      "age": 30,
      "config": {
        "font": 14,
        "theme": "dark"
      },
      "name": "Bob"
    },
    "3": {}
  },
  "t_id": 4
}
`

	var tables types.Table
	err = json.Unmarshal([]byte(data), &tables)
	if err != nil {
		t.Fatal(err)
	}

	seg, err := NewSegment("key-01", &tables, 0)
	if err != nil {
		t.Fatal(err)
	}

	err = fss.PutSegment("key-01", seg)
	if err != nil {
		t.Fatal(err)
	}

	_, seg, err = fss.FetchSegment("key-01")
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%v", seg)
}

func BenchmarkVFSWrite(b *testing.B) {
	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		b.Fatal(err)
	}

	data := `
{
  "table": {
    "1": {
      "active": true,
      "age": 25,
      "name": "Alice",
      "score": 95.5,
      "tags": [
        "admin",
        "user"
      ]
    },
    "2": {
      "active": false,
      "age": 30,
      "config": {
        "font": 14,
        "theme": "dark"
      },
      "name": "Bob"
    },
    "3": {}
  },
  "t_id": 4
}
`
	tables := types.AcquireTable()
	err = json.Unmarshal([]byte(data), &tables)
	if err != nil {
		b.Fatal(err)
	}

	// 重置计时器，忽略 setup 代码的影响
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)

		// 从复用池里创建
		seg, err := AcquirePoolSegment(key, tables, 0)
		if err != nil {
			seg.ReleaseToPool()
			b.Fatal(err)
		}

		err = fss.PutSegment(key, seg)
		if err != nil {
			seg.ReleaseToPool()
			b.Fatal(err)
		}

		// 使用完放回复用池里
		seg.ReleaseToPool()
	}
}

func BenchmarkVFSReads(b *testing.B) {
	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		_, _, err := fss.FetchSegment(key)
		if err != nil {
			b.Logf("%v", err)
		}
	}
}

func TestUpdateSegmentWithCAS_Concurrent(t *testing.T) {
	var wg sync.WaitGroup

	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		t.Fatal(err)
	}

	data := `
{
  "table": {
    "1": {
      "active": true,
      "age": 25,
      "name": "Alice",
      "score": 95.5,
      "tags": [
        "admin",
        "user"
      ]
    },
    "2": {
      "active": false,
      "age": 30,
      "config": {
        "font": 14,
        "theme": "dark"
      },
      "name": "Bob"
    },
    "3": {}
  },
  "t_id": 4
}
`
	var tables types.Table
	err = json.Unmarshal([]byte(data), &tables)
	if err != nil {
		t.Fatal(err)
	}

	key := "key-01"
	seg, err := NewSegment(key, &tables, -1)
	if err != nil {
		t.Fatal(err)
	}

	err = fss.PutSegment(key, seg)
	if err != nil {
		t.Fatal(err)
	}

	var failures int32
	var success int32

	concurrentUpdates := rand.Intn(100)
	// 记录开始时间
	startTime := time.Now()
	for i := 0; i < concurrentUpdates; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// 1. 获取当前版本号
			version, _, err := fss.FetchSegment(key)
			if err != nil {
				t.Errorf("goroutine %d: failed to fetch segment: %v", id, err)
				return
			}

			// 2. 创建新的 `Segment`
			newseg, err := NewSegment(key, &tables, -1)
			if err != nil {
				t.Errorf("goroutine %d: failed to create segment: %v", id, err)
				return
			}

			// 3. CAS 更新
			err = fss.UpdateSegmentWithCAS(key, version, newseg)
			if err != nil {
				atomic.AddInt32(&failures, 1)
				t.Logf("goroutine %d: CAS update failed (expected version: %d)", id, version)
			} else {
				atomic.AddInt32(&success, 1)
				t.Logf("goroutine %d: CAS update succeeded (version: %d)", id, version)
			}
		}(i)
	}

	wg.Wait()

	// 记录结束时间
	duration := time.Since(startTime)
	t.Logf("Total execution time: %v", duration)

	t.Logf("Total success: %d, Total failures: %d,Updates concurrent: %d", success, failures, concurrentUpdates)

	// 断言至少有一些失败的情况（正常情况下应该有很多失败）
	if failures == 0 && (failures+success) != int32(concurrentUpdates) {
		t.Error("Expected some CAS failures, but got none")
	}
}

func TestConcurrentPutAndFetchSegment(t *testing.T) {
	var wg sync.WaitGroup
	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	if err != nil {
		t.Fatal(err)
	}

	// 定义并发数量
	concurrentWrites := 100 // 写操作并发数
	concurrentReads := 100  // 读操作并发数

	wg.Add(concurrentWrites)
	// 存储测试数据
	for i := 0; i < concurrentWrites; i++ {
		go func(id int) {
			defer wg.Done()
			// 创建 segment
			k := fmt.Sprintf("key-%d", id)

			record := types.NewVariant(int16(id))

			segment, err := NewSegment(k, record, 0)
			if err != nil {
				t.Errorf("failed to create segment for key %s: %v", k, err)
				return
			}

			// 存储 segment
			err = fss.PutSegment(k, segment)
			if err != nil {
				t.Errorf("failed to put segment for key %s: %v", k, err)
			}
		}(i)
	}

	wg.Wait()

	for i := 0; i < concurrentReads; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			k := fmt.Sprintf("key-%d", id)

			// 获取 segment
			_, seg, err := fss.FetchSegment(k)
			if err != nil {
				t.Errorf("failed to fetch segment for key: %s \t %v", k, err)
				return
			}

			// 转换为 record 并验证
			record, err := seg.ToVariant()
			if err != nil {
				t.Errorf("failed to convert segment to record for key %s \t %v", k, err)
			}

			t.Logf("K:%s\tV:%v", k, record.Value.(int16))
		}(i)
	}

	// 等待所有 goroutine 完成
	wg.Wait()
}

func TestVFSOpertions(t *testing.T) {
	err := os.RemoveAll(conf.Settings.Path)
	assert.NoError(t, err)

	fss, err := OpenFS(&Options{
		FSPerm:    conf.FSPerm,
		Path:      conf.Settings.Path,
		Threshold: conf.Settings.Region.Threshold,
	})
	assert.NoError(t, err)

	data := `
{
  "table": {
    "1": {
      "active": true,
      "age": 25,
      "name": "Alice",
      "score": 95.5,
      "tags": [
        "admin",
        "user"
      ]
    },
    "2": {
      "active": false,
      "age": 30,
      "config": {
        "font": 14,
        "theme": "dark"
      },
      "name": "Bob"
    },
    "3": {}
  },
  "t_id": 4
}
`

	var tables types.Table
	err = json.Unmarshal([]byte(data), &tables)
	assert.NoError(t, err)

	seg, err := NewSegment("key-01", &tables, -1)
	assert.NoError(t, err)

	err = fss.PutSegment("key-01", seg)
	assert.NoError(t, err)

	assert.Equal(t, fss.IsActive("key-01"), true)

	assert.Equal(t, fss.RefreshInodeCount(), uint64(1))

	err = fss.DeleteSegment("key-01")
	assert.NoError(t, err)

	_, _, err = fss.FetchSegment("key-01")
	assert.Equal(t, err.Error(), "inode index for 9171687345308829835 not found")

	err = fss.ExportSnapshotIndex()
	assert.NoError(t, err)

	err = fss.SetEncryptor(AESCryptor, []byte("1234567890123456"))
	assert.NoError(t, err)

	fss.SetCompressor(SnappyCompressor)

	pipeline = NewPipeline()

	os.RemoveAll(conf.Settings.Path)
}

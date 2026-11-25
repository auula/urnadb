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

package service

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/auula/urnadb/conf"
	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/vfs"
	"github.com/stretchr/testify/assert"
)

func openTestFS(t *testing.T) *vfs.LogStructuredFS {
	t.Helper()

	storage, err := vfs.OpenFS(&vfs.Options{
		FSPerm:    conf.FSPerm,
		Path:      t.TempDir(),
		Threshold: 1,
	})
	assert.NoError(t, err)

	return storage
}

// 同一把锁同时只能被一个客户端获取
func TestAcquireLock_Concurrent(t *testing.T) {
	locks := NewLocksServiceImpl(openTestFS(t))

	for i := 0; i < 50; i++ {
		var acquired atomic.Int32
		var token atomic.Value
		var wg sync.WaitGroup

		start := make(chan struct{})
		for g := 0; g < 16; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				lease, err := locks.AcquireLock("lock", 60)
				if err == nil {
					acquired.Add(1)
					token.Store(lease.Token)
				} else {
					assert.ErrorIs(t, err, ErrAlreadyLocked)
				}
			}()
		}
		close(start)
		wg.Wait()

		assert.Equal(t, int32(1), acquired.Load())
		assert.NoError(t, locks.ReleaseLock("lock", token.Load().(string)))
	}
}

// 删除变量时其他请求可能正在等待同一把锁
func TestDeleteVariant_Concurrent(t *testing.T) {
	variants := NewVariantsServiceImpl(openTestFS(t))

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				switch g % 3 {
				case 0:
					_ = variants.SetVariant("counter", types.NewVariant(float64(1)), 60)
				case 1:
					_, _ = variants.Increment("counter", 1)
				default:
					_ = variants.DeleteVariant("counter")
				}
			}
		}(g)
	}
	wg.Wait()

	assert.Empty(t, variants.(*VariantsServiceImpl).vlock.locks)
}

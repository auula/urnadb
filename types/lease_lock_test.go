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
	"testing"

	"github.com/auula/urnadb/utils"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNewLeaseLock(t *testing.T) {
	ll := NewLeaseLock()

	assert.NotNil(t, ll)
	assert.NotEmpty(t, ll.Token)
	assert.Equal(t, 26, len(ll.Token))
	assert.NotEqual(t, ll.Token, NewLeaseLock().Token)
}

func TestAcquireLeaseLock(t *testing.T) {
	ll := AcquireLeaseLock()

	assert.NotNil(t, ll)
	assert.NotEmpty(t, ll.Token)
	assert.Equal(t, 26, len(ll.Token))

	// 获取另一个锁，验证 Token 唯一性
	ll2 := AcquireLeaseLock()
	assert.NotEqual(t, ll.Token, ll2.Token)

	ll.ReleaseToPool()
	ll2.ReleaseToPool()
}

func TestLeaseLockClear(t *testing.T) {
	ll := NewLeaseLock()
	originalToken := ll.Token

	assert.NotEmpty(t, originalToken)

	ll.Clear()
	assert.Equal(t, nullString, ll.Token)
}

func TestLeaseLockReleaseToPool(t *testing.T) {
	ll := AcquireLeaseLock()
	ll.Token = utils.NewULID()
	originalToken := ll.Token

	assert.NotEmpty(t, originalToken)

	ll.ReleaseToPool()
	assert.Equal(t, nullString, ll.Token)

	// 验证对象被放回池中
	ll2 := AcquireLeaseLock()
	ll2.Token = utils.NewULID()
	assert.NotNil(t, ll2)
	assert.NotEmpty(t, ll2.Token)
	assert.NotEqual(t, originalToken, ll2.Token) // 新的 Token 应该不同

	ll2.ReleaseToPool()
}

func TestLeaseLockToBytes(t *testing.T) {
	ll := NewLeaseLock()

	bytes, err := ll.ToBytes()
	assert.NoError(t, err)
	assert.NotNil(t, bytes)

	// 验证可以反序列化
	var token string
	err = msgpack.Unmarshal(bytes, &token)
	assert.NoError(t, err)
	assert.Equal(t, ll.Token, token)
}

func TestLeaseLockToJSON(t *testing.T) {
	ll := NewLeaseLock()

	jsonBytes, err := ll.ToJSON()
	assert.NoError(t, err)
	assert.NotNil(t, jsonBytes)

	// 验证可以反序列化
	var token string
	err = json.Unmarshal(jsonBytes, &token)
	assert.NoError(t, err)
	assert.Equal(t, ll.Token, token)
}

func TestLeaseLockConcurrency(t *testing.T) {
	const goroutines = 100
	tokenChan := make(chan string, goroutines)
	var wg sync.WaitGroup

	// 并发创建锁（不使用对象池，避免竞争）
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ll := NewLeaseLock() // 使用 NewLeaseLock 而不是 AcquireLeaseLock
			tokenChan <- ll.Token
		}()
	}

	wg.Wait()
	close(tokenChan)

	// 验证所有 Token 都是唯一的
	tokenSet := make(map[string]bool)
	for token := range tokenChan {
		assert.NotEmpty(t, token)
		assert.False(t, tokenSet[token], "Token should be unique: %s", token)
		tokenSet[token] = true
	}
	assert.Equal(t, goroutines, len(tokenSet))
}

func TestLeaseLockPoolFunctionality(t *testing.T) {
	// 测试对象池的基本功能（顺序执行，避免竞争）
	locks := make([]*LeaseLock, 10)
	tokens := make([]string, 10)

	// 获取多个锁
	for i := 0; i < 10; i++ {
		locks[i] = AcquireLeaseLock()
		locks[i].Token = utils.NewULID()
		tokens[i] = locks[i].Token
		assert.NotEmpty(t, tokens[i])
	}

	// 验证所有 token 都不同
	for i := 0; i < 10; i++ {
		for j := i + 1; j < 10; j++ {
			assert.NotEqual(t, tokens[i], tokens[j])
		}
	}

	// 释放所有锁
	for i := 0; i < 10; i++ {
		locks[i].ReleaseToPool()
	}

	// 再次获取锁，验证对象可能被复用但 token 不同
	newLocks := make([]*LeaseLock, 5)
	for i := 0; i < 5; i++ {
		newLocks[i] = AcquireLeaseLock()
		newLocks[i].Token = utils.NewULID()
		assert.NotEmpty(t, newLocks[i].Token)
		// 新 token 应该与之前的都不同
		for j := 0; j < 10; j++ {
			assert.NotEqual(t, tokens[j], newLocks[i].Token)
		}
		newLocks[i].ReleaseToPool()
	}
}

func BenchmarkNewLeaseLock(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ll := NewLeaseLock()
		_ = ll.Token
	}
}

func BenchmarkAcquireLeaseLock(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ll := AcquireLeaseLock()
		ll.ReleaseToPool()
	}
}

func BenchmarkLeaseLockToBytes(b *testing.B) {
	ll := NewLeaseLock()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = ll.ToBytes()
	}
}

func BenchmarkLeaseLockToJSON(b *testing.B) {
	ll := NewLeaseLock()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = ll.ToJSON()
	}
}

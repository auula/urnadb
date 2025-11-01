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
	originalToken := ll.Token

	assert.NotEmpty(t, originalToken)

	ll.ReleaseToPool()
	assert.Equal(t, nullString, ll.Token)

	// 验证对象被放回池中
	ll2 := AcquireLeaseLock()
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
	tokens := make([]string, goroutines)
	var wg sync.WaitGroup

	// 并发获取锁
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			ll := AcquireLeaseLock()
			tokens[index] = ll.Token
			ll.ReleaseToPool()
		}(i)
	}

	wg.Wait()

	// 验证所有 Token 都是唯一的
	tokenSet := make(map[string]bool)
	for _, token := range tokens {
		assert.NotEmpty(t, token)
		assert.False(t, tokenSet[token], "Token should be unique: %s", token)
		tokenSet[token] = true
	}
}

func TestLeaseLockPoolReuse(t *testing.T) {
	// 获取一个锁
	ll1 := AcquireLeaseLock()
	token1 := ll1.Token
	
	// 释放回池
	ll1.ReleaseToPool()
	
	// 再次获取锁（可能是同一个对象）
	ll2 := AcquireLeaseLock()
	token2 := ll2.Token
	
	// Token 应该不同（即使对象可能被复用）
	assert.NotEqual(t, token1, token2)
	assert.NotEmpty(t, token2)
	
	ll2.ReleaseToPool()
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

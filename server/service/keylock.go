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

import "sync"

// keyLocks 为每个 key 提供一把读写锁。等待者也会被计数，
// 最后一个使用者离开时锁才会被移除，否则等待者和新来者会拿到同一个 key 的两把不同的锁。
type keyLocks struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

type keyLock struct {
	sync.RWMutex
	users int
}

// Lock 获取写锁，返回对应的解锁函数
func (kl *keyLocks) Lock(key string) func() {
	lock := kl.acquire(key)
	lock.Lock()

	return func() {
		lock.Unlock()
		kl.release(key, lock)
	}
}

// RLock 获取读锁，返回对应的解锁函数
func (kl *keyLocks) RLock(key string) func() {
	lock := kl.acquire(key)
	lock.RLock()

	return func() {
		lock.RUnlock()
		kl.release(key, lock)
	}
}

func (kl *keyLocks) acquire(key string) *keyLock {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	if kl.locks == nil {
		kl.locks = make(map[string]*keyLock)
	}

	lock, ok := kl.locks[key]
	if !ok {
		lock = new(keyLock)
		kl.locks[key] = lock
	}
	lock.users++

	return lock
}

func (kl *keyLocks) release(key string, lock *keyLock) {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	lock.users--
	if lock.users == 0 {
		delete(kl.locks, key)
	}
}

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

const (
	nullString = ""
)

// 创建一个对象池
var leaseLockPools = sync.Pool{
	New: func() any {
		return new(LeaseLock)
	},
}

func init() {
	// 预先填充池中的对象，把对象放入池中
	for i := 0; i < 10; i++ {
		// 预先填充的 ulid 时间戳部分一定早于当前实时生成的
		leaseLockPools.Put(NewLeaseLock())
	}
}

// LeaseLock 定义了一个同步锁结构体
type LeaseLock struct {
	// Token 是锁的唯一标识，解锁的时候客户端需要提供相同的 Token 才能解锁，除非锁已经过期。
	Token string `json:"token" msgpack:"token"`
}

// NewLeaseLock 创建一个新的 LeaseLock 实例带有唯一的 Token
func NewLeaseLock() *LeaseLock {
	return &LeaseLock{
		Token: utils.NewULID(),
	}
}

// 从对象池获取一个 LeaseLock ，内存被复用但是锁 Token 不会被复用
func AcquireLeaseLock() *LeaseLock {
	return leaseLockPools.Get().(*LeaseLock)
}

// 放回对象池，清理数据
func (ll *LeaseLock) Clear() {
	ll.Token = nullString
}

// 其实这样里方便的是 utils.ReleaseToPool 可以直接调用，
// 如果是 Java8 那种完全就没必要实现这个，直接在接口中提供默认的实现。
func (ll *LeaseLock) ReleaseToPool() {
	ll.Clear()
	leaseLockPools.Put(ll)
}

// ToBytes 是给 AcquirePoolSegment 内部使用
func (ll *LeaseLock) ToBytes() ([]byte, error) {
	return msgpack.Marshal(&ll.Token)
}

// ToJSON 是给 segment 内部类型转换使用
func (ll *LeaseLock) ToJSON() ([]byte, error) {
	return json.Marshal(&ll.Token)
}

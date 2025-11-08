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

package services

import (
	"errors"
	"sync"
	"time"

	"github.com/auula/urnadb/clog"
	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/utils"
	"github.com/auula/urnadb/vfs"
)

var (
	ErrAlreadyLocked   = errors.New("resource already locked")
	ErrLockNotFound    = errors.New("resource lock not found")
	ErrInvalidToken    = errors.New("invalid lock token")
	ErrInvalidLeaseTTL = errors.New("lock lifetime must not be negative")
)

type LocksService interface {
	ReleaseLock(name string, token string) error
	AcquireLock(name string, ttl int64) (lock *types.LeaseLock, err error)
	DoLeaseLock(name string, token string) (lock *types.LeaseLock, err error)
}

type LeaseLockService struct {
	// 锁是对象的一部分，而不是指向对象的资源
	atomicLeaseLocks sync.Map
	storage          *vfs.LogStructuredFS
}

func NewLocksServiceImpl(storage *vfs.LogStructuredFS) LocksService {
	return &LeaseLockService{
		storage: storage,
	}
}

func (s *LeaseLockService) acquireLeaseLock(key string) *sync.Mutex {
	actual, _ := s.atomicLeaseLocks.LoadOrStore(key, new(sync.Mutex))
	return actual.(*sync.Mutex)
}

func (s *LeaseLockService) ReleaseLock(name string, token string) error {
	if !s.storage.IsActive(name) {
		return ErrLockNotFound
	}

	s.acquireLeaseLock(name).Lock()

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		s.acquireLeaseLock(name).Unlock()
		clog.Errorf("[LocksService.ReleaseLock] %v", err)
		return err
	}

	slock, err := seg.ToLeaseLock()
	if err != nil {
		seg.ReleaseToPool()
		s.acquireLeaseLock(name).Unlock()
		clog.Errorf("[LocksService.ReleaseLock] %v", err)
		return err
	}

	defer utils.ReleaseToPool(seg, slock)

	if slock.Token != token {
		s.acquireLeaseLock(name).Unlock()
		return ErrInvalidToken
	}

	err = s.storage.DeleteSegment(name)
	if err != nil {
		s.acquireLeaseLock(name).Unlock()
		clog.Errorf("[LocksService.ReleaseLock] %v", err)
		return err
	}

	s.acquireLeaseLock(name).Unlock()
	s.atomicLeaseLocks.Delete(name)
	return nil
}

func (s *LeaseLockService) AcquireLock(name string, ttl int64) (*types.LeaseLock, error) {
	// 存在则表示锁已经存在，意味着同一把锁还没有过期，同一资源还未过期。
	if s.storage.IsActive(name) {
		return nil, ErrAlreadyLocked
	}

	s.acquireLeaseLock(name).Lock()
	defer s.acquireLeaseLock(name).Unlock()

	if ttl < 0 {
		return nil, ErrInvalidLeaseTTL
	}

	// 创建一把新租期锁并且设置锁的租期
	lease := types.AcquireLeaseLock()
	// 尝试创建 segment
	seg, err := vfs.AcquirePoolSegment(name, lease, ttl)
	if err != nil {
		utils.ReleaseToPool(lease)
		clog.Errorf("[LocksService.AcquireLock] %v", err)
		return nil, err
	}

	// 持久化这把租期锁
	err = s.storage.PutSegment(name, seg)
	if err != nil {
		utils.ReleaseToPool(lease, seg)
		clog.Errorf("[LocksService.AcquireLock] %v", err)
		return nil, err
	}

	seg.ReleaseToPool()

	return lease, nil
}

// 续租一定要注意服务器中途宕机了，客户端还认为服务器还活着，客户端也要有一个超时，如果超时了客户端抛出异常准备回滚。
// 正常续租成功了，应该更换客户端的 token 凭证，解锁的时候需要使用这个 token 作为凭证。
func (s *LeaseLockService) DoLeaseLock(name string, token string) (*types.LeaseLock, error) {
	if !s.storage.IsActive(name) {
		return nil, ErrLockNotFound
	}

	s.acquireLeaseLock(name).Lock()
	defer s.acquireLeaseLock(name).Unlock()

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		clog.Errorf("[LocksService.DoLeaseLock] %v", err)
		return nil, err
	}

	old, err := seg.ToLeaseLock()
	if err != nil {
		seg.ReleaseToPool()
		clog.Errorf("[LocksService.DoLeaseLock] %v", err)
		return nil, err
	}

	defer utils.ReleaseToPool(seg, old)

	if old.Token != token {
		return nil, ErrInvalidToken
	}

	// 创建一把新租期锁并且设置锁的租期，租期锁一定有存活时间的，默认是续租期 10s 秒
	newlease := types.AcquireLeaseLock()
	newttl := int64(10)
	if seg.ExpiredAt > 0 {
		// 类似于滑动窗口，把锁到期时间向后移动，续租是时间和前一个租期时间一致
		newttl = (seg.ExpiredAt - seg.CreatedAt) / int64(time.Microsecond)
	}

	// 持久化这把新租期锁
	newseg, err := vfs.AcquirePoolSegment(name, newlease, newttl)
	if err != nil {
		utils.ReleaseToPool(newlease)
		clog.Errorf("[LocksService.DoLeaseLock] %v", err)
		return nil, err
	}

	err = s.storage.PutSegment(name, newseg)
	if err != nil {
		clog.Errorf("[LocksService.DoLeaseLock] %v", err)
		return nil, err
	}

	newseg.ReleaseToPool()

	return newlease, nil
}

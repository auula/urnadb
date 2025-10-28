package services

import (
	"errors"
	"sync"
	"time"

	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/utils"
	"github.com/auula/urnadb/vfs"
)

var (
	ErrAlreadyLocked  = errors.New("resource already locked.")
	ErrLockNotFound   = errors.New("resource lock not found.")
	ErrInvalidToken   = errors.New("invalid lock token.")
	ErrStorageFailure = errors.New("storage operation failed.")
)

type LockService interface {
	ReleaseLock(name string, token string) error
	AcquireLock(name string, ttl int64) (lock *types.LeaseLock, err error)
	DoLeaseLock(name string, token string) (lock *types.LeaseLock, err error)
}

type LeaseLockService struct {
	// 注意这把锁可能会永久不会删除，需要主动删除???
	atomicLeaseLocks *sync.Map
	storage          *vfs.LogStructuredFS
}

func NewLockServiceImpl(storage *vfs.LogStructuredFS) LockService {
	return &LeaseLockService{
		atomicLeaseLocks: new(sync.Map),
		storage:          storage,
	}
}

func (s *LeaseLockService) acquireLeaseLock(key string) *sync.Mutex {
	actual, _ := s.atomicLeaseLocks.LoadOrStore(key, new(sync.Mutex))
	return actual.(*sync.Mutex)
}

func (s *LeaseLockService) ReleaseLock(name string, token string) error {
	s.acquireLeaseLock(name).Lock()

	if !s.storage.HasSegment(name) {
		s.acquireLeaseLock(name).Unlock()
		return ErrLockNotFound
	}

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		s.acquireLeaseLock(name).Unlock()
		return err
	}

	slock, err := seg.ToLeaseLock()
	if err != nil {
		seg.ReleaseToPool()
		s.acquireLeaseLock(name).Unlock()
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
		return err
	}

	s.acquireLeaseLock(name).Unlock()
	s.atomicLeaseLocks.Delete(name)
	return nil
}

func (s *LeaseLockService) AcquireLock(name string, ttl int64) (*types.LeaseLock, error) {
	s.acquireLeaseLock(name).Lock()
	defer s.acquireLeaseLock(name).Unlock()

	// 存在则表示锁已经存在，意味着同一把锁还没有过期，同一资源还未过期。
	if s.storage.HasSegment(name) {
		return nil, ErrAlreadyLocked
	}

	// 创建一把新租期锁并且设置锁的租期
	lease := types.AcquireLeaseLock()
	// 尝试创建 segment
	seg, err := vfs.AcquirePoolSegment(name, lease, ttl)
	if err != nil {
		utils.ReleaseToPool(lease)
		return nil, err
	}

	// 持久化这把租期锁
	err = s.storage.PutSegment(name, seg)
	if err != nil {
		utils.ReleaseToPool(lease, seg)
		return nil, err
	}

	seg.ReleaseToPool()

	return lease, nil
}

func (s *LeaseLockService) DoLeaseLock(name string, token string) (*types.LeaseLock, error) {
	s.acquireLeaseLock(name).Lock()
	defer s.acquireLeaseLock(name).Unlock()

	if !s.storage.HasSegment(name) {
		return nil, ErrLockNotFound
	}

	_, seg, err := s.storage.FetchSegment(name)
	if err != nil {
		return nil, err
	}

	old, err := seg.ToLeaseLock()
	if err != nil {
		seg.ReleaseToPool()
		return nil, err
	}

	defer utils.ReleaseToPool(seg, old)

	if old.Token != token {
		return nil, ErrInvalidToken
	}

	// 创建一把新租期锁并且设置锁的租期，租期锁一定有存活时间的，默认是续租期 10s 秒
	newlease := types.AcquireLeaseLock()
	newTTL := int64(10)
	if seg.ExpiredAt > 0 {
		// 类似于滑动窗口，把锁到期时间向后移动
		newTTL = (seg.ExpiredAt - seg.CreatedAt) / int64(time.Microsecond)
	}

	// 持久化这把新租期锁
	newseg, err := vfs.AcquirePoolSegment(name, newlease, newTTL)
	if err != nil {
		utils.ReleaseToPool(newlease)
		return nil, err
	}

	err = s.storage.PutSegment(name, newseg)
	if err != nil {
		return nil, err
	}

	newseg.ReleaseToPool()

	return newlease, nil
}

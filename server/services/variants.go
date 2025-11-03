package services

import (
	"errors"
	"sync"

	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/vfs"
)

// 如果 Number 类型要完成类似于 redis 的 increment 的操作，
// 客户端只需要发生算数运输的偏移量即可，最终操作中服务器端完成运算和持久化。
type VariantService interface {
	GetVariant(name string) (*types.Variant, error)
	SetVariant(name string, value *types.Variant, ttl int64) error
	Increment(name string, delta float64) (float64, error)
}

func (vs *VariantServiceImpl) acquireTablesLock(key string) *sync.RWMutex {
	actual, _ := vs.vlock.LoadOrStore(key, new(sync.RWMutex))
	return actual.(*sync.RWMutex)
}

type VariantServiceImpl struct {
	storage *vfs.LogStructuredFS
	vlock   sync.Map
}

// 构造函数 - 需要指定类型参数
func NewVariantServiceImpl(storage *vfs.LogStructuredFS) VariantService {
	return &VariantServiceImpl{
		storage: storage,
	}
}

// GetVariant 获取变量值
func (vs *VariantServiceImpl) GetVariant(name string) (*types.Variant, error) {
	vs.acquireTablesLock(name).RLock()
	defer vs.acquireTablesLock(name).RUnlock()

	_, seg, err := vs.storage.FetchSegment(name)
	if err != nil {
		return nil, err
	}

	return seg.ToVariant()
}

// SetVariant 设置变量值
func (vs *VariantServiceImpl) SetVariant(name string, value *types.Variant, ttl int64) error {
	vs.acquireTablesLock(name).Lock()
	defer vs.acquireTablesLock(name).Unlock()

	seg, err := vfs.AcquirePoolSegment(name, value, ttl)
	if err != nil {
		return err
	}

	return vs.storage.PutSegment(name, seg)
}

// Increment 增量操作 - 只对数值类型有效
func (vs *VariantServiceImpl) Increment(name string, delta float64) (float64, error) {
	vs.acquireTablesLock(name).Lock()
	defer vs.acquireTablesLock(name).Unlock()

	_, seg, err := vs.storage.FetchSegment(name)
	if err != nil {
		return 0, err
	}

	variant, err := seg.ToVariant()
	if err != nil {
		return 0, err
	}

	// 过滤非数值类型
	if variant.IsBool() || variant.IsString() {
		return 0, errors.New("varinat value is bool or string")
	}

	res_num := variant.AddFloat64(delta)

	ttl, ok := seg.ExpiresIn()
	if !ok {
		return 0, nil
	}

	seg, err = vfs.AcquirePoolSegment(name, variant, ttl)
	if err != nil {
		return 0, err
	}

	err = vs.storage.PutSegment(name, seg)
	if err != nil {
		return 0, err
	}

	return res_num, nil
}

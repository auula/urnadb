package services

import (
	"sync"

	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/vfs"
)

// 如果 Number 类型要完成类似于 redis 的 increment 的操作，
// 客户端只需要发生算数运输的偏移量即可，最终操作中服务器端完成运算和持久化。
type VariantService[N types.Number, V types.Variant] interface {
	GetVariant(name string) (V, error)
	SetVariant(name string, value V) error
	Increment(name string, delta N) error
}

func (vs *VariantServiceImpl[N, V]) acquireTablesLock(key string) *sync.RWMutex {
	actual, _ := vs.vlock.LoadOrStore(key, new(sync.RWMutex))
	return actual.(*sync.RWMutex)
}

type VariantServiceImpl[N types.Number, V types.Variant] struct {
	storage *vfs.LogStructuredFS
	vlock   sync.Map
}

// 构造函数 - 需要指定类型参数
func NewVariantServiceImpl[N types.Number, V types.Variant](storage *vfs.LogStructuredFS) VariantService[N, V] {
	return &VariantServiceImpl[N, V]{
		storage: storage,
	}
}

// GetVariant 获取变量值
func (vs *VariantServiceImpl[N, V]) GetVariant(name string) (V, error) {
	vs.acquireTablesLock(name).RLock()
	defer vs.acquireTablesLock(name).RUnlock()
	var value V

	_, seg, err := vs.storage.FetchSegment(name)
	if err != nil {
		return value, err
	}

	return value, nil
}

// SetVariant 设置变量值
func (vs *VariantServiceImpl[N, V]) SetVariant(name string, value V) error {
	return nil
}

// Increment 增量操作 - 只对数值类型有效
func (vs *VariantServiceImpl[N, V]) Increment(name string, delta N) error {
	return nil
}

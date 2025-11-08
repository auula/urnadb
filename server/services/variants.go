package services

import (
	"errors"
	"sync"

	"github.com/auula/urnadb/clog"
	"github.com/auula/urnadb/types"
	"github.com/auula/urnadb/utils"
	"github.com/auula/urnadb/vfs"
)

var (
	ErrVariantNotFound = errors.New("variant not found")
	ErrVariantExpired  = errors.New("variant ttl is invalid or expired")
)

// 如果 Number 类型要完成类似于 redis 的 increment 的操作，
// 客户端只需要发生算数运输的偏移量即可，最终操作中服务器端完成运算和持久化。
type VariantsService interface {
	GetVariant(name string) (*types.Variant, error)
	SetVariant(name string, value *types.Variant, ttl int64) error
	Increment(name string, delta float64) (float64, error)
	DeleteVariant(name string) error
}

func (vs *VariantsServiceImpl) acquireVariantLock(key string) *sync.RWMutex {
	actual, _ := vs.vlock.LoadOrStore(key, new(sync.RWMutex))
	return actual.(*sync.RWMutex)
}

type VariantsServiceImpl struct {
	storage *vfs.LogStructuredFS
	vlock   sync.Map
}

// 构造函数 - 需要指定类型参数
func NewVariantsServiceImpl(storage *vfs.LogStructuredFS) VariantsService {
	return &VariantsServiceImpl{
		storage: storage,
	}
}

// GetVariant 获取变量值
func (vs *VariantsServiceImpl) GetVariant(name string) (*types.Variant, error) {
	vs.acquireVariantLock(name).RLock()
	defer vs.acquireVariantLock(name).RUnlock()

	_, seg, err := vs.storage.FetchSegment(name)
	if err != nil {
		clog.Errorf("[VariantsService.GetVariant] %v", err)
		return nil, err
	}

	return seg.ToVariant()
}

// SetVariant 设置变量值
func (vs *VariantsServiceImpl) SetVariant(name string, value *types.Variant, ttl int64) error {
	vs.acquireVariantLock(name).Lock()
	defer vs.acquireVariantLock(name).Unlock()

	seg, err := vfs.AcquirePoolSegment(name, value, ttl)
	if err != nil {
		clog.Errorf("[VariantsService.SetVariant] %v", err)
		return err
	}

	defer seg.ReleaseToPool()

	return vs.storage.PutSegment(name, seg)
}

// Increment 增量操作 - 只对数值类型有效
func (vs *VariantsServiceImpl) Increment(name string, delta float64) (float64, error) {
	if !vs.storage.IsActive(name) {
		return 0, ErrVariantNotFound
	}

	vs.acquireVariantLock(name).Lock()
	defer vs.acquireVariantLock(name).Unlock()

	_, seg, err := vs.storage.FetchSegment(name)
	if err != nil {
		clog.Errorf("[VariantsService.Increment] %v", err)
		return 0, err
	}

	variant, err := seg.ToVariant()
	if err != nil {
		clog.Errorf("[VariantsService.Increment] %v", err)
		return 0, err
	}

	// 过滤非数值类型
	if variant.IsBool() || variant.IsString() {
		return 0, errors.New("varinat value is bool or string")
	}

	res_num := variant.AddFloat64(delta)

	ttl, ok := seg.ExpiresIn()
	if !ok {
		return 0, ErrVariantExpired
	}

	defer utils.ReleaseToPool(seg, variant)

	seg, err = vfs.AcquirePoolSegment(name, variant, ttl)
	if err != nil {
		clog.Errorf("[VariantsService.Increment] %v", err)
		return 0, err
	}

	err = vs.storage.PutSegment(name, seg)
	if err != nil {
		clog.Errorf("[VariantsService.Increment] %v", err)
		return 0, err
	}

	return res_num, nil
}

func (vs *VariantsServiceImpl) DeleteVariant(name string) error {
	if !vs.storage.IsActive(name) {
		return ErrVariantNotFound
	}

	vs.acquireVariantLock(name).Lock()

	err := vs.storage.DeleteSegment(name)
	if err != nil {
		clog.Errorf("[VariantsService.DeleteVariant] %v", err)
		return err
	}

	vs.acquireVariantLock(name).Unlock()
	vs.vlock.Delete(name)

	return nil
}

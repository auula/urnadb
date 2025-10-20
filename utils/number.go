package utils

import (
	"fmt"
	"math"
)

// 把 uint64 转 int64 前做安全检查
func Uint64ToInt64Safe(u uint64) (int64, error) {
	if u > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("value too large for int64: %d", u)
	}
	return int64(u), nil
}

// 把 int64 转 uint64 前做安全检查（防负）
func Int64ToUint64Safe(s int64) (uint64, error) {
	if s < 0 {
		return 0, fmt.Errorf("negative value cannot be converted to uint64: %d", s)
	}
	return uint64(s), nil
}

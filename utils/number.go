package utils

import (
	"fmt"
	"math"
	"strings"
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

// 第一个 bool 值为 true 就是数字，第二个 bool 是负数就是 true ,第三个 bool 是小数
func IsStrictNumber(s string) (isNumber bool, isNegative bool, isFloat bool) {
	if s == "" {
		return false, false, false
	}

	// 处理符号
	hasMinus := s[0] == '-'
	clean := s
	if hasMinus || s[0] == '+' {
		clean = s[1:]
	}

	if clean == "" {
		return false, hasMinus, false
	}

	// 检查小数点，拿到小数点出现的位置
	dotIndex := strings.Index(clean, ".")
	hasDot := dotIndex != -1
	isFloat = hasDot

	// 是带小数点字符串
	if hasDot {
		// 严格检查：小数点前后都必须有数字
		if dotIndex == 0 || dotIndex == len(clean)-1 {
			return false, hasMinus, isFloat
		}
		// 检查是否包含多个小数点
		if strings.Count(clean, ".") > 1 {
			return false, hasMinus, isFloat
		}
	}

	// 验证字符
	hasDigit := false
	for _, ch := range clean {
		if ch >= '0' && ch <= '9' {
			hasDigit = true
			continue
		}
		if ch == '.' {
			continue
		}
		return false, hasMinus, isFloat
	}

	return hasDigit, hasMinus, isFloat
}

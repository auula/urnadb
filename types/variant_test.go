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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNewVariant(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{"string variant", "hello world", "hello world"},
		{"int64 variant", int64(42), int64(42)},
		{"float64 variant", 3.14159, 3.14159},
		{"bool variant true", true, true},
		{"bool variant false", false, false},
		{"zero values", int64(0), int64(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := NewVariant(tt.input)
			assert.Equal(t, tt.expected, variant.Value)
		})
	}
}

func TestVariant_String(t *testing.T) {
	tests := []struct {
		name        string
		input       any
		expected    string
		shouldPanic bool
	}{
		{"valid string", "test string", "test string", false},
		{"empty string", "", "", false},
		{"non-string panics", int64(100), "", true},
		{"bool panics", true, "", true},
		{"float panics", 1.23, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := NewVariant(tt.input)

			if tt.shouldPanic {
				assert.Panics(t, func() {
					_ = variant.String()
				})
			} else {
				result := variant.String()
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestVariant_AddInt64(t *testing.T) {
	tests := []struct {
		name        string
		input       any
		delta       int64
		expected    int64
		shouldPanic bool
	}{
		{"positive addition", int64(10), 5, 15, false},
		{"negative addition", int64(100), -50, 50, false},
		{"zero addition", int64(42), 0, 42, false},
		{"large numbers", int64(1<<63 - 1), 0, 1<<63 - 1, false},
		{"non-int64 panics", "string", 10, 0, true},
		{"float panics", 3.14, 5, 0, true},
		{"bool panics", true, 1, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := NewVariant(tt.input)

			if tt.shouldPanic {
				assert.Panics(t, func() {
					_ = variant.AddInt64(tt.delta)
				})
			} else {
				result := variant.AddInt64(tt.delta)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestVariant_AddFloat64(t *testing.T) {
	tests := []struct {
		name        string
		input       any
		delta       float64
		expected    float64
		shouldPanic bool
	}{
		{"float addition", 10.5, 2.5, 13.0, false},
		{"negative float addition", 100.0, -25.5, 74.5, false},
		{"decimal precision", 1.1, 0.2, 1.3, false},
		{"zero addition", 3.14, 0.0, 3.14, false},
		{"non-float panics", "string", 1.0, 0, true},
		{"int64 panics", int64(10), 1.0, 0, true},
		{"bool panics", true, 1.0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := NewVariant(tt.input)

			if tt.shouldPanic {
				assert.Panics(t, func() {
					_ = variant.AddFloat64(tt.delta)
				})
			} else {
				result := variant.AddFloat64(tt.delta)
				assert.InEpsilon(t, tt.expected, result, 0.0001) // 使用精度比较
			}
		})
	}
}

func TestVariant_Bool(t *testing.T) {
	tests := []struct {
		name        string
		input       any
		expected    bool
		shouldPanic bool
	}{
		{"true bool", true, true, false},
		{"false bool", false, false, false},
		{"non-bool panics", "string", false, true},
		{"int panics", int64(1), false, true},
		{"float panics", 1.0, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := NewVariant(tt.input)

			if tt.shouldPanic {
				assert.Panics(t, func() {
					_ = variant.Bool()
				})
			} else {
				result := variant.Bool()
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestVariant_ToBytes(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{"string msgpack", "hello", "hello"},
		{"int64 msgpack", int64(255), int64(255)},
		{"float64 msgpack", 2.718, 2.718},
		{"bool msgpack true", true, true},
		{"bool msgpack false", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := NewVariant(tt.input)

			data, err := variant.ToBytes()
			assert.NoError(t, err)
			assert.NotEmpty(t, data)

			// 验证可以正确反序列化
			var result any
			err = msgpack.Unmarshal(data, &result)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestVariant_ToJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"string json", "hello", `"hello"`},
		{"int64 json", int64(100), "100"},
		{"float64 json", 3.14, "3.14"},
		{"bool json true", true, "true"},
		{"bool json false", false, "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			variant := NewVariant(tt.input)

			data, err := variant.ToJSON()
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, string(data))
		})
	}
}

func TestVariant_SerializationRoundtrip(t *testing.T) {
	tests := []struct {
		name  string
		input any
	}{
		{"string roundtrip", "roundtrip test"},
		{"int64 roundtrip", int64(12345)},
		{"float64 roundtrip", 123.456},
		{"bool roundtrip", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := NewVariant(tt.input)

			// 测试 msgpack 往返
			msgpackData, err := original.ToBytes()
			assert.NoError(t, err)

			var msgpackResult any
			err = msgpack.Unmarshal(msgpackData, &msgpackResult)
			assert.NoError(t, err)
			assert.Equal(t, tt.input, msgpackResult)

			// 测试 JSON 往返（数值类型会有类型变化）
			jsonData, err := original.ToJSON()
			assert.NoError(t, err)

			var jsonResult any
			err = json.Unmarshal(jsonData, &jsonResult)
			assert.NoError(t, err)

			// JSON 反序列化后类型可能变化，需要类型断言比较
			switch v := tt.input.(type) {
			case string:
				assert.Equal(t, v, jsonResult.(string))
			case int64:
				// JSON 数字会变成 float64
				assert.Equal(t, float64(v), jsonResult.(float64))
			case float64:
				assert.Equal(t, v, jsonResult.(float64))
			case bool:
				assert.Equal(t, v, jsonResult.(bool))
			}
		})
	}
}

func TestVariant_EdgeCases(t *testing.T) {
	t.Run("nil value", func(t *testing.T) {
		variant := NewVariant(nil)
		assert.Nil(t, variant.Value)

		// nil 值的序列化测试
		data, err := variant.ToBytes()
		assert.NoError(t, err)

		var result any
		err = msgpack.Unmarshal(data, &result)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("max int64", func(t *testing.T) {
		variant := NewVariant(int64(1<<63 - 1))
		result := variant.AddInt64(0) // 加0测试
		assert.Equal(t, int64(1<<63-1), result)
	})

	t.Run("float precision", func(t *testing.T) {
		variant := NewVariant(0.1 + 0.2)
		result := variant.AddFloat64(0.0)
		assert.InEpsilon(t, 0.3, result, 0.0001)
	})
}

func TestVariant_TypeSafety(t *testing.T) {
	t.Run("correct type methods work", func(t *testing.T) {
		// 字符串类型
		strVariant := NewVariant("test")
		assert.NotPanics(t, func() { _ = strVariant.String() })

		// 整数类型
		intVariant := NewVariant(int64(100))
		assert.NotPanics(t, func() { _ = intVariant.AddInt64(10) })

		// 浮点类型
		floatVariant := NewVariant(1.5)
		assert.NotPanics(t, func() { _ = floatVariant.AddFloat64(0.5) })

		// 布尔类型
		boolVariant := NewVariant(true)
		assert.NotPanics(t, func() { _ = boolVariant.Bool() })
	})

	t.Run("wrong type methods panic", func(t *testing.T) {
		strVariant := NewVariant("not_a_number")
		assert.Panics(t, func() { _ = strVariant.AddInt64(1) })
		assert.Panics(t, func() { _ = strVariant.AddFloat64(1.0) })

		intVariant := NewVariant(int64(100))
		assert.Panics(t, func() { _ = intVariant.String() })
		assert.Panics(t, func() { _ = intVariant.Bool() })
	})
}

// 性能测试
func BenchmarkVariant_ToBytes(b *testing.B) {
	variant := NewVariant("benchmark string")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data, _ := variant.ToBytes()
		_ = data
	}
}

func BenchmarkVariant_ToJSON(b *testing.B) {
	variant := NewVariant("benchmark string")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data, _ := variant.ToJSON()
		_ = data
	}
}

func BenchmarkVariant_AddInt64(b *testing.B) {
	variant := NewVariant(int64(100))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = variant.AddInt64(int64(i))
	}
}

func BenchmarkVariant_AddFloat64(b *testing.B) {
	variant := NewVariant(100.0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = variant.AddFloat64(float64(i))
	}
}

// 测试对象池功能
func TestVariantPool(t *testing.T) {
	t.Run("acquire and release", func(t *testing.T) {
		v1 := AcquireVariant()
		assert.NotNil(t, v1)

		v1.Value = "test"
		assert.Equal(t, "test", v1.Value)

		v1.ReleaseToPool()

		v2 := AcquireVariant()
		assert.NotNil(t, v2)
		assert.NotEqual(t, "test", v2.Value)
	})
}

// 测试 Clear 方法
func TestVariant_Clear(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{"clear string", "hello", ""},
		{"clear int64", int64(100), int64(0)},
		{"clear float64", 3.14, 0.0},
		{"clear bool", true, false},
		{"clear unknown type", []int{1, 2, 3}, float64(0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewVariant(tt.input)
			v.Clear()
			assert.Equal(t, tt.expected, v.Value)
		})
	}
}

// 测试类型检查方法
func TestVariant_IsString(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected bool
	}{
		{"string is string", "hello", true},
		{"empty string is string", "", true},
		{"int64 is not string", int64(100), false},
		{"float64 is not string", 3.14, false},
		{"bool is not string", true, false},
		{"nil is not string", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewVariant(tt.input)
			result := v.IsString()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestVariant_IsNumber(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected bool
	}{
		{"int64 is number", int64(100), true},
		{"float64 is number", 3.14, true},
		{"string is not number", "hello", false},
		{"bool is not number", true, false},
		{"nil is not number", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewVariant(tt.input)
			result := v.IsNumber()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestVariant_IsBool(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected bool
	}{
		{"true is bool", true, true},
		{"false is bool", false, true},
		{"string is not bool", "hello", false},
		{"int64 is not bool", int64(100), false},
		{"float64 is not bool", 3.14, false},
		{"nil is not bool", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewVariant(tt.input)
			result := v.IsBool()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestVariant_IsVariant(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected bool
	}{
		{"complex type is variant", map[string]int{"a": 1}, true},
		{"slice is variant", []int{1, 2, 3}, true},
		{"struct is variant", struct{ Name string }{Name: "test"}, true},
		{"nil is not variant", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := NewVariant(tt.input)
			result := v.IsVariant()
			assert.Equal(t, tt.expected, result)
		})
	}
}

// 测试 FromBytesSafe 方法
func TestVariant_FromBytesSafe(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected any
	}{
		{"string from bytes", "hello world", "hello world"},
		{"int64 from bytes", int64(12345), int64(12345)},
		{"float64 from bytes", 3.14159, 3.14159},
		{"bool from bytes", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := NewVariant(tt.input)
			data, err := original.ToBytes()
			assert.NoError(t, err)

			v := NewVariant(nil)
			err = v.FromBytesSafe(data)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, v.Value)
		})
	}

	t.Run("invalid data", func(t *testing.T) {
		v := NewVariant(nil)
		// 使用一个真正无效的 msgpack 格式
		// 0xC1 是 msgpack 中的保留字节，应该产生错误
		err := v.FromBytesSafe([]byte{0xC1})
		assert.Error(t, err)
	})
}

// 测试 nil 值处理
func TestVariant_NilHandling(t *testing.T) {
	t.Run("nil value string", func(t *testing.T) {
		v := NewVariant(nil)
		result := v.String()
		assert.Equal(t, "", result)
	})

	t.Run("nil value add int64", func(t *testing.T) {
		v := NewVariant(nil)
		result := v.AddInt64(10)
		assert.Equal(t, int64(0), result)
	})

	t.Run("nil value add float64", func(t *testing.T) {
		v := NewVariant(nil)
		result := v.AddFloat64(1.5)
		assert.Equal(t, 0.0, result)
	})

	t.Run("nil value bool", func(t *testing.T) {
		v := NewVariant(nil)
		result := v.Bool()
		assert.Equal(t, false, result)
	})
}

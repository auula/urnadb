package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

func TestNewValue(t *testing.T) {
	t.Run("string value", func(t *testing.T) {
		val := NewValue("hello")
		assert.Equal(t, "hello", val.Value)
	})

	t.Run("int64 value", func(t *testing.T) {
		val := NewValue(int64(42))
		assert.Equal(t, int64(42), val.Value)
	})

	t.Run("float64 value", func(t *testing.T) {
		val := NewValue(3.14)
		assert.Equal(t, 3.14, val.Value)
	})

	t.Run("bool value", func(t *testing.T) {
		val := NewValue(true)
		assert.Equal(t, true, val.Value)
	})
}

func TestValue_String(t *testing.T) {
	t.Run("string type", func(t *testing.T) {
		val := NewValue("test string")
		result := val.String()
		assert.Equal(t, "test string", result)
	})

	t.Run("non-string type panics", func(t *testing.T) {
		val := NewValue(int64(100))
		assert.Panics(t, func() {
			_ = val.String()
		})
	})
}

func TestValue_Int64(t *testing.T) {
	t.Run("int64 type", func(t *testing.T) {
		val := NewValue(int64(123))
		result := val.Int64()
		assert.Equal(t, int64(123), result)
	})

	t.Run("non-int64 type panics", func(t *testing.T) {
		val := NewValue("not a number")
		assert.Panics(t, func() {
			val.Int64()
		})
	})
}

func TestValue_Float64(t *testing.T) {
	t.Run("float64 type", func(t *testing.T) {
		val := NewValue(3.14159)
		result := val.Float64()
		assert.Equal(t, 3.14159, result)
	})

	t.Run("non-float64 type panics", func(t *testing.T) {
		val := NewValue(true)
		assert.Panics(t, func() {
			val.Float64()
		})
	})
}

func TestValue_Bool(t *testing.T) {
	t.Run("bool type", func(t *testing.T) {
		val := NewValue(true)
		result := val.Bool()
		assert.Equal(t, true, result)
	})

	t.Run("non-bool type panics", func(t *testing.T) {
		val := NewValue("not a bool")
		assert.Panics(t, func() {
			val.Bool()
		})
	})
}

func TestValue_ToBytes(t *testing.T) {
	testCases := []struct {
		name     string
		value    interface{}
		expected interface{}
	}{
		{"string", "hello world", "hello world"},
		{"int64", int64(42), int64(42)},
		{"float64", 2.718, 2.718},
		{"bool", true, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			switch v := tc.value.(type) {
			case string:
				val := NewValue(v)
				data, err := val.ToBytes()
				assert.NoError(t, err)

				var result string
				err = msgpack.Unmarshal(data, &result)
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)

			case int64:
				val := NewValue(v)
				data, err := val.ToBytes()
				assert.NoError(t, err)

				var result int64
				err = msgpack.Unmarshal(data, &result)
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)

			case float64:
				val := NewValue(v)
				data, err := val.ToBytes()
				assert.NoError(t, err)

				var result float64
				err = msgpack.Unmarshal(data, &result)
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)

			case bool:
				val := NewValue(v)
				data, err := val.ToBytes()
				assert.NoError(t, err)

				var result bool
				err = msgpack.Unmarshal(data, &result)
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestValue_ToJSON(t *testing.T) {
	testCases := []struct {
		name     string
		value    interface{}
		expected interface{}
	}{
		{"string", "json test", "json test"},
		{"int64", int64(100), float64(100)}, // JSON unmarshals numbers as float64
		{"float64", 1.618, 1.618},
		{"bool", false, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			switch v := tc.value.(type) {
			case string:
				val := NewValue(v)
				data, err := val.ToJSON()
				assert.NoError(t, err)
				assert.Equal(t, `"json test"`, string(data))

			case int64:
				val := NewValue(v)
				data, err := val.ToJSON()
				assert.NoError(t, err)
				assert.Equal(t, "100", string(data))

			case float64:
				val := NewValue(v)
				data, err := val.ToJSON()
				assert.NoError(t, err)
				assert.Equal(t, "1.618", string(data))

			case bool:
				val := NewValue(v)
				data, err := val.ToJSON()
				assert.NoError(t, err)
				assert.Equal(t, "false", string(data))
			}
		})
	}
}

func TestValue_Integration(t *testing.T) {
	t.Run("roundtrip msgpack", func(t *testing.T) {
		original := NewValue("roundtrip test")
		data, err := original.ToBytes()
		assert.NoError(t, err)

		var restored string
		err = msgpack.Unmarshal(data, &restored)
		assert.NoError(t, err)
		assert.Equal(t, original.Value, restored)
	})

	t.Run("roundtrip json", func(t *testing.T) {
		original := NewValue(int64(255))
		data, err := original.ToJSON()
		assert.NoError(t, err)

		var restored int64
		err = json.Unmarshal(data, &restored)
		assert.NoError(t, err)
		assert.Equal(t, original.Value, restored)
	})
}

// 边缘情况测试
func TestValue_EdgeCases(t *testing.T) {
	t.Run("empty string", func(t *testing.T) {
		val := NewValue("")
		assert.Equal(t, "", val.Value)
		assert.Equal(t, "", val.String())
	})

	t.Run("zero values", func(t *testing.T) {
		intVal := NewValue(int64(0))
		floatVal := NewValue(0.0)
		boolVal := NewValue(false)

		assert.Equal(t, int64(0), intVal.Value)
		assert.Equal(t, 0.0, floatVal.Value)
		assert.Equal(t, false, boolVal.Value)
	})

	t.Run("max int64", func(t *testing.T) {
		val := NewValue(int64(1<<63 - 1))
		assert.Equal(t, int64(1<<63-1), val.Int64())
	})
}

// 性能测试
func BenchmarkValue_ToBytes(b *testing.B) {
	val := NewValue("benchmark string")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = val.ToBytes()
	}
}

func BenchmarkValue_ToJSON(b *testing.B) {
	val := NewValue("benchmark string")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _ = val.ToJSON()
	}
}

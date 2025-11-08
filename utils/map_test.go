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

package utils

import (
	"reflect"
	"testing"
)

func TestDeepMergeMaps(t *testing.T) {
	tests := []struct {
		name     string
		base     map[string]interface{}
		news     map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name: "simple overwrite",
			base: map[string]interface{}{"a": 1, "b": 2},
			news: map[string]interface{}{"b": 20, "c": 30},
			expected: map[string]interface{}{
				"a": 1,
				"b": 20,
				"c": 30,
			},
		},
		{
			name: "nested merge",
			base: map[string]interface{}{
				"a": map[string]interface{}{"x": 1, "y": 2},
				"b": 10,
			},
			news: map[string]interface{}{
				"a": map[string]interface{}{"y": 20, "z": 30},
				"b": 20,
				"c": 30,
			},
			expected: map[string]interface{}{
				"a": map[string]interface{}{"x": 1, "y": 20, "z": 30},
				"b": 20,
				"c": 30,
			},
		},
		{
			name: "type conflict",
			base: map[string]interface{}{
				"a": map[string]interface{}{"x": 1},
			},
			news: map[string]interface{}{
				"a": 100, // 类型冲突，覆盖 base["a"]
			},
			expected: map[string]interface{}{
				"a": 100,
			},
		},
		{
			name: "nested empty map",
			base: map[string]interface{}{
				"a": map[string]interface{}{"x": 1},
			},
			news: map[string]interface{}{
				"a": map[string]interface{}{}, // 空 map，不影响 base
			},
			expected: map[string]interface{}{
				"a": map[string]interface{}{"x": 1},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			DeepMergeMaps(tt.base, tt.news)
			if !reflect.DeepEqual(tt.base, tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, tt.base)
			}
		})
	}
}

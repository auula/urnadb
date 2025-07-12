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
	"testing"
)

// mockReusable 用于测试正常对象场景
type mockReusable struct {
	t *testing.T
}

func (m *mockReusable) ReleaseToPool() {
	// 防止直接 new(mockReusable)
	// 直接 new 会导致 m.t 为 nil
	if m.t != nil {
		m.t.Logf("mock reusable released to pool")
	}
}

func TestReleaseToPool(t *testing.T) {
	cases := []struct {
		name  string
		input []Reusable
	}{
		{"nil slice", nil},
		{"empty slice", []Reusable{}},
		{"slice with nils", []Reusable{nil, nil}},
		{"normal slice", []Reusable{new(mockReusable), &mockReusable{
			t: t,
		}}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// 可根据 ReleaseToPool 的副作用补充断言
			ReleaseToPool(c.input...)
		})
	}
}

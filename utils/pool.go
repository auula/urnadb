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

type Reusable interface {
	ReleaseToPool()
}

// ReleaseToPool releases one or more Reusable objects back to their respective pools.
// It accepts a variadic parameter of Reusable objects and returns them to the pool for reuse,
// helping to reduce memory allocations and improve performance.
func ReleaseToPool(pools ...Reusable) {
	if pools == nil {
		return
	}
	for _, p := range pools {
		if p == nil {
			continue
		}
		p.ReleaseToPool()
	}
}

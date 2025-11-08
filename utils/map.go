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

func DeepMergeMaps(base, news map[string]interface{}) {
	for k, v := range news {
		if inner, ok := v.(map[string]interface{}); ok {
			// v 是 map[string]interface{}
			if exist, ok := base[k].(map[string]interface{}); ok {
				// base[k] 也是 map[string]interface{}，递归合并
				DeepMergeMaps(exist, inner)
			} else {
				// base[k] 不存在或者类型不是 map，直接覆盖
				base[k] = inner
			}
		} else {
			// v 不是 map[string]interface{}，直接覆盖 base[k]
			base[k] = v
		}
	}
}

func SearchInMap(m map[string]any, key string) []any {
	var results []any
	if item, exists := m[key]; exists {
		results = append(results, item)
	}

	// 遍历 map，查找是否有嵌套的 map 类型
	for _, value := range m {
		if nestedMap, ok := value.(map[string]any); ok {
			// 递归查找嵌套的 map
			results = append(results, SearchInMap(nestedMap, key)...)
		}
	}

	return results
}

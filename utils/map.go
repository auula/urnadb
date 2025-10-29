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

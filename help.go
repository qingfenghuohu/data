package data

func RemoveDuplicateElement(data []DataCacheKey) []DataCacheKey {
	var result []DataCacheKey
	for _, v := range data {
		if len(result) == 0 {
			result = append(result, v)
		} else {
			for _, val := range result {
				if v.String() != val.String() {
					result = append(result, v)
				}
			}
		}
	}
	return result
}

func GetTypeDataCacheKey(data []DataCacheKey) map[string][]DataCacheKey {
	var result map[string][]DataCacheKey
	for _, v := range data {
		if ok := result[v.CType]; len(ok) == 0 {
			result[v.CType] = []DataCacheKey{}
		}
		result[v.CType] = append(result[v.CType], v)
	}
	return result
}

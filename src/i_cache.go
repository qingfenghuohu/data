package data

type IReal struct {
	ready    bool
	cacheKey DataCacheKey
	params   []interface{}
}

func IRealData(resetKey []DataCacheKey, dbData []RealCacheData) []RealCacheData {
	real := make(map[string]map[string][]DataCacheKey)
	realDb := make(map[string]ModelInfo)
	cacheKey := make(map[string]DataCacheKey)
	for _, v := range resetKey {
		tmpKey := v.Model.DbName() + "_" + v.Model.TableName()
		kk := CreateCacheKeyStr(v)
		cacheKey[kk] = v
		if ok := real[tmpKey]; len(ok) <= 0 {
			real[tmpKey] = map[string][]DataCacheKey{}
			realDb[tmpKey] = v.Model
		}
		if ok := real[tmpKey][v.Key]; len(ok) <= 0 {
			real[tmpKey][v.Key] = []DataCacheKey{}
		}
		real[tmpKey][v.Key] = append(real[tmpKey][v.Key], v)
	}
	for key, value := range real {
		res := realDb[key].GetRealData(value)
		for k, v := range res {
			dbData = append(dbData, RealCacheData{CacheKey: cacheKey[k], Result: v})
		}
	}
	return dbData
}

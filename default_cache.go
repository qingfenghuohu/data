package data

import "github.com/qingfenghuohu/tools/redis"

type Real struct {
	dck []DataCacheKey
}

func (real *Real) SetCacheData(rcd []RealCacheData) {
	CacheData := map[string][]interface{}{}
	Keys := map[string]map[int64][]string{}
	for _, v := range rcd {
		if len(CacheData[v.CacheKey.ConfigName]) == 0 {
			CacheData[v.CacheKey.ConfigName] = []interface{}{}
			Keys[v.CacheKey.ConfigName] = map[int64][]string{}
		}
		if len(Keys[v.CacheKey.ConfigName][int64(v.CacheKey.LifeTime)]) == 0 {
			Keys[v.CacheKey.ConfigName][int64(v.CacheKey.LifeTime)] = []string{}
		}
		CacheData[v.CacheKey.ConfigName] = append(CacheData[v.CacheKey.ConfigName], v.CacheKey.String())
		CacheData[v.CacheKey.ConfigName] = append(CacheData[v.CacheKey.ConfigName], v.Result)
		Keys[v.CacheKey.ConfigName][int64(v.CacheKey.LifeTime)] = append(Keys[v.CacheKey.ConfigName][int64(v.CacheKey.LifeTime)], v.CacheKey.String())
	}
	for key, val := range CacheData {
		redis.GetInstance(key).MSet(val...)
		for k, v := range Keys[key] {
			redis.GetInstance(key).Expire(k, v...)
		}
	}
}

func (real *Real) GetCacheData(res *Result) {
	Keys := map[string][]string{}
	for _, v := range real.dck {
		if len(Keys[v.ConfigName]) == 0 {
			Keys[v.ConfigName] = []string{}
		}
		Keys[v.ConfigName] = append(Keys[v.ConfigName], v.String())
	}
	for k, v := range Keys {
		tmp := redis.GetInstance(k).MGet(v...)
		for key, val := range tmp {
			res.write(key, val)
		}
	}
}

func (real *Real) GetRealData() []RealCacheData {
	var result []RealCacheData
	return result
}

func (real *Real) SetDataCacheKey(dck []DataCacheKey) {
	real.dck = RemoveDuplicateElement(dck)
}
func (real *Real) DelCacheData() {
	keys := map[string][]interface{}{}
	for _, v := range real.dck {
		if len(keys[v.ConfigName]) == 0 {
			keys[v.ConfigName] = []interface{}{}
		}
		keys[v.ConfigName] = append(keys[v.ConfigName], v.String())
	}
	for key, val := range keys {
		redis.GetInstance(key).Delete(val...)
	}
}

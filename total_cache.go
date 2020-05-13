package data

import "github.com/qingfenghuohu/tools/redis"

type TotalReal struct {
	dck []DataCacheKey
}

func (real *TotalReal) SetCacheData(rcd []RealCacheData) {
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
func (real *TotalReal) GetCacheData(res *Result) {
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
func (real *TotalReal) GetRealData() []RealCacheData {
	var result []RealCacheData
	dataCacheKey := map[string]map[string][]DataCacheKey{}
	models := map[string]ModelInfo{}
	for _, v := range real.dck {
		TableName := v.Model.DbName() + "." + v.Model.TableName()
		if len(dataCacheKey[TableName]) == 0 {
			dataCacheKey[TableName] = map[string][]DataCacheKey{}
		}
		if len(dataCacheKey[TableName][v.Key]) == 0 {
			dataCacheKey[TableName][v.Key] = []DataCacheKey{}
		}
		dataCacheKey[TableName][v.Key] = append(dataCacheKey[TableName][v.Key], v)
		models[TableName] = v.Model
	}
	for key, val := range dataCacheKey {
		tmp := models[key].GetRealData(val)
		result = append(result, tmp...)
	}
	return result
}
func (real *TotalReal) SetDataCacheKey(dck []DataCacheKey) {
	real.dck = RemoveDuplicateElement(dck)
}
func (real *TotalReal) DelCacheData() {
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

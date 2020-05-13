package data

import (
	"github.com/qingfenghuohu/tools/redis"
	"strconv"
)

type RelReal struct {
	dck []DataCacheKey
}

func (real *RelReal) SetCacheData(rcd []RealCacheData) {
	Keys := map[string][]redis.HMSMD{}
	for _, v := range rcd {
		Hmsmd := redis.HMSMD{}
		if len(Keys[v.CacheKey.ConfigName]) == 0 {
			Keys[v.CacheKey.ConfigName] = []redis.HMSMD{}
		}
		Hmsmd.Key = real.GetCacheKey(v.CacheKey)
		Hmsmd.Data = map[string]interface{}{v.CacheKey.Params[1]: v.Result}
		Hmsmd.Ttl = v.CacheKey.LifeTime
		Keys[v.CacheKey.ConfigName] = append(Keys[v.CacheKey.ConfigName], Hmsmd)
	}
	for key, val := range Keys {
		redis.GetInstance(key).HMSetMulti(val)
	}
}
func (real *RelReal) GetCacheData(res *Result) {
	Keys := map[string]map[string][]string{}
	for _, v := range real.dck {
		if len(Keys[v.ConfigName]) == 0 {
			Keys[v.ConfigName] = map[string][]string{}
		}
		if len(Keys[v.ConfigName][real.GetCacheKey(v)]) == 0 {
			Keys[v.ConfigName][real.GetCacheKey(v)] = []string{}
		}
		if v.Params[1] != "" {
			Keys[v.ConfigName][real.GetCacheKey(v)] = append(Keys[v.ConfigName][real.GetCacheKey(v)], v.Params[1])
		}
	}
	for k, v := range Keys {
		tmp := redis.GetInstance(k).HMGetMulti(v)
		for key, val := range tmp {
			for kk, vv := range val {
				res.write(key+"_"+kk, vv)
			}
		}
	}
}
func (real *RelReal) GetRealData() []RealCacheData {
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
func (real *RelReal) SetDataCacheKey(dck []DataCacheKey) {
	real.dck = RemoveDuplicateElement(dck)
}
func (real *RelReal) DelCacheData() {
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
func (real *RelReal) GetCacheKey(key DataCacheKey) string {
	return strconv.Itoa(key.Version) + ":" +
		key.CType + ":" +
		key.Model.DbName() + "." + key.Model.TableName() + ":" +
		key.Key + ":" +
		key.Params[0]
}

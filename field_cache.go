package data

import (
	"github.com/qingfenghuohu/tools/redis"
	"strings"
)

type FieldReal struct {
	dck []DataCacheKey
}

func (real *FieldReal) SetCacheData(rcd []RealCacheData) {
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
func (real *FieldReal) GetCacheData(res *Result) {
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
func (real *FieldReal) GetRealData() []RealCacheData {
	var result []RealCacheData
	realData := make(map[string]map[string][]DataCacheKey)
	for _, v := range real.dck {
		tmpKey := v.Model.DbName() + "_" + v.Model.TableName()
		if ok := realData[tmpKey]; len(ok) <= 0 {
			realData[tmpKey] = map[string][]DataCacheKey{}
		}
		if ok := realData[tmpKey][v.Key]; len(ok) <= 0 {
			realData[tmpKey][v.Key] = []DataCacheKey{}
		}
		realData[tmpKey][v.Key] = append(realData[tmpKey][v.Key], v)
	}
	keys := []string{}
	var mm ModelInfo
	var filed string
	resData := make(map[string]interface{})
	for _, value := range realData {
		for _, val := range value {
			for _, v := range val {
				if v.Params[0] != "" {
					keys = append(keys, v.Params[0])
				}
				filed = v.RelField[0]
				mm = v.Model
			}
			mysqlFiled := Model(mm).InitField().fieldStruct[filed]
			if mysqlFiled != "" {
				param := "'" + strings.Join(keys, "','") + "'"
				res := Model(mm).Where(mysqlFiled + " in(" + param + ")").Select()
				for _, kv := range res {
					kk := kv[filed].(string)
					resData[kk] = kv
				}
			}
			for _, v := range val {
				ok := resData[v.Params[0]]
				if ok == nil {
					result = append(result, RealCacheData{CacheKey: v, Result: []map[string]interface{}{}})
				} else {
					result = append(result, RealCacheData{CacheKey: v, Result: ok})
				}
			}
		}
	}
	return result
}
func (real *FieldReal) SetDataCacheKey(dck []DataCacheKey) {
	real.dck = RemoveDuplicateElement(dck)
}
func (real *FieldReal) DelCacheData() {
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

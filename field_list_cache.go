package data

import (
	"github.com/qingfenghuohu/tools/redis"
	"strings"
)

type FieldListReal struct {
	dck []DataCacheKey
}

func (real *FieldListReal) SetCacheData(rcd []RealCacheData) {
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
func (real *FieldListReal) GetCacheData(res *Result) {
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
func (real *FieldListReal) GetRealData() []RealCacheData {
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

	for _, value := range realData {
		for _, val := range value {
			for _, v := range val {
				Condition := []string{}
				WhereVal := []interface{}{}
				for i, p := range v.Params {
					if p != "" {
						WhereVal = append(WhereVal, p)
						Condition = append(Condition, v.RelField[i]+" = ? ")
					}
				}
				WhereCondition := strings.Join(Condition, " and ")
				if len(Condition) > 0 && len(WhereVal) > 0 {
					tmp := Model(v.Model).Where(WhereCondition, WhereVal...).Select()
					result = append(result, RealCacheData{CacheKey: v, Result: tmp})
				} else {
					result = append(result, RealCacheData{CacheKey: v, Result: nil})
				}
			}
		}
	}
	return result
}
func (real *FieldListReal) SetDataCacheKey(dck []DataCacheKey) {
	real.dck = dck
}
func (real *FieldListReal) DelCacheData() {
}

package data

import (
	"strings"
)

type RelReal struct {
	ready    bool
	cacheKey DataCacheKey
	params   []interface{}
}

func RelRealData(resetKey []DataCacheKey, dbData []RealCacheData) []RealCacheData {
	real := make(map[string]map[string][]DataCacheKey)
	for _, v := range resetKey {
		tmpKey := v.Model.DbName() + "_" + v.Model.TableName()
		if ok := real[tmpKey]; len(ok) <= 0 {
			real[tmpKey] = map[string][]DataCacheKey{}
		}
		if ok := real[tmpKey][v.Key]; len(ok) <= 0 {
			real[tmpKey][v.Key] = []DataCacheKey{}
		}
		real[tmpKey][v.Key] = append(real[tmpKey][v.Key], v)
	}
	keys := []string{}
	var mm ModelInfo
	var filed string
	resData := make(map[string]interface{})
	for _, value := range real {

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
					dbData = append(dbData, RealCacheData{CacheKey: v, Result: map[string]interface{}{}})
				} else {
					dbData = append(dbData, RealCacheData{CacheKey: v, Result: ok})
				}

			}
		}
	}
	return dbData
}

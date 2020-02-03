package data

import (
	"strings"
)

type IdReal struct {
	ready    bool
	cacheKey DataCacheKey
	params   []interface{}
}

func IdRealData(resetKey []DataCacheKey, dbData []RealCacheData) []RealCacheData {
	real := make(map[string]DataCacheKey)
	realParams := make(map[string][]string)
	for _, v := range resetKey {
		tmpKey := v.Model.DbName() + "_" + v.Model.TableName()
		if ok := real[tmpKey]; ok.CType == "" {
			real[tmpKey] = v
			realParams[tmpKey] = []string{}
		}
		if v.Params[0] != "" {
			realParams[tmpKey] = append(realParams[tmpKey], v.Params[0])
		}
	}
	for i, v := range real {
		param := strings.Join(realParams[i], ",")
		m := Model(v.Model).InitField()
		if m.pk == "" {
			m.pk = "Id"
		}
		if m.pkMysql == "" {
			m.pkMysql = "id"
		}
		res := Model(v.Model).Where(m.pkMysql + " in(" + param + ")").Select()
		for _, vv := range res {
			v.Params = []string{vv[m.pk].(string)}
			dbData = append(dbData, RealCacheData{CacheKey: v, Result: vv})
		}
	}
	return dbData
}

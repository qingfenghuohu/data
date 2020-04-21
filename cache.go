package data

import (
	"encoding/json"
	"github.com/qingfenghuohu/tools/redis"
	"strconv"
	"strings"
)

const (
	DataCacheTypeIds      = "id"
	DataCacheTypeRelation = "rel"
	DataCacheTypeI        = "i"
	DataCacheTypeIs       = "is"
	DataCacheTypeTotal    = "tot"
	DataCacheTypeList     = "list"
)

type DataCacheKey struct {
	Key        string
	CType      string
	Model      ModelInfo
	Params     []string
	LifeTime   int
	ResetTime  int
	ResetCount int
	Version    int
	RelField   []string
	ResetType  int //0=重建,1=删除
	ConfigName string
	Data       interface{}
}

type CacheKey struct {
	Config map[int]DataCacheKey
}

type RealCacheData struct {
	Result   interface{}
	CacheKey DataCacheKey
}
type CacheTime struct {
	time   int64
	keys   []string
	config string
}

func ReBuild(resetKey map[string][]DataCacheKey, result map[string]interface{}) {
	dbData := map[string][]RealCacheData{}
	//获取真实数据
	for cacheType, confVal := range resetKey {
		if ok := dbData[cacheType]; len(ok) == 0 {
			dbData[cacheType] = []RealCacheData{}
		}
		switch cacheType {
		case DataCacheTypeIds:
			dbData[cacheType] = IdRealData(confVal, dbData[cacheType])
			break
		case DataCacheTypeRelation:
			dbData[cacheType] = RelRealData(confVal, dbData[cacheType])
			break
		case DataCacheTypeI:
			dbData[cacheType] = IRealData(confVal, dbData[cacheType])
			break
		case DataCacheTypeIs:
			break
		case DataCacheTypeList:
			break
		case DataCacheTypeTotal:
			break
		}
	}
	//设置缓存
	SetCacheData(dbData)
	//拼装真实数据及缓存数据
	for _, v := range dbData {
		for _, vv := range v {
			result[CreateCacheKeyStr(vv.CacheKey)] = vv.Result
		}
	}
	//fmt.Println(result)

}
func getCache(realKey *map[string]map[string]DataCacheKey) map[string]interface{} {
	result := make(map[string]interface{})
	getCacheConfig := make(map[string]DataCacheKey)
	for cacheType, confVal := range *realKey {
		switch cacheType {
		default:
			for k, v := range confVal {
				getCacheConfig[k] = v
			}
			break
		}
	}
	//fmt.Println("getCacheConfig", getCacheConfig)
	GetCacheData(getCacheConfig, result)

	return result
}
func GetCacheData(confValue map[string]DataCacheKey, result map[string]interface{}) {
	cacheKey := make(map[string][]string)
	for i, v := range confValue {
		if ok := cacheKey[v.ConfigName]; ok == nil {
			cacheKey[v.ConfigName] = []string{}
		}
		cacheKey[v.ConfigName] = append(cacheKey[v.ConfigName], i)
	}

	for i, v := range cacheKey {
		cacheRes := redis.GetInstance(i).MGet(v...)
		if len(cacheRes) > 0 {
			for ii, vv := range cacheRes {
				if vv != nil {
					m := make(map[string]interface{})
					b := []byte(vv.(string))
					err := json.Unmarshal(b, &m)
					if err == nil {
						result[ii] = m
					} else {
						mm := []map[string]interface{}{}
						err = json.Unmarshal(b, &mm)
						if err == nil {
							result[ii] = mm
						}
					}
				}
			}
		}
	}
}
func SetCacheData(dbData map[string][]RealCacheData) {
	data := make(map[string][]interface{})
	ct := map[string][]string{}
	setData := []RealCacheData{}
	for key, val := range dbData {
		switch key {
		//case DataCacheTypeIds:
		//	break
		default:
			setData = append(setData, val...)
			break
		}
	}
	for _, v := range setData {
		if ok := data[v.CacheKey.ConfigName]; len(ok) == 1 {
			data[v.CacheKey.ConfigName] = []interface{}{}
		}
		kk := v.CacheKey.ConfigName + "|-|" + strconv.Itoa(v.CacheKey.LifeTime)
		if ok := ct[kk]; len(ok) <= 0 {
			ct[kk] = []string{}
		}
		ct[kk] = append(ct[kk], CreateCacheKeyStr(v.CacheKey))
		data[v.CacheKey.ConfigName] = append(data[v.CacheKey.ConfigName], CreateCacheKeyStr(v.CacheKey))
		data[v.CacheKey.ConfigName] = append(data[v.CacheKey.ConfigName], v.Result)
	}

	for i, v := range data {
		redis.GetInstance(i).MSet(v...)
		//fmt.Println("SetCacheData", flag)

	}

	for i, v := range ct {
		redisConfig := strings.Split(i, "|-|")
		time, _ := strconv.ParseInt(redisConfig[1], 10, 64)
		host := redisConfig[0]
		redis.GetInstance(host).Expire(time, v...)
	}
}
func DelCacheData(data map[string][]DataCacheKey) {
	result := make(map[string][]interface{})
	for k, v := range data {
		switch k {
		default:
			for _, vv := range v {
				if ok := result[vv.ConfigName]; len(ok) == 0 {
					result[vv.ConfigName] = []interface{}{}
				}
				result[vv.ConfigName] = append(result[vv.ConfigName], CreateCacheKeyStr(vv))
			}
			for i, v := range result {
				redis.GetInstance(i).Delete(v...)
			}
			break
		}
	}

}
func CreateDataCacheKey(m ModelInfo, key string, p ...string) DataCacheKey {
	result := Model(m).modelInfo.GetDataCacheKey()[key]
	result.Params = p
	return result
}

func GetData(configKey *[]DataCacheKey) map[string]interface{} {
	realKey := AnalysisCacheKey(configKey)
	//获取全部缓存数据
	AllData := getCache(&realKey)
	//获取需要重建key
	resetKey := make(map[string][]DataCacheKey)
	for _, v := range *configKey {
		tmpKey := CreateCacheKeyStr(v)
		if ok := AllData[tmpKey]; ok == nil {
			if ok2 := resetKey[v.CType]; ok2 == nil {
				resetKey[v.CType] = []DataCacheKey{}
			}
			resetKey[v.CType] = append(resetKey[v.CType], v)
		}
	}
	//重置缓存数据
	if len(resetKey) > 0 {
		ReBuild(resetKey, AllData)
	}
	//fmt.Println("AllData", AllData)
	return AllData
}

func AnalysisCacheKey(configKey *[]DataCacheKey) map[string]map[string]DataCacheKey {
	realKey := make(map[string]map[string]DataCacheKey)
	for _, v := range *configKey {
		if v.Model != nil {
			tmpKey := CreateCacheKeyStr(v)
			if ok := realKey[v.CType]; len(ok) == 0 {
				realKey[v.CType] = make(map[string]DataCacheKey)
			}
			realKey[v.CType][tmpKey] = v
		}
	}
	return realKey
}

func CreateCacheKeyStr(dck DataCacheKey) string {
	tmpParam := strings.Join(dck.Params, "_")

	result := []string{}
	result = append(result, strconv.Itoa(dck.Version))
	result = append(result, dck.CType)
	result = append(result, dck.Model.DbName()+"."+dck.Model.TableName())
	result = append(result, dck.Key)

	result = append(result, tmpParam)
	return strings.Join(result, ":")
}

func SaveCache(beData []map[string]interface{}, Data []map[string]interface{}, m ModelInfo) {
	saveCacheData := make(map[string][]RealCacheData)
	delCacheData := make(map[string][]DataCacheKey)
	tmpCache := m.DbToCache(Data, beData)
	for _, v := range tmpCache {
		if v.CacheKey.ResetType == 0 {
			saveCacheData["default"] = append(saveCacheData["default"], v)
		}
		if v.CacheKey.ResetType == 1 {
			delCacheData["default"] = append(delCacheData["default"], v.CacheKey)
		}
	}

	field := Model(m).InitField().pk
	dataCacheKey := m.GetDataCacheKey()
	dataCacheKeyList := make(map[string][]DataCacheKey)
	//fmt.Println("data.cache",dataCacheKey)
	for _, v := range Data {
		for confType, confVal := range dataCacheKey {
			if ok := dataCacheKeyList[confType]; len(ok) == 0 {
				dataCacheKeyList[confType] = []DataCacheKey{}
			}
			switch confVal.CType {
			case DataCacheTypeIds:
				if ok2 := dataCacheKeyList[confType]; len(ok2) == 0 {
					dataCacheKeyList[confType] = []DataCacheKey{}
				}
				confVal.Params = []string{v[field].(string)}
				if confVal.ResetType == 1 {
					saveCacheData[DataCacheTypeIds] = append(saveCacheData[DataCacheTypeIds], RealCacheData{Result: v, CacheKey: confVal})
				}
				if confVal.ResetType == 0 {
					delCacheData[DataCacheTypeIds] = append(delCacheData[DataCacheTypeIds], confVal)
				}
				break
			case DataCacheTypeRelation:
				f := confVal.RelField[0]
				confVal.Params = []string{v[f].(string)}
				if confVal.ResetType == 1 {
					saveCacheData[DataCacheTypeRelation] = append(saveCacheData[DataCacheTypeRelation], RealCacheData{Result: v, CacheKey: confVal})
				}
				if confVal.ResetType == 0 {
					delCacheData[DataCacheTypeRelation] = append(delCacheData[DataCacheTypeRelation], confVal)
				}
				break
			case DataCacheTypeI:
				if confVal.ResetType == 1 {
					saveCacheData[DataCacheTypeI] = m.DbToCache(Data, beData)
				}
				if confVal.ResetType == 0 {
					delCacheData[DataCacheTypeI] = m.DbToCacheKey(Data, beData)
				}
				break
			}
		}
	}
	SetCacheData(saveCacheData)
	DelCacheData(delCacheData)
}
func DelCache(beData []map[string]interface{}, m ModelInfo) {
	dataCacheKeyList := createDataCacheKeyList(beData, m)
	DelCacheData(dataCacheKeyList)
}

func createDataCacheKeyList(data []map[string]interface{}, m ModelInfo) map[string][]DataCacheKey {
	dataCacheKeyList := make(map[string][]DataCacheKey)
	dataCacheKey := m.GetDataCacheKey()
	field := Model(m).InitField().pk
	for _, v := range data {
		for confType, confVal := range dataCacheKey {
			if ok := dataCacheKeyList[confType]; len(ok) == 0 {
				dataCacheKeyList[confType] = []DataCacheKey{}
			}
			switch confVal.CType {
			case DataCacheTypeIds:
				if ok2 := dataCacheKeyList[confType]; len(ok2) == 0 {
					dataCacheKeyList[confType] = []DataCacheKey{}
				}
				confVal.Params = []string{v[field].(string)}
				dataCacheKeyList[confType] = append(dataCacheKeyList[confType], confVal)
				break
			case DataCacheTypeRelation:
				break
			case DataCacheTypeI:
				break
			}

		}
	}
	return dataCacheKeyList
}

func FormatIDCackey(dataCacheKey map[string][]DataCacheKey) (map[string]DataCacheKey, map[string][]interface{}, map[string]interface{}) {
	realKey := make(map[string]DataCacheKey)
	realKeyParams := make(map[string][]interface{})
	result := map[string]interface{}{}
	for _, value := range dataCacheKey {
		realKey[value[0].Key] = value[0]
		for _, v := range value {
			if ok := realKeyParams[v.Key]; len(ok) == 0 {
				realKeyParams[v.Key] = []interface{}{}
			}
			realKeyParams[v.Key] = append(realKeyParams[v.Key], v.Params)
			result[CreateCacheKeyStr(v)] = nil
		}
	}
	return realKey, realKeyParams, result
}

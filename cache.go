package data

import (
	"strconv"
	"strings"
	"sync"
)

const (
	CacheTypeIds       = "id"
	CacheTypeRelation  = "rel"
	CacheTypeI         = "i"
	CacheTypeField     = "f"
	CacheTypeTotal     = "t"
	CacheTypeFieldList = "fl"
)

type Cache interface {
	SetCacheData(rcd []RealCacheData)
	GetCacheData(res *Result)
	GetRealData() []RealCacheData
	SetDataCacheKey(dck []DataCacheKey)
	DelCacheData()
}

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

type ListDataCacheKey struct {
	data []DataCacheKey
}

type RealCacheData struct {
	Result   interface{}
	CacheKey DataCacheKey
}
type RealData struct {
	Data      []RealCacheData
	Sync      sync.Mutex
	WaitGroup sync.WaitGroup
}

func ReBuild(resetKey []DataCacheKey) []RealCacheData {
	var result RealData
	typeResetKey := GetTypeDataCacheKey(resetKey)
	//获取真实数据
	for key, val := range typeResetKey {
		result.Add()
		go func() {
			rc := RunCache(key)
			rc.SetDataCacheKey(val)
			RealCacheData := rc.GetRealData()
			rc.SetCacheData(RealCacheData)
			result.append(RealCacheData...)
		}()
	}
	result.Wait()
	return result.Data
}

func getCache(configKey []DataCacheKey) (Result, []DataCacheKey) {
	var result Result
	var defect []DataCacheKey
	dataCacheKey := GetTypeDataCacheKey(configKey)
	for key, val := range dataCacheKey {
		result.Add()
		go func() {
			rc := RunCache(key)
			rc.SetDataCacheKey(val)
			rc.GetCacheData(&result)
			result.Done()
		}()
	}
	result.Wait()
	for _, v := range configKey {
		if result.read(v.String()) == nil {
			defect = append(defect, v)
		}
	}
	return result, defect
}

func RunCache(key string) Cache {
	var result Cache
	switch key {
	case CacheTypeFieldList:
		result = &FieldListReal{}
		break
	case CacheTypeRelation:
		result = &RelReal{}
		break
	case CacheTypeIds:
		result = &IdReal{}
		break
	case CacheTypeField:
		result = &FieldReal{}
		break
	case CacheTypeTotal:
		break
	case CacheTypeI:
		result = &IReal{}
		break
	default:
		result = &Real{}
		break
	}
	return result
}

func CreateDataCacheKey(m ModelInfo, key string, p ...string) DataCacheKey {
	result := Model(m).modelInfo.GetDataCacheKey()[key]
	result.Params = p
	return result
}

func GetData(configKey []DataCacheKey) map[string]interface{} {
	configKey = RemoveDuplicateElement(configKey)
	//获取全部缓存数据
	AllData, resetKey := getCache(configKey)
	//重置缓存数据
	if len(resetKey) > 0 {
		RealCacheData := ReBuild(resetKey)
		for _, v := range RealCacheData {
			AllData.write(v.CacheKey.String(), v.Result)
		}
	}
	return AllData.Map()
}

func CreateCacheKeyStr(dck DataCacheKey) string {
	return strconv.Itoa(dck.Version) + ":" +
		dck.CType + ":" +
		dck.Model.DbName() + "." + dck.Model.TableName() + ":" +
		dck.Key + ":" +
		strings.Join(dck.Params, "_")
}

func SaveCache(beData []map[string]interface{}, Data []map[string]interface{}, m ModelInfo) {
	var dataCacheKey []DataCacheKey
	var saveDataCacheKey []DataCacheKey
	var delDataCacheKey []DataCacheKey
	var tmp []DataCacheKey
	tmp = DbDataToCacheKey(Data, beData, m)
	dataCacheKey = append(dataCacheKey, tmp...)

	for _, v := range dataCacheKey {
		if v.ResetType == 1 {
			saveDataCacheKey = append(saveDataCacheKey, v)
		}
		if v.ResetType == 0 {
			delDataCacheKey = append(delDataCacheKey, v)
		}
	}
	ReBuild(saveDataCacheKey)

	typeDataCacheKey := GetTypeDataCacheKey(delDataCacheKey)
	for key, val := range typeDataCacheKey {
		rc := RunCache(key)
		rc.SetDataCacheKey(val)
		rc.DelCacheData()
	}
}

func DelCache(Data []map[string]interface{}, m ModelInfo) {
	var dataCacheKey []DataCacheKey
	tmp := DbDataToCacheKey(Data, m)
	dataCacheKey = append(dataCacheKey, tmp...)
	dataCacheKey = RemoveDuplicateElement(dataCacheKey)
	typeDataCacheKey := GetTypeDataCacheKey(dataCacheKey)
	for key, val := range typeDataCacheKey {
		rc := RunCache(key)
		rc.SetDataCacheKey(val)
		rc.DelCacheData()
	}
}

func DbDataToCacheKey(Data []map[string]interface{}, beData []map[string]interface{}, m ModelInfo) []DataCacheKey {
	var result []DataCacheKey
	field := Model(m).InitField().pk
	dataCacheKey := m.GetDataCacheKey()
	for _, v := range Data {
		for _, confVal := range dataCacheKey {
			switch confVal.CType {
			case CacheTypeIds:
				confVal.Params = []string{v[field].(string)}
				result = append(result, confVal)
				break
			case CacheTypeRelation:
				tmp := []string{}
				for _, val := range confVal.RelField {
					if v[val].(string) != "" {
						tmp = append(tmp, v[val].(string))
					}
				}
				confVal.Params = tmp
				result = append(result, confVal)
				break
			case CacheTypeList:
				tmp := []string{}
				for _, val := range confVal.RelField {
					if v[val].(string) != "" {
						tmp = append(tmp, v[val].(string))
					}
				}
				confVal.Params = tmp
				result = append(result, confVal)
				break
			default:
				tmp1 := m.DbToCacheKey(Data, beData)
				result = append(result, tmp1...)
				break
			}

		}
	}
	return result
}

func (dck *DataCacheKey) String() string {
	return strconv.Itoa(dck.Version) + ":" +
		dck.CType + ":" +
		dck.Model.DbName() + "." + dck.Model.TableName() + ":" +
		dck.Key + ":" +
		strings.Join(dck.Params, "_")
}

func (rd *RealData) append(data ...RealCacheData) {
	rd.Sync.Lock()
	rd.Data = append(rd.Data, data...)
	rd.Sync.Unlock()
}
func (rd *RealData) Add() {
	rd.WaitGroup.Add(1)
}

func (rd *RealData) Done() {
	rd.WaitGroup.Done()
}

func (rd *RealData) Wait() {
	rd.WaitGroup.Wait()
}

func Run() *ListDataCacheKey {
	result := ListDataCacheKey{}
	return &result
}

func (ld *ListDataCacheKey) Add(model ModelInfo, key string, params ...string) *ListDataCacheKey {
	ld.data = append(ld.data, CreateDataCacheKey(model, key, params...))
	return ld
}

func (ld *ListDataCacheKey) GetData() map[string]interface{} {
	return GetData(ld.data)
}

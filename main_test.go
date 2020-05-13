package data

import (
	"testing"
)

func TestModel(t *testing.T) {
	Product := Product{}
	//var config []DataCacheKey
	//config = append(config,CreateDataCacheKey(&Product, ProductModelDataCacheKeyState, "1"))
	//res := tools.Interface2MapSliceStr(GetData(&config))
	//fmt.Println(res)
	//Product.Content = strconv.Itoa(tools.MtRand(1000,9999))
	//flag := Model(&Product).Where("id = 1").Save(&Product)
	//fmt.Println(flag)
	Run().
		Add(&Product, ProductModelDataCacheKeyState, "1").
		GetData()
}

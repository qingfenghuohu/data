package data

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jinzhu/gorm"
	"github.com/json-iterator/go"
	_ "github.com/mattn/go-sqlite3"
	"github.com/qingfenghuohu/config"
	"github.com/qingfenghuohu/tools"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

type ModelInfo interface {
	TableName() string
	DbName() string
	GetRealData(dataCacheKey map[string][]DataCacheKey) []RealCacheData
	GetDataCacheKey() map[string]DataCacheKey
	DbToCache(dbData []map[string]interface{}, beData []map[string]interface{}, operation string) []RealCacheData
	DbToCacheKey(dbData []map[string]interface{}, beData []map[string]interface{}, operation string) []DataCacheKey
}

type m struct {
	fieldMysql  map[string]string
	fieldStruct map[string]string
	field       string
	pk          string
	pkMysql     string
	modelInfo   ModelInfo
	connM       *gorm.DB
	connS       map[int]*gorm.DB
	where       whereConfig
	set         setConfig
	orderBy     string
	groupBy     string
	page        pageConfig
	limit       int
}
type NResult struct {
	N int //or int ,or some else
}
type AvgResult struct {
	N float64 //or int ,or some else
}
type pageConfig struct {
	page int
	size int
}
type setConfig struct {
	c string
	p interface{}
}
type whereConfig struct {
	w string
	p []interface{}
}

func Model(model ModelInfo) *m {
	var connM *gorm.DB
	var connS map[int]*gorm.DB
	var where whereConfig
	var set setConfig
	var page pageConfig

	m := m{map[string]string{}, map[string]string{}, "", "", "", model, connM, connS, where, set, "", "", page, 0}
	m.connS = make(map[int]*gorm.DB)
	return &m
}

func (m *m) InitField() *m {
	if m.field == "" {
		modelType := reflect.TypeOf(m.modelInfo).Elem()
		for i := 0; i < modelType.NumField(); i++ {
			var tag = modelType.Field(i).Tag.Get("gorm")
			var col = gormTag(tag)
			if strings.Index(tag, "PRIMARY_KEY") != -1 {
				m.pk = modelType.Field(i).Name
				if col["column"] != "" {
					m.pkMysql = col["column"]
				} else {
					m.pkMysql = strings.ToLower(modelType.Field(i).Name)
				}
			}
			if col["column"] != "" {
				m.fieldStruct[modelType.Field(i).Name] = col["column"]
				m.fieldMysql[col["column"]] = modelType.Field(i).Name
				if m.field == "" {
					m.field += col["column"]
				} else {
					m.field += "," + col["column"]
				}
			} else {
				m.fieldStruct[modelType.Field(i).Name] = strings.ToLower(modelType.Field(i).Name)
				m.fieldMysql[strings.ToLower(modelType.Field(i).Name)] = modelType.Field(i).Name
				if m.field == "" {
					m.field += m.fieldMysql[strings.ToLower(modelType.Field(i).Name)]
				} else {
					m.field += "," + m.fieldMysql[strings.ToLower(modelType.Field(i).Name)]
				}
			}
			m.field += " as " + modelType.Field(i).Name
		}
	}
	return m
}

var connM map[string]*gorm.DB
var connS map[string][]*gorm.DB

var once sync.Once

func init() {

	for dbName, v := range config.Data["database"].(map[string]interface{}) {
		masterData := v.(map[string]interface{})["master"].(map[string]interface{})
		slaveData := v.(map[string]interface{})["slave"].(map[string]interface{})
		if len(connM) <= 0 {
			connM = map[string]*gorm.DB{}
			connS = map[string][]*gorm.DB{}
		}
		if len(connS[dbName]) <= 0 {
			connS[dbName] = []*gorm.DB{}
		}
		idleNum, _ := strconv.Atoi(masterData["idleNum"].(string))
		openNum, _ := strconv.Atoi(masterData["openNum"].(string))
		connM[dbName], _ = gorm.Open(masterData["type"].(string), masterData["url"].(string))

		if idleNum > 0 && openNum > 0 {
			connM[dbName].DB().SetMaxIdleConns(idleNum)
			connM[dbName].DB().SetMaxOpenConns(openNum)
		}
		if masterData["debug"].(bool) {
			connM[dbName].LogMode(true)
		}
		for i, v := range slaveData["url"].([]interface{}) {
			idleNum, _ := strconv.Atoi(slaveData["idleNum"].(string))
			openNum, _ := strconv.Atoi(slaveData["idleNum"].(string))
			if len(connS[dbName]) < len(slaveData["url"].([]interface{})) {
				tmp, _ := gorm.Open(slaveData["type"].(string), v.(string))
				connS[dbName] = append(connS[dbName], tmp)
			}
			if idleNum > 0 && openNum > 0 {
				connS[dbName][i].DB().SetMaxIdleConns(idleNum)
				connS[dbName][i].DB().SetMaxOpenConns(openNum)
			}
			if slaveData["debug"].(bool) {
				connS[dbName][i].LogMode(true)
			}
		}
		//m.connM.LogMode(true)
	}
	fmt.Println("db库计数打印")
}

func (m *m) connMaster() *gorm.DB {
	dbName := m.modelInfo.DbName()
	m.InitField()
	return connM[dbName]
}
func (m *m) connSlave() *gorm.DB {
	dbName := m.modelInfo.DbName()
	m.InitField()
	totalNum := len(connS[dbName])
	var dbNum int
	if totalNum > 1 {
		tools.MtRand(totalNum-1, totalNum)
	}
	return connS[dbName][dbNum]
}
func (m *m) Where(sql string, params ...interface{}) *m {
	m.where.w = sql
	m.where.p = params
	return m
}
func (m *m) OrderBy(sql string) *m {
	m.orderBy = sql
	return m
}
func (m *m) GroupBy(sql string) *m {
	m.groupBy = sql
	return m
}
func (m *m) Limit(num int) *m {
	m.limit = num
	return m
}
func (m *m) Page(page int, size int) *m {
	m.page.page = page
	m.page.size = size
	return m
}
func (m *m) Field(field string) *m {
	m.field = field
	return m
}
func (m *m) clear() {
	m.where = whereConfig{}
	m.set = setConfig{}
	m.page = pageConfig{}
	m.limit = 0
	m.groupBy = ""
	m.orderBy = ""
}
func (m *m) Add(data interface{}) int {
	m.connMaster().Model(m.modelInfo).Create(data)
	var result []map[string]interface{}
	t := reflect.TypeOf(data).Elem()
	v := reflect.ValueOf(data).Elem()
	var id int
	var mData = make(map[string]interface{})
	for i := 0; i < t.NumField(); i++ {
		if typeof(v.Field(i).Interface()) == "int" {
			mData[t.Field(i).Name] = strconv.Itoa(v.Field(i).Interface().(int))
		} else {
			mData[t.Field(i).Name] = v.Field(i).Interface()
		}
	}
	id, _ = strconv.Atoi(mData[m.pk].(string))
	if id > 0 {
		result = append(result, mData)
		SaveCache([]map[string]interface{}{}, result, m.modelInfo)
	}
	return id
}
func (m *m) Save(data interface{}) bool {
	if m.where.w == "" {
		panic("条件不能为空")
	}
	tmpWhere := m.where
	beData := m.Select()
	reData := []map[string]interface{}{}
	m.where = tmpWhere
	result := m.connMaster().Model(m.modelInfo).Where(m.where.w, m.where.p...).Updates(data).RowsAffected
	m.clear()
	if result > 0 {
		mData := Struct2Map(data)
		for _, v := range beData {
			tmpData := make(map[string]interface{})
			for ii, vv := range v {
				if ok := mData[ii]; ok != "" && m.pk != ii {
					tmpData[ii] = ok
				} else {
					tmpData[ii] = vv.(string)
				}
			}
			reData = append(reData, tmpData)
		}
		SaveCache(beData, reData, m.modelInfo)
		return true
	} else {
		return false
	}
}
func (m *m) IncrSets(field string, number int, field2 string, number2 int) bool {
	if m.where.w == "" {
		panic("条件不能为空")
	}
	tmpWhere := m.where
	beData := m.Select()
	reData := []map[string]interface{}{}
	m.where = tmpWhere

	result := m.connMaster().Model(m.modelInfo).Where(m.where.w, m.where.p...).UpdateColumn(field, gorm.Expr(field+" + ?", number), field2, gorm.Expr(field2+" + ?", number2)).RowsAffected
	m.clear()

	if result > 0 {
		for _, v := range beData {
			tmpData := make(map[string]interface{})
			for ii, vv := range v {
				if ii == field {
					tmpData[ii] = strconv.Itoa(vv.(int) + number)
				} else if ii == field2 {
					tmpData[ii] = strconv.Itoa(vv.(int) + number2)
				} else {
					tmpData[ii] = vv.(string)
				}
			}
			reData = append(reData, tmpData)
		}
		SaveCache(beData, reData, m.modelInfo)
		return true
	} else {
		return false
	}
}

func (m *m) DecrSet(field string, number int) bool {
	if m.where.w == "" {
		panic("条件不能为空")
	}
	tmpWhere := m.where
	beData := m.Select()
	reData := []map[string]interface{}{}
	m.where = tmpWhere
	result := m.connMaster().Model(m.modelInfo).Where(m.where.w, m.where.p...).UpdateColumn(field, gorm.Expr(field+" - ?", number)).RowsAffected
	m.clear()
	if result > 0 {
		for _, v := range beData {
			tmpData := make(map[string]interface{})
			for ii, vv := range v {
				if ii == field {
					tmpData[ii] = strconv.Itoa(vv.(int) - number)
				} else {
					tmpData[ii] = vv.(string)
				}
			}
			reData = append(reData, tmpData)
		}
		SaveCache(beData, reData, m.modelInfo)
		return true
	} else {
		return false
	}
}
func (m *m) Del() bool {
	tmpWhere := m.where
	beData := m.Select()
	m.where = tmpWhere
	result := m.connMaster().Where(m.where.w, m.where.p).Delete(m.modelInfo).RowsAffected
	m.clear()
	if result > 0 {
		DelCache(beData, m.modelInfo)
		return true
	} else {
		return false
	}
}
func (m *m) AddMulit() bool {
	m.clear()
	return true
}
func (m *m) Select() []map[string]interface{} {
	tx := m.connSlave()
	tx = m.whereExec(tx)
	m.InitField()
	rows, err := tx.Select(m.field).Rows()
	if err != nil {
		fmt.Println(m.modelInfo.DbName() + "." + m.modelInfo.TableName())
		panic(err.Error())
	}
	columns, err := rows.Columns()
	if err != nil {
		panic(err.Error())
	}
	values := make([]sql.RawBytes, len(columns))

	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	result := []map[string]interface{}{}
	for rows.Next() {
		tmpData := make(map[string]interface{})
		err = rows.Scan(scanArgs...)
		if err != nil {
			panic(err.Error())
		}
		var value string
		for i, col := range values {
			if col == nil {
				value = "NULL"
			} else {
				value = string(col)
			}
			tmpData[columns[i]] = value
		}
		result = append(result, tmpData)
	}
	m.clear()
	return result
}
func (m *m) Count() int {
	var value int
	tx := m.connMaster().Model(m.modelInfo)
	tx = m.whereExec(tx)
	tx.Count(&value)
	m.clear()
	return value
}
func (m *m) Sum(value string) int {
	var n NResult
	tx := m.connSlave().Model(m.modelInfo)
	tx = m.whereExec(tx)
	tx.Select("sum(" + value + ") as n").Scan(&n)
	m.clear()
	return n.N
}
func (m *m) Max(value string) int {
	var n NResult
	tx := m.connSlave().Model(m.modelInfo)
	tx = m.whereExec(tx)
	tx.Select("max(" + value + ") as n").Scan(&n)
	m.clear()
	return n.N
}
func (m *m) Min(value string) int {
	var n NResult
	tx := m.connSlave().Model(m.modelInfo)
	tx = m.whereExec(tx)
	tx.Select("min(" + value + ") as n").Scan(&n)
	m.clear()
	return n.N
}
func (m *m) Avg(value string) float64 {
	var n AvgResult
	tx := m.connSlave().Model(m.modelInfo)
	tx = m.whereExec(tx)
	tx.Select("avg(" + value + ") as n").Scan(&n)
	m.clear()
	return n.N
}
func (m *m) whereExec(tx *gorm.DB) *gorm.DB {
	tx = tx.Table(m.modelInfo.TableName())
	if m.where.w != "" {
		tx = tx.Where(m.where.w, m.where.p...)
	}
	if m.groupBy != "" {
		tx = tx.Group(m.groupBy)
	}
	if m.orderBy != "" {
		tx = tx.Order(m.orderBy)
	}
	if m.page.page > 0 {
		start := (m.page.page - 1) * m.page.size
		fmt.Println("m.page", m.page, "start", start)
		tx = tx.Offset(start)
		tx = tx.Limit(m.page.size)
	}
	if m.limit > 0 {
		tx = tx.Limit(m.page.size)
	}
	return tx
}
func (m *m) DropColumn(column string) {
	m.connMaster().Model(m.modelInfo).DropColumn(column)
}
func (m *m) ModifyColumn(column string, typ string) {
	m.connMaster().Model(m.modelInfo).ModifyColumn(column, typ)
}
func (m *m) AddForeignKey(field string, dest string, onDelete string, onUpdate string) {
	m.connMaster().Model(m.modelInfo).AddForeignKey(field, dest, onDelete, onUpdate)
}
func (m *m) AddIndex(indexName string, columns ...string) {
	m.connMaster().Model(m.modelInfo).AddIndex(indexName, columns...)
}
func (m *m) AddUniqueIndex(indexName string, columns ...string) {
	m.connMaster().Model(m.modelInfo).AddUniqueIndex(indexName, columns...)
}
func (m *m) RemoveForeignKey(field string, dest string) {
	m.connMaster().Model(m.modelInfo).RemoveForeignKey(field, dest)
}
func (m *m) HasTable(value interface{}) bool {
	return m.connMaster().Model(m.modelInfo).HasTable(value)
}
func (m *m) CreateTable(models ...interface{}) {
	m.connMaster().Model(m.modelInfo).CreateTable(models...)
}
func (m *m) Set(name string, value interface{}) *m {
	m.connMaster().Model(m.modelInfo).Set(name, value)
	return m
}
func (m *m) Query(sql string) interface{} {
	var result interface{}
	return result
}
func (m *m) Exec(sql string) interface{} {
	var result interface{}
	return result
}

func gormTag(data string) map[string]string {
	reData := strings.Split(data, ";")
	result := make(map[string]string)
	for i := 0; i < len(reData); i++ {
		var col = strings.Split(reData[i], ":")
		if len(col) > 1 {
			if typeof(col[0]) == "string" && typeof(col[1]) == "string" {
				result[col[0]] = col[1]
			}
		} else {
			if typeof(col[0]) == "string" {
				result[strconv.Itoa(i)] = col[0]
			}
		}
	}
	return result
}
func (m *m) Db() *gorm.DB {
	return m.connMaster().Model(m.modelInfo)
}
func typeof(v interface{}) string {
	return fmt.Sprintf("%T", v)
}

func Struct2Map(m interface{}) map[string]string {
	var result map[string]string
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	data, _ := json.Marshal(m)
	reader := strings.NewReader(string(data))
	decoder := json.NewDecoder(reader)
	decoder.Decode(&result)
	return result
}

package tdengine

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/shinxiang/tdengine/scope"
	"github.com/shinxiang/tdengine/util"
	_ "github.com/taosdata/driver-go/v3/taosRestful"
)

type TDengine struct {
	conn    *sql.DB
	dsn     string
	logMode bool
}

// New initialize a new db connection.
// @param dsn https://docs.taosdata.com/connector/go/#DSN
func New(dsn string) (*TDengine, error) {
	if !strings.Contains(dsn, "@http") {
		return nil, errors.New("dsn not found @http")
	}

	db := &TDengine{
		dsn: dsn,
	}
	err := db.connect()
	return db, err
}

func (t *TDengine) connect() (err error) {
	t.conn, err = sql.Open("taosRestful", t.dsn)
	if err != nil {
		t.debug("Failed to connect tdengine by restful, err: " + err.Error())
		return
	}
	err = t.Ping()
	if err == nil {
		t.debug("Connect tdengine success.")
	}
	return
}

func (t *TDengine) Close() error {
	return t.conn.Close()
}

func (t *TDengine) Ping() error {
	if t.conn == nil {
		return errors.New("tdengine unconnected")
	}
	err := t.conn.Ping()
	if err != nil {
		t.debug("tdengine connect error: " + err.Error())
	}
	return err
}

func (t *TDengine) LogMode(enable bool) *TDengine {
	t.logMode = enable
	return t
}

func (t *TDengine) debug(v string) {
	if t.logMode {
		log.Println("[DEBUG]", v)
	}
}

// Exec executes a query without returning any rows.
func (t *TDengine) Exec(sql string, args ...interface{}) (rowsAffected int64, err error) {
	result, err := t.conn.Exec(sql, args...)
	if err != nil {
		t.debug(fmt.Sprintf("%s \n\033[36;31m[%v]\033[0m", sql, err))
		return
	}
	rowsAffected, err = result.RowsAffected()
	t.debug(fmt.Sprintf("%s \n\033[36;31m[%v]\033[0m", sql, strconv.FormatInt(rowsAffected, 10)+" rows affected or returned"))
	return
}

// Query finds all records that returns result.
func (t *TDengine) Query(result interface{}, sql string, args ...interface{}) error {
	if result == nil {
		return errors.New("result is nil")
	}
	rs := reflect.ValueOf(result)
	if rs.Type().Kind() != reflect.Ptr {
		return errors.New("result is not a pointer")
	}
	rsRow := reflect.Indirect(rs)
	if rsRow.Type().Kind() != reflect.Slice {
		return errors.New("result is not a pointer of slice")
	}
	rsRowType := rsRow.Type().Elem()

	rows, err := t.conn.Query(sql, args...)
	if err != nil {
		t.debug(fmt.Sprintf("%s \n\033[36;31m[%v]\033[0m", sql, err))
		return err
	}
	t.debug(sql)
	defer rows.Close()

	columns, _ := rows.Columns()                   // columns name
	var values = make([]interface{}, len(columns)) // columns value
	var pDest = make([]interface{}, len(columns))
	var index = make(map[string]int)
	for i, name := range columns {
		index[name] = i
		pDest[i] = &values[i]
	}

	for rows.Next() {
		// Scan value must be a pointer
		err = rows.Scan(pDest...)
		if err != nil {
			return err
		}

		// New instance
		var item reflect.Value
		if rsRowType.Kind() == reflect.Map {
			item = reflect.MakeMap(rsRowType)
		} else {
			item = reflect.New(rsRowType).Elem()
		}

		switch rsRowType.Kind() {
		case reflect.Map:
			for i, value := range values {
				item.SetMapIndex(reflect.ValueOf(columns[i]), reflect.ValueOf(value))
			}
		case reflect.Struct:
			for i := 0; i < item.NumField(); i++ {
				if field := item.Field(i); field.CanSet() {
					structField := util.NewStructField(item.Type().Field(i).Tag)

					if columnName, ok := structField.TagSettingsGet("COLUMN"); ok {
						if idx, found := index[columnName]; found {
							value := reflect.ValueOf(values[idx])
							util.SetFieldValue(&field, value)
						}
					}
				}
			}
		default:
			return errors.New("unsupported result type, must be *[]struct or *[]map[string]interface{}")
		}
		rsRow.Set(reflect.Append(rsRow, item))
	}

	return nil
}

// First finds the first record that return a result.
func (t *TDengine) First(result interface{}, sql string, args ...interface{}) error {
	if result == nil {
		return errors.New("result is nil")
	}
	rs := reflect.ValueOf(result)
	if rs.Type().Kind() != reflect.Ptr {
		return errors.New("result is not a pointer")
	}
	rsRow := reflect.Indirect(rs)
	if rsRow.Type().Kind() != reflect.Struct {
		return errors.New("result is not a pointer of struct")
	}

	rows, err := t.conn.Query(sql, args...)
	if err != nil {
		t.debug(fmt.Sprintf("%s \n\033[36;31m[%v]\033[0m", sql, err))
		return err
	}
	t.debug(sql)
	defer rows.Close()

	columns, _ := rows.Columns()                   // columns name
	var values = make([]interface{}, len(columns)) // columns value
	var pDest = make([]interface{}, len(columns))
	var index = make(map[string]int)
	for i, name := range columns {
		index[name] = i
		pDest[i] = &values[i]
	}

	for rows.Next() {
		// Scan value must be a pointer
		err = rows.Scan(pDest...)
		if err != nil {
			return err
		}

		// New instance
		var item reflect.Value
		item = reflect.New(rsRow.Type()).Elem()

		for i := 0; i < item.NumField(); i++ {
			if field := item.Field(i); field.CanSet() {
				structField := util.NewStructField(item.Type().Field(i).Tag)

				if columnName, ok := structField.TagSettingsGet("COLUMN"); ok {
					if idx, found := index[columnName]; found {
						value := reflect.ValueOf(values[idx])
						util.SetFieldValue(&field, value)
					}
				}
			}
		}
		rsRow.Set(item)
		break
	}

	return nil
}

// Count return the count of records.
func (t *TDengine) Count(sql string, args ...interface{}) (int64, error) {
	var result = struct {
		Count int64 `td:"column:total_count"`
	}{}
	err := t.First(&result, "SELECT COUNT(*) AS total_count FROM ("+sql+") AS _TEMPORARY_TABLE_COUNT", args...)
	if err != nil {
		return 0, err
	}
	return result.Count, nil
}

// Insert one struct data
// @param stableName super table Name
// @param value one struct data
func (t *TDengine) Insert(stableName string, value interface{}) error {
	if stableName == "" {
		return errors.New("stable name is nil")
	}
	if value == nil {
		return errors.New("value is nil")
	}

	refVal := reflect.ValueOf(value)
	if refVal.Type().Kind() == reflect.Ptr {
		refVal = refVal.Elem()
	}
	if refVal.Type().Kind() != reflect.Struct {
		return errors.New("unsupported value type, must be struct or *struct")
	}

	// Get sub table name.
	newScope := scope.NewScope(refVal)
	tableName := newScope.TableName()

	var strSql = "INSERT INTO"
	var tags = make([]string, 0)
	var vals = make([]string, 0)
	for i := 0; i < refVal.NumField(); i++ {
		if field := refVal.Field(i); field.IsValid() {
			structField := util.NewStructField(refVal.Type().Field(i).Tag)

			if !structField.IsIgnored {
				if structField.IsTagKey {
					tags = append(tags, getTagsByField(field))
				} else {
					vals = append(vals, getValsByField(field))
				}
			}
		}
	}
	tagStr := strings.Join(tags, ",")
	valStr := strings.Join(vals, ",")

	if stableName != "" && len(tags) > 0 {
		strSql += fmt.Sprintf(" %s USING %s TAGS (%s) VALUES (%s)", tableName, stableName, tagStr, valStr)
	} else {
		strSql += fmt.Sprintf(" %s VALUES (%s)", tableName, valStr)
	}

	_, err := t.Exec(strSql)
	return err
}

// InsertBatch insert a slice of struct data
// @param stableName super table Name
// @param value the slice of struct data
func (t *TDengine) InsertBatch(stableName string, values interface{}) error {
	if stableName == "" {
		return errors.New("stable name is nil")
	}
	if values == nil {
		return errors.New("values is nil")
	}
	refValues := reflect.ValueOf(values)
	if refValues.Type().Kind() == reflect.Ptr {
		refValues = refValues.Elem()
	}
	if refValues.Type().Kind() != reflect.Slice {
		return errors.New("values is not a slice")
	}
	if refValues.Len() == 0 {
		return errors.New("values is empty")
	}

	var strSql = "INSERT INTO"
	var tableName string
	var tags = make([]string, 0)
	var vals = make([]string, 0)
	for i := 0; i < refValues.Len(); i++ {
		refVal := refValues.Index(i)
		if refVal.Type().Kind() == reflect.Ptr {
			refVal = refVal.Elem()
		}
		if refVal.Type().Kind() != reflect.Struct {
			return errors.New("unsupported value type, must be []struct or []*struct")
		}

		// Get sub table name.
		newScope := scope.NewScope(refVal)
		tableName = newScope.TableName()

		tags = tags[0:0]
		vals = vals[0:0]
		for i := 0; i < refVal.NumField(); i++ {
			if field := refVal.Field(i); field.IsValid() {
				structField := util.NewStructField(refVal.Type().Field(i).Tag)

				if !structField.IsIgnored {
					if structField.IsTagKey {
						tags = append(tags, getTagsByField(field))
					} else {
						vals = append(vals, getValsByField(field))
					}
				}
			}
		}
		tagStr := strings.Join(tags, ",")
		valStr := strings.Join(vals, ",")

		if stableName != "" && len(tags) > 0 {
			strSql += fmt.Sprintf(" %s USING %s TAGS (%s) VALUES (%s)", tableName, stableName, tagStr, valStr)
		} else {
			strSql += fmt.Sprintf(" %s VALUES (%s)", tableName, valStr)
		}
	}

	_, err := t.Exec(strSql)
	return err
}

// get TAGS of sql
func getTagsByField(field reflect.Value) (tags string) {
	v := field.Interface()
	switch field.Kind() {
	case reflect.String:
		tags = fmt.Sprintf("'%s'", v)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		tags = fmt.Sprintf("%d", v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		tags = fmt.Sprintf("%d", v)
	case reflect.Float32, reflect.Float64:
		tags = fmt.Sprintf("%v", v)
	default:
		tags = fmt.Sprintf("%v", v)
	}
	return
}

// get VALUES of sql
func getValsByField(field reflect.Value) (vals string) {
	v := field.Interface()
	switch field.Kind() {
	case reflect.Struct:
		if t, ok := v.(time.Time); ok {
			vals = fmt.Sprintf("%d", t.UnixMilli())
		} else {
			vals = fmt.Sprintf("'%s'", util.ToJSON(v))
		}
	case reflect.String:
		vals = fmt.Sprintf("'%s'", v)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		vals = fmt.Sprintf("%d", v)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		vals = fmt.Sprintf("%d", v)
	case reflect.Float32, reflect.Float64:
		vals = fmt.Sprintf("%v", v)
	default:
		vals = fmt.Sprintf("%v", v)
	}
	return
}

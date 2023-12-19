package util

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func SetFieldValue(field *reflect.Value, value reflect.Value) {
	if value.IsValid() {
		if field.Type() == value.Type() {
			field.Set(value)
		} else {
			switch field.Kind() {
			case reflect.Bool:
				field.SetBool(toBool(value))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				field.SetInt(toInt(value))
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				field.SetUint(toUint(value))
			case reflect.Float32, reflect.Float64:
				field.SetFloat(toFloat(value))
			}
		}
	}
}

func ToJSON(data interface{}) string {
	j, err := json.Marshal(data)
	if err != nil {
		return "{}"
	} else {
		js := string(j)
		js = strings.Replace(js, "\\u003c", "<", -1)
		js = strings.Replace(js, "\\u003e", ">", -1)
		js = strings.Replace(js, "\\u0026", "&", -1)
		return js
	}
}

func toBool(data reflect.Value) bool {
	switch data.Kind() {
	case reflect.Bool:
		return data.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return data.Int() != 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return data.Uint() != 0
	case reflect.Float32, reflect.Float64:
		return data.Float() != 0
	case reflect.String:
		b, err := strconv.ParseBool(data.String())
		if err == nil {
			return b
		} else {
			return false
		}
	default:
		return false
	}
}

func toInt(data reflect.Value) int64 {
	switch data.Kind() {
	case reflect.Bool:
		if data.Bool() {
			return 1
		} else {
			return 0
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return data.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(data.Uint())
	case reflect.Float32, reflect.Float64:
		return int64(data.Float())
	case reflect.String:
		v, err := strconv.ParseInt(data.String(), 10, 64)
		if err == nil {
			return v
		} else {
			return 0
		}
	case reflect.Struct:
		v := data.Interface()
		if t, ok := v.(time.Time); ok {
			return t.UnixMilli()
		} else {
			return 0
		}
	default:
		return 0
	}
}

func toUint(data reflect.Value) uint64 {
	switch data.Kind() {
	case reflect.Bool:
		if data.Bool() {
			return 1
		} else {
			return 0
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return uint64(data.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return data.Uint()
	case reflect.Float32, reflect.Float64:
		return uint64(data.Float())
	case reflect.String:
		v, err := strconv.ParseUint(data.String(), 10, 64)
		if err == nil {
			return v
		} else {
			return 0
		}
	case reflect.Struct:
		v := data.Interface()
		if t, ok := v.(time.Time); ok {
			return uint64(t.UnixMilli())
		} else {
			return 0
		}
	default:
		return 0
	}
}

func toFloat(data reflect.Value) float64 {
	switch data.Kind() {
	case reflect.Bool:
		if data.Bool() {
			return 1
		} else {
			return 0
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(data.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(data.Uint())
	case reflect.Float32, reflect.Float64:
		return data.Float()
	case reflect.String:
		v, err := strconv.ParseFloat(data.String(), 64)
		if err == nil {
			return v
		} else {
			return 0
		}
	default:
		return 0
	}
}

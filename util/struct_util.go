package util

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
)

func SetFieldValue(field *reflect.Value, value reflect.Value) {
	if value.IsValid() {
		if field.Type() == value.Type() {
			field.Set(value)
		} else {
			switch field.Kind() {
			case reflect.Bool:
				field.SetBool(ToBool(value))
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				field.SetInt(value.Int())
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				field.SetUint(value.Uint())
			case reflect.Float32, reflect.Float64:
				field.SetFloat(value.Float())
			}
		}
	}
}

func ToBool(data reflect.Value) bool {
	switch data.Kind() {
	case reflect.Bool:
		return data.Bool()
	case reflect.Int:
		return data.Int() != 0
	case reflect.Uint:
		return data.Uint() != 0
	case reflect.Float32:
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

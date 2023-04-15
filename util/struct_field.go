package util

import (
	"reflect"
	"strings"
	"sync"
)

// StructField model field's struct definition
type StructField struct {
	Tag         reflect.StructTag
	TagSettings map[string]string

	IsTagKey  bool // IS TAG COLUMN
	IsIgnored bool // IS IGNORE

	tagSettingsLock sync.RWMutex
}

func NewStructField(tag reflect.StructTag) *StructField {
	field := &StructField{
		Tag:         tag,
		TagSettings: parseTagSetting(tag),
	}
	// is ignored field
	if _, ok := field.TagSettingsGet("-"); ok {
		field.IsIgnored = true
	} else {
		if _, ok := field.TagSettingsGet("TAG"); ok {
			field.IsTagKey = true
		}
	}
	return field
}

// TagSettingsGet returns a tag from the tag settings
func (sf *StructField) TagSettingsGet(key string) (string, bool) {
	sf.tagSettingsLock.RLock()
	defer sf.tagSettingsLock.RUnlock()
	val, ok := sf.TagSettings[key]
	return val, ok
}

// TagSettingsSet Sets a tag in the tag settings map
func (sf *StructField) TagSettingsSet(key, val string) {
	sf.tagSettingsLock.Lock()
	defer sf.tagSettingsLock.Unlock()
	sf.TagSettings[key] = val
}

func parseTagSetting(tags reflect.StructTag) map[string]string {
	setting := map[string]string{}
	for _, str := range []string{tags.Get("sql"), tags.Get("td")} {
		if str == "" {
			continue
		}
		tags := strings.Split(str, ";")
		for _, value := range tags {
			v := strings.Split(value, ":")
			k := strings.TrimSpace(strings.ToUpper(v[0]))
			if len(v) >= 2 {
				setting[k] = strings.Join(v[1:], ":")
			} else {
				setting[k] = k
			}
		}
	}
	return setting
}

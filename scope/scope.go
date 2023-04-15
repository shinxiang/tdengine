package scope

import (
	"reflect"
	"regexp"
	"strings"
)

type tabler interface {
	TableName() string
}

type Scope struct {
	Value interface{}
	Type  reflect.Type

	tabler tabler
}

// NewScope create a new Scope
func NewScope(value reflect.Value) *Scope {
	scope := &Scope{
		Value: reflect.New(value.Type()).Interface(),
		Type:  value.Type(),
	}

	if tabler, ok := scope.Value.(tabler); ok {
		scope.tabler = tabler
		scope.copy(value)
	}
	return scope
}

// TableName return table name
func (scope *Scope) TableName() string {
	if scope.tabler != nil {
		return scope.tabler.TableName()
	}
	return scope.GetModelStructName()
}

func (scope *Scope) GetModelStructName() string {
	name := scope.Type.String()
	s := strings.Split(name, ".")
	if len(s) >= 2 {
		name = s[len(s)-1]
	}
	return camelCase2Underscore(name)
}

func (scope *Scope) copy(value reflect.Value) error {
	destVal := reflect.ValueOf(scope.Value).Elem()
	for i := 0; i < value.NumField(); i++ {
		src := value.Field(i)
		dest := destVal.Field(i)
		if dest.IsValid() && dest.Type() == src.Type() {
			dest.Set(src)
		}
	}
	return nil
}

func camelCase2Underscore(str string) string {
	var matchNonAlphaNumeric = regexp.MustCompile(`[^a-zA-Z0-9]+`)
	var matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	var matchAllCap = regexp.MustCompile("([a-z0-9])([A-Z])")

	str = matchNonAlphaNumeric.ReplaceAllString(str, "_")
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")

	return strings.ToLower(snake) //全部转小写
}

## TDengine ORM SDK
- TDengine是国产优秀的时序数据库，详见https://docs.taosdata.com/ 
- 在[TDengine 客户端](https://github.com/taosdata/driver-go)基础上仿GORM进行了简单封装，实现了ORM映射。
- 本SDK驱动基于REST 连接支持所有能运行 Go 的平台。

## Model Struct
- 结构体中字段的顺序必须与超级表中字段的顺序相同，字段类型相匹配
- 结构体的第一个字段必须是time.Time或int64类型的TIMESTAMP字段
- 结构体中tag标签用法和[GORM](https://gorm.io/docs/)类似，关键字是 td（ 或 sql）
- 字段名标识为"column"，超级表TAG字段标识为"TAG"，忽略字段为"-"
  示例:
```go
type Model struct {
	Ts       time.Time `td:"column:ts" json:"ts"`
	Current  float32   `td:"column:current" json:"current"`
	Voltage  int       `td:"column:voltage" json:"voltage"`
	DeviceId string    `td:"column:device_id;TAG" json:"device_id"`
	GroupId  int       `td:"column:group_id;TAG" json:"group_id"`
	Ignore   bool      `td:"-" json:"-"`
}

func (a *Model) TableName() string {
	return "device_" + a.DeviceId
}
```

## Install

```
go get -u github.com/shinxiang/tdengine@master
```

### Usage

```go
import (
    "github.com/shinxiang/tdengine"
)
```
## Example

Check example code [example](https://github.com/shinxiang/tdengine/blob/master/example/example.go)


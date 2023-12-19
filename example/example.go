package main

import (
	"github.com/shinxiang/tdengine"
	"log"
	"strconv"
	"time"
)

type Model struct {
	Ts       time.Time `td:"column:ts" json:"ts"`
	Current  float32   `td:"column:current" json:"current,omitempty"`
	Voltage  int       `td:"column:voltage" json:"voltage,omitempty"`
	Phase    float32   `td:"column:phase" json:"phase,omitempty"`
	DeviceId string    `td:"column:device_id;TAG" json:"device_id,omitempty"`
	GroupId  int       `td:"column:group_id;TAG" json:"group_id,omitempty"`
}

// TableName return sub table name of Model
func (a *Model) TableName() string {
	return "device_" + a.DeviceId
}

func main() {
	var dsn = "root:taosdata@http(192.168.1.250:6041)/test"
	db, err := tdengine.New(dsn)
	if err != nil {
		log.Fatalln("failed to connect, err:", err)
	}
	db.LogMode(true)
	defer db.Close()

	createDatabase(db)
	createStable(db)
	insertTest(db)
	insertBatchTest(db)
	queryTest(db)
}

func createDatabase(db *tdengine.TDengine) {
	_, err := db.Exec("CREATE DATABASE IF NOT EXISTS test")
	if err != nil {
		log.Fatalln("failed to create database, err:", err)
	}
}

func createStable(db *tdengine.TDengine) {
	_, err := db.Exec("DROP TABLE IF EXISTS test.meters")
	if err != nil {
		log.Fatalln("failed to drop stable, err:", err)
	}
	_, err = db.Exec("CREATE STABLE IF NOT EXISTS test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (device_id BINARY(64), group_id INT)")
	if err != nil {
		log.Fatalln("failed to create stable, err:", err)
	}
}

func insertTest(db *tdengine.TDengine) {
	now := time.Now()
	data := Model{
		Ts:       now,
		Current:  19.1,
		Voltage:  290,
		Phase:    19,
		DeviceId: "1000",
		GroupId:  1,
	}

	err := db.Insert("meters", &data)
	if err != nil {
		log.Fatalln("failed to insert, err:", err)
	}
}

func insertBatchTest(db *tdengine.TDengine) {
	now := time.Now()
	data := make([]*Model, 0)
	for i := 1; i <= 4; i++ {
		m := &Model{
			Ts:       now.Add(1 * time.Second),
			Current:  11.1,
			Voltage:  220,
			Phase:    3,
			DeviceId: strconv.Itoa(100 + i),
			GroupId:  i,
		}
		data = append(data, m)
	}

	err := db.InsertBatch("meters", &data)
	if err != nil {
		log.Fatalln("failed to insert, err:", err)
	}
}

func queryTest(db *tdengine.TDengine) {
	var item []Model
	err := db.Query(&item, "SELECT * FROM meters order by device_id")
	if err != nil {
		log.Fatalln("failed to select from table, err:", err)
	}

	log.Println("Query Data:", item)
}

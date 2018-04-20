package mongo

import (
	"encoding/json"
	"time"

	"github.com/goinbox/golog"
	"gopkg.in/mgo.v2/bson"

	"testing"
)

type TestIdGenterStruct struct {
	Id    string `bson:"_id"`
	Maxid int32  `bson:"max_id"`
}

var client *Client

const (
	MONGO_TEST_COLLECTION = "mycoll"
)

func getTestClient() *Client {
	w, _ := golog.NewFileWriter("/tmp/test_mongo.log")
	logger, _ := golog.NewSimpleLogger(w, golog.LEVEL_INFO, golog.NewSimpleFormater())

	config := NewConfig("localhost", "myport", "myuser", "mypass", "mydb")
	config.LogLevel = golog.LEVEL_DEBUG

	return NewClient(config, logger)
}

func init() {
	client = getTestClient()
	//client.Free()
}

func TestInsert(t *testing.T) {
	var err error
	doc := bson.M{"_id": 11, "a": 1, "b": 2}
	err = client.Insert(MONGO_TEST_COLLECTION, doc)
	if err != nil {
		t.Error(err)
	}
	total := 3
	docs := make([]interface{}, total)
	for i := 0; i < total; i++ {
		docs[i] = bson.M{"_id": i, "a": 3, "b": 4}
	}
	err = client.Insert(MONGO_TEST_COLLECTION, docs...)
	if err != nil {
		t.Error(err)
	}
}

func TestUpdate(t *testing.T) {
	selector := bson.M{"_id": 1}
	updater := bson.M{
		"$inc":         bson.M{"view_count": 1},
		"$currentDate": bson.M{"edit_time": true},
	}
	err := client.Update(MONGO_TEST_COLLECTION, selector, updater)
	if err != nil {
		t.Error(err)
	}
}

func TestUpdateAll(t *testing.T) {
	selector := bson.M{"_id": bson.M{"$gt": 0}}
	updater := bson.M{
		"$inc":         bson.M{"view_count": 1},
		"$currentDate": bson.M{"edit_time": true},
	}
	err := client.UpdateAll(MONGO_TEST_COLLECTION, selector, updater)
	if err != nil {
		t.Error(err)
	}
}

func TestUpdateId(t *testing.T) {
	id := 1
	updater := bson.M{
		"$inc":         bson.M{"view_count": 1},
		"$currentDate": bson.M{"edit_time": true},
	}
	err := client.UpdateId(MONGO_TEST_COLLECTION, id, updater)
	if err != nil {
		t.Error(err)
	}
}

func TestUpsert(t *testing.T) {
	selector := bson.M{"_id": 4}
	updater := bson.M{
		"$inc":         bson.M{"view_count": 1},
		"$currentDate": bson.M{"edit_time": true},
		"$setOnInsert": bson.M{"add_time": "2018-06-23 09:00:00"},
	}
	err := client.Upsert(MONGO_TEST_COLLECTION, selector, updater)
	if err != nil {
		t.Error(err)
	}
}

func TestQuery(t *testing.T) {
	query := NewQuery().SetMaxTime(1 * time.Second)
	result, err := client.Query(MONGO_TEST_COLLECTION, query)
	if err != nil {
		t.Error(err)
	}
	jsonData, _ := json.Marshal(result)
	t.Logf("%s", jsonData)
}

func TestQueryOne(t *testing.T) {
	query := NewQuery().Query(bson.M{"_id": bson.M{"$gt": 0}}).Select(bson.M{"_id": 0}).Skip(1)
	result, err := client.QueryOne(MONGO_TEST_COLLECTION, query)
	if err != nil {
		t.Error(err)
	}
	jsonData, _ := json.Marshal(result)
	t.Logf("%s", jsonData)
}

func TestQueryId(t *testing.T) {
	result, err := client.QueryId(MONGO_TEST_COLLECTION, 4)
	if err != nil {
		t.Error(err)
	}
	jsonData, _ := json.Marshal(result)
	t.Logf("%s", jsonData)
}

func TestQueryCount(t *testing.T) {
	query := NewQuery()
	result, err := client.QueryCount(MONGO_TEST_COLLECTION, query)
	if err != nil {
		t.Error(err)
	}
	jsonData, _ := json.Marshal(result)
	t.Logf("%s", jsonData)
}

func TestFind(t *testing.T) {
	result := []bson.M{}
	err := client.Find(MONGO_TEST_COLLECTION, bson.M{"_id": bson.M{"$gt": 0}}).All(&result)
	if err != nil {
		t.Error(err)
	}
	jsonData, _ := json.Marshal(result)
	t.Logf("%s", jsonData)
}

func TestFindId(t *testing.T) {
	result := bson.M{}
	err := client.FindId(MONGO_TEST_COLLECTION, 4).One(&result)
	if err != nil {
		t.Error(err)
	}
	jsonData, _ := json.Marshal(result)
	t.Logf("%s", jsonData)
}

func TestFindAndModify(t *testing.T) {
	finder := bson.M{"_id": "app"}
	updater := bson.M{"$inc": bson.M{"max_id": 1}}
	result, err := client.FindAndModify("id_genter", finder, updater)
	if err != nil {
		t.Error(err)
	}

	doc := new(TestIdGenterStruct)
	err = client.ConvertBsonToStruct(result, doc)
	if err != nil {
		t.Error(err)
	}
	t.Log(doc)

	jsonData, _ := json.Marshal(result)
	t.Logf("%s", jsonData)
	json.Unmarshal(jsonData, doc)
	t.Log(doc)
}

func TestRemove(t *testing.T) {
	selector := bson.M{"_id": 4}
	err := client.Remove(MONGO_TEST_COLLECTION, selector)
	if err != nil {
		t.Error(err)
	}
}

func TestRemoveAll(t *testing.T) {
	selector := bson.M{"_id": bson.M{"$gt": 0}}
	err := client.RemoveAll(MONGO_TEST_COLLECTION, selector)
	if err != nil {
		t.Error(err)
	}
}

func TestRemoveId(t *testing.T) {
	id := 1
	err := client.RemoveId(MONGO_TEST_COLLECTION, id)
	if err != nil {
		t.Error(err)
	}
}

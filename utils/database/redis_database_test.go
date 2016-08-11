// Copyright 2016 IBM Corporation
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package database

import (
	//"encoding/json"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/assert"

	"github.com/amalgam8/registry/auth"
)

var mockConn *redigomock.Conn
var db Database
var mockNamespace auth.Namespace

type Endpoint struct {
	Type  string
	Value string
}
type ServiceInstance struct {
	ID               string
	ServiceName      string
	Endpoint         *Endpoint
	Status           string
	Metadata         []byte
	RegistrationTime time.Time
	LastRenewal      time.Time
	TTL              time.Duration
	Tags             []string
	Extension        map[string]interface{}
}

func init() {
	mockConn = redigomock.NewConn()
	mockNamespace = "test"
	db = NewRedisDBWithConn(mockConn, mockNamespace, "addr", "pass")
}

func TestRedisDBReadKeys(t *testing.T) {
	mockConn.Clear()

	var expectedKeys []interface{}
	expectedKeys = append(expectedKeys, []byte("key1"))
	expectedKeys = append(expectedKeys, []byte("key2"))
	expectedKeys = append(expectedKeys, []byte("key3"))

	expectedStrings := []string{"key1", "key2", "key3"}

	hkeyCmd := mockConn.GenericCommand("HKEYS").Expect(expectedKeys)

	keys, err := db.ReadKeys()

	assert.Equal(t, 1, mockConn.Stats(hkeyCmd))
	assert.NoError(t, err)
	assert.NotNil(t, keys)
	assert.Equal(t, expectedStrings, keys)
}

func TestRedisDBReadEntry(t *testing.T) {
	mockConn.Clear()

	key := "key1"
	value := "value1"

	cmd := mockConn.Command("HGET", mockNamespace.String(), key).Expect([]byte(value))

	entry, err := db.ReadEntry(key)

	assert.Equal(t, 1, mockConn.Stats(cmd))
	assert.NoError(t, err)
	assert.Equal(t, value, entry)
}

func TestRedisDBReadEntryErrorReturned(t *testing.T) {
	mockConn.Clear()

	key := "key1error"
	hgetError := fmt.Errorf("Error calling HGET")

	cmd := mockConn.Command("HGET", mockNamespace.String(), key).ExpectError(hgetError)

	_, err := db.ReadEntry(key)

	assert.Equal(t, 1, mockConn.Stats(cmd))
	assert.Error(t, err)
	assert.Equal(t, hgetError, err)
}

func TestRedisDBReadAllEntries(t *testing.T) {
	mockConn.Clear()

	expectedMap := make(map[string]string)
	expectedMap["key1"] = "value1"
	expectedMap["key2"] = "value2"
	expectedMap["key3"] = "value3"

	hkeyCmd := mockConn.Command("HGETALL", mockNamespace.String()).ExpectMap(expectedMap)

	entries, err := db.ReadAllEntries()

	assert.Equal(t, 1, mockConn.Stats(hkeyCmd))
	assert.NoError(t, err)
	assert.NotNil(t, entries)
	assert.Equal(t, expectedMap, entries)
}

func TestRedisDBReadAllMatchingEntries(t *testing.T) {
	mockConn.Clear()

	si := &ServiceInstance{
		ServiceName: "Calc",
		Endpoint:    &Endpoint{Value: "192.168.0.1", Type: "tcp"},
	}

	s, _ := generateMockHScanCommandOutput("inst-id", si)
	cmd := mockConn.GenericCommand("HSCAN").Expect(s)

	instance, err := db.ReadAllMatchingEntries("inst-id")
	assert.NoError(t, err)

	var actualSI ServiceInstance
	err = json.Unmarshal([]byte(instance["inst-id"]), &actualSI)

	assert.Equal(t, 1, mockConn.Stats(cmd))
	assert.Equal(t, "inst-id", actualSI.ID)
	assert.Equal(t, "Calc", actualSI.ServiceName)
}

func TestRedisDBInsertEntry(t *testing.T) {
	mockConn.Clear()

	key := "key1"
	entry := "entry1"

	cmd := mockConn.Command("HSET", mockNamespace.String(), key, entry).Expect(123)

	err := db.InsertEntry(key, entry)

	assert.Equal(t, 1, mockConn.Stats(cmd))
	assert.NoError(t, err)
}

func TestRedisDBDeleteEntry(t *testing.T) {
	mockConn.Clear()

	cmd := mockConn.Command("HDEL", mockNamespace.String(), "inst-id").Expect([]byte("1"))

	hdel, err := db.DeleteEntry("inst-id")

	assert.Equal(t, 1, mockConn.Stats(cmd))
	assert.NoError(t, err)
	assert.Equal(t, 1, hdel)
}

func generateMockHScanCommandOutput(instID string, instance *ServiceInstance) ([]interface{}, *ServiceInstance) {
	if instID != "" {
		instance.ID = instID
	}
	instanceJSON, _ := json.Marshal(instance)

	var s, sBytes []interface{}

	b1 := []byte{'0'}
	s = append(s, b1)

	var instanceData interface{}
	instBytes := []byte(instID)
	instanceData = instBytes
	sBytes = append(sBytes, instanceData)

	instBytes = []byte(instanceJSON)
	instanceData = instBytes

	sBytes = append(sBytes, instanceData)

	s = append(s, sBytes)

	return s, instance
}

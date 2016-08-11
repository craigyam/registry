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

const (
	module = "DATABASE"
)

// Database functions to manage data stored in an external database
type Database interface {
	ReadKeys() ([]string, error)
	ReadEntry(key string) (string, error)
	ReadAllEntries() (map[string]string, error)
	ReadAllMatchingEntries(match string) (map[string]string, error)
	InsertEntry(key string, entry string) error
	DeleteEntry(key string) (int, error)
}

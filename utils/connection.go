// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"fmt"

	"github.com/gocraft/dbr/v2"

	_ "github.com/go-sql-driver/mysql"
)

// Connection represents a MySQL or Dolt connection.
type Connection struct {
	conn *dbr.Connection
}

// NewConnection returns a new Connection.
func NewConnection(user string, password string, host string, port int) (*Connection, error) {
	conn, err := dbr.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/", user, password, host, port), nil)
	if err != nil {
		return nil, err
	}
	_, err = conn.Exec(`SET CHARACTER SET "utf8mb4";`)
	if err != nil {
		return nil, err
	}
	_, err = conn.Exec(`SET collation_connection = "utf8mb4_0900_bin";`)
	if err != nil {
		return nil, err
	}
	_, err = conn.Exec(`SET character_set_results = binary;`)
	if err != nil {
		return nil, err
	}
	return &Connection{conn}, nil
}

// Query is used to retrieve the value of a query that returns a single row and a single value.
func (conn *Connection) Query(query string) (_ []byte, err error) {
	results, err := conn.conn.Query(query)
	if err != nil {
		return nil, err
	}
	defer func() {
		nerr := results.Close()
		if err == nil {
			err = nerr
		}
	}()
	out := []byte{}
	if !results.Next() {
		return nil, fmt.Errorf("no rows returned from query: %s", query)
	}
	if colNames, err := results.Columns(); err != nil {
		return nil, err
	} else if len(colNames) != 1 {
		return nil, fmt.Errorf("the following query returned %d columns instead of 1: %s", len(colNames), query)
	}
	err = results.Scan(&out)
	if err != nil {
		return nil, err
	}
	if out == nil {
		return nil, nil
	}
	return out, nil
}

// Close should be called when the connection is no longer needed.
func (conn *Connection) Close() error {
	return conn.conn.Close()
}

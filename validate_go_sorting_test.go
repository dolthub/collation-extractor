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

package main

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/collation-extractor/utils"
)

const (
	TestValidateGoSorting_user     = "root"
	TestValidateGoSorting_password = "password"
	TestValidateGoSorting_host     = "localhost"
	TestValidateGoSorting_port     = 3306
)

// TestValidateGoSorting compares Go's standard string sorting (using the comparison operators `<` and `>`) with the
// default collation of GMS (which is `utf8mb4_0900_bin`). It does this in two steps. First, we ensure that Go's UTF8
// encoding sorts in the same order as the rune order. Second, we use MySQL's `STRCMP` function to compare characters,
// validating that the collation weighs its characters in the same order that Go does with its runes.
func TestValidateGoSorting(t *testing.T) {
	// This enforces that UTF8-encoded equivalents of runes are also sorted the same as their numeric counterparts
	iter := utils.NewUTF8Iter()
	prevR, _ := iter.Next()
	for r, ok := iter.Next(); ok; r, ok = iter.Next() {
		// Converting a rune to a string will encode the rune (which is an int32) as a sequence of valid UTF8 bytes.
		// It is important to note that this byte sequence may have NO RELATION to the initial rune, and it is best
		// viewed as an arbitrary mapping from rune to byte sequence.
		require.True(t, string(prevR) < string(r), "%d >= %d", prevR, r)
		prevR = r
	}

	// Using the STRCMP function on strings containing a single character allows us to get the order of each character
	// in relation to the others.
	iter.Reset()
	conn, err := utils.NewConnection(TestValidateGoSorting_user, TestValidateGoSorting_password, TestValidateGoSorting_host, TestValidateGoSorting_port)
	require.NoError(t, err)
	defer conn.Close()
	prevR, _ = iter.Next()
	for r, ok := iter.Next(); ok; r, ok = iter.Next() {
		rAsBytes := []byte(string(r))
		prevRAsBytes := []byte(string(prevR))
		// We convert the string to a hexadecimal to ensure that Go's exact byte representation is being given to MySQL.
		// This also allows us to bypass escape rules.
		sqlOutput, err := conn.Query(fmt.Sprintf(
			"SELECT STRCMP(CONVERT(_utf8mb4 0x%s USING utf8mb4) COLLATE utf8mb4_0900_bin,"+
				"CONVERT(_utf8mb4 0x%s USING utf8mb4) COLLATE utf8mb4_0900_bin);",
			hex.EncodeToString(prevRAsBytes), hex.EncodeToString(rAsBytes)))
		if assert.NoError(t, err) {
			assert.Equal(t, "-1", string(sqlOutput),
				"Previous Rune: %d, Bytes: %v\nCurrent Rune: %d, Bytes: %v", prevR, prevRAsBytes, r, rAsBytes)
		}
		prevR = r
	}
}

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
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/collation-extractor/utils"
)

const (
	TestValidateGoUTF8_user     = "root"
	TestValidateGoUTF8_password = "password"
	TestValidateGoUTF8_host     = "localhost"
	TestValidateGoUTF8_port     = 3306
)

// TestValidateGoUTF8 is used to validate Go's entire range of unicode characters against MySQL's `utf8mb4` character
// set, as their encodings should be equivalent as they're both based on `utf8`. In addition, this validates that the
// iterator returns only valid UTF8 characters, and it returns the entire set of unicode characters (at least as much as
// Go implements, which may expand in future versions).
func TestValidateGoUTF8(t *testing.T) {
	// First we validate that the iterator returns all valid unicode characters
	iter := utils.NewUTF8Iter()
	// No valid runes are negative (at the time of writing this), so we can stop once we overflow and hit negative numbers
	for r := rune(0); r >= 0; r++ {
		iterR, ok := iter.Next()
		if !ok {
			// We've hit the end of the iterator, so all remaining numbers should be invalid runes.
			// This is a requirement rather than an assertion, otherwise we may log billions of errors.
			require.False(t, utf8.ValidRune(r))
			continue
		}
		// We increment both at the same time, so if r pulls ahead then the iterator returned the same rune twice
		require.True(t, r > iterR, "duplicate rune returned by iterator: %d", iterR)
		for ; r < iterR; r++ {
			// The iterator skipped this rune, so it should be invalid
			assert.False(t, utf8.ValidRune(r))
		}
		assert.True(t, utf8.ValidRune(r))
	}

	// Validate that all runes have the same encoding between Go and MySQL's `utf8mb4` character set
	iter.Reset()
	conn, err := utils.NewConnection(TestValidateGoUTF8_user, TestValidateGoUTF8_password, TestValidateGoUTF8_host, TestValidateGoUTF8_port)
	require.NoError(t, err)
	defer conn.Close()
	for r, ok := iter.Next(); ok; r, ok = iter.Next() {
		// Converting a rune to a string will encode the rune (which is an int32) as a sequence of valid UTF8 bytes.
		// It is important to note that this byte sequence may have NO RELATION to the initial rune, and it is best
		// viewed as an arbitrary mapping from rune to byte sequence.
		rAsStr := string(r)
		// We convert the string to a hexadecimal to ensure that Go's exact byte representation is being given to MySQL.
		// This also allows us to bypass escape rules.
		sqlOutput, err := conn.Query(fmt.Sprintf(`SELECT CAST(CONVERT(_utf8mb4 0x%s USING utf8mb4) AS BINARY);`,
			hex.EncodeToString([]byte(rAsStr))))
		if assert.NoError(t, err) {
			assert.Equal(t, []byte(rAsStr), sqlOutput)
		}
	}
}

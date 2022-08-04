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
	"os"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/collation-extractor/utils"
)

const (
	TestExtractCharacterSet_user     = "root"
	TestExtractCharacterSet_password = "password"
	TestExtractCharacterSet_host     = "localhost"
	TestExtractCharacterSet_port     = 3306
	TestExtractCharacterSet_charset  = "utf16"
	TestExtractCharacterSet_file     = "./" + TestExtractCharacterSet_charset + ".go.txt"
)

// TestExtractCharacterSet creates a Go file for embedding into GMS. It contains the data necessary to encode and decode
// the target character set. The prerequisite structs (such as RangeMap) should already be in GMS.
func TestExtractCharacterSet(t *testing.T) {
	iter := utils.NewUTF8Iter()
	charsetToGoString := utils.NewCharacterSetEncodingTree()
	conn, err := utils.NewConnection(TestExtractCharacterSet_user, TestExtractCharacterSet_password, TestExtractCharacterSet_host, TestExtractCharacterSet_port)
	require.NoError(t, err)
	defer conn.Close()
	for r, ok := iter.Next(); ok; r, ok = iter.Next() {
		// Converting a rune to a string will encode the rune (which is an int32) as a sequence of valid UTF8 bytes.
		// We then convert it to a byte slice to pass to the hex encoder and encoding trees.
		rAsBytes := []byte(string(r))
		// We convert the string to a hexadecimal to ensure that Go's exact byte representation is being given to MySQL.
		// This also allows us to bypass escape rules.
		sqlOutput, err := conn.Query(fmt.Sprintf(`SELECT CAST(CONVERT(_utf8mb4 0x%s USING %s) AS BINARY);`,
			hex.EncodeToString(rAsBytes), TestExtractCharacterSet_charset))
		require.NoError(t, err)

		// We add the output to the tree for converting from the character set to Go's encoding
		toGoStr := charsetToGoString
		for _, byteVal := range sqlOutput {
			toGoStr = toGoStr.AddChild(byteVal)
		}
		require.True(t, toGoStr.SetData(rAsBytes))
	}

	// Add all codepoints to the constructor
	charsetToGoIter := charsetToGoString.Iterator()
	rangeMapConstructor := utils.NewRangeMapConstructor()
	for inputEncoding, outputEncoding, ok := charsetToGoIter.Next(); ok; inputEncoding, outputEncoding, ok = charsetToGoIter.Next() {
		rangeMapConstructor.AddValidEncoding(inputEncoding, outputEncoding)
	}
	rangeMap := rangeMapConstructor.Map()

	// Verify that the range map returns the correct results for all valid inputs
	charsetToGoIter = charsetToGoString.Iterator()
	for inputEncoding, outputEncoding, ok := charsetToGoIter.Next(); ok; inputEncoding, outputEncoding, ok = charsetToGoIter.Next() {
		generatedOutputEncoding, ok := rangeMap.Decode(inputEncoding)
		if assert.True(t, ok) {
			assert.Equal(t, outputEncoding, generatedOutputEncoding, "Decode\ninput: '%c', expected output: '%c', actual output: '%c'",
				[]rune(string(inputEncoding))[0], []rune(string(outputEncoding))[0], []rune(string(generatedOutputEncoding))[0])
		}
		generatedInputEncoding, ok := rangeMap.Encode(outputEncoding)
		if assert.True(t, ok) {
			assert.Equal(t, inputEncoding, generatedInputEncoding, "Encode\ninput: '%c', expected output: '%c', actual output: '%c'",
				[]rune(string(outputEncoding))[0], []rune(string(inputEncoding))[0], []rune(string(generatedInputEncoding))[0])
		}
	}

	// Grab the uppercase and lowercase conversions (case conversions may be asymmetric, so we have to test them individually)
	iter.Reset()
	var toUpper [][2]rune
	var toLower [][2]rune
	for r, ok := iter.Next(); ok; r, ok = iter.Next() {
		// First we'll do the uppercase conversion
		rAsBytes := []byte(string(r))
		sqlOutput, err := conn.Query(fmt.Sprintf(`SELECT CAST(CONVERT(UPPER(CONVERT(_utf8mb4 0x%s USING %s)) USING utf8mb4) AS BINARY);`,
			hex.EncodeToString(rAsBytes), TestExtractCharacterSet_charset))
		require.NoError(t, err)
		// The output should be equivalent to a single rune
		outputAsRune := []rune(string(sqlOutput))[0]
		if assert.True(t, utf8.RuneCountInString(string(sqlOutput)) == 1 && utf8.ValidRune(outputAsRune)) && r != outputAsRune {
			toUpper = append(toUpper, [2]rune{r, outputAsRune})
		}

		// Afterward we do the lowercase conversion
		sqlOutput, err = conn.Query(fmt.Sprintf(`SELECT CAST(CONVERT(LOWER(CONVERT(_utf8mb4 0x%s USING %s)) USING utf8mb4) AS BINARY);`,
			hex.EncodeToString(rAsBytes), TestExtractCharacterSet_charset))
		require.NoError(t, err)
		outputAsRune = []rune(string(sqlOutput))[0]
		if assert.True(t, utf8.RuneCountInString(string(sqlOutput)) == 1 && utf8.ValidRune(outputAsRune)) && r != outputAsRune {
			toLower = append(toLower, [2]rune{r, outputAsRune})
		}
	}

	// Write the output to a file
	file, err := os.OpenFile(TestExtractCharacterSet_file, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	defer file.Close()
	_, err = file.WriteString(utils.RangeMapToGoFile(rangeMap, toUpper, toLower, TestExtractCharacterSet_charset))
	require.NoError(t, err)
	err = file.Sync()
	require.NoError(t, err)
}

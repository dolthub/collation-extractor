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
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/collation-extractor/utils"
)

const (
	TestExtractCollation_user      = "root"
	TestExtractCollation_password  = "password"
	TestExtractCollation_host      = "localhost"
	TestExtractCollation_port      = 3306
	TestExtractCollation_collation = "utf16_unicode_ci"
	TestExtractCollation_file      = "./" + TestExtractCollation_collation + ".go.txt"
)

// TestExtractCollation creates a Go file for embedding into GMS. It contains the data necessary to sort and compare
// strings based on the specified collation. May take up to 120 minutes depending on the complexity of the collation.
func TestExtractCollation(t *testing.T) {
	// All collations start with the character set followed by an underscore
	charset := strings.Split(TestExtractCollation_collation, "_")[0]

	iter := utils.NewUTF8Iter()
	conn, err := utils.NewConnection(TestExtractCollation_user, TestExtractCollation_password, TestExtractCollation_host, TestExtractCollation_port)
	require.NoError(t, err)
	defer conn.Close()
	// The RangeMap allows us to check that a rune is valid in the character set, so that we may skip over invalid runes
	rangeMap := CharacterSetToRangeMap(t, conn, charset)

	// This is a map that takes a rune as an input and return the weight, which is represented as a byte slice. MySQL
	// encodes weights as binary strings, and they cannot be converted to unsigned integers due to their length (which
	// can be over the 8 byte limit of a 64-bit integer).
	runeToWeight := make(map[rune][]byte)
	runeComparator := utils.NewRuneComparator()
	// The comparator returns the relative sorting order of any two given runes
	runeComparator.SetComparator(func(l rune, r rune) int {
		// If we have the weights for both of the runes then we may use those for comparison
		lWeight, lOk := runeToWeight[l]
		rWeight, rOk := runeToWeight[r]
		if lOk && rOk {
			return bytes.Compare(lWeight, rWeight)
		}

		// Without the weights, we can resort to using MySQL's STRCMP to get a comparison. Check the "for" loop below
		// for details on our byte slices and hex encoding usage here.
		lAsBytes := []byte(string(l))
		rAsBytes := []byte(string(r))
		sqlOutput, err := conn.Query(fmt.Sprintf(
			"SELECT STRCMP(CONVERT(_utf8mb4 0x%s USING %s) COLLATE %s, CONVERT(_utf8mb4 0x%s USING %s) COLLATE %s);",
			hex.EncodeToString(lAsBytes), charset, TestExtractCollation_collation,
			hex.EncodeToString(rAsBytes), charset, TestExtractCollation_collation))
		require.NoError(t, err)
		switch string(sqlOutput) {
		case "1":
			return 1
		case "-1":
			return -1
		case "0":
			// If they're comparably equivalent and one has a weight, we can assign the other the same weight to
			// potentially save time on future comparisons
			if lOk && !rOk {
				runeToWeight[r] = lWeight
			} else if !lOk && rOk {
				runeToWeight[l] = rWeight
			}
			return 0
		default:
			t.Fatalf("unknown output `%s` for comparing '%s' (%d) and '%s' (%d)", string(sqlOutput), string(l), l, string(r), r)
			return 0 // Won't actually be reached due to the above call, needed to compile
		}
	})

	for r, ok := iter.Next(); ok; r, ok = iter.Next() {
		// Ensure that this rune is a valid character in the character set, as we only want to process valid runes
		_, ok := rangeMap.Encode([]byte(string(r)))
		if !ok {
			continue
		}

		// Converting a rune to a string will encode the rune (which is an int32) as a sequence of valid UTF8 bytes.
		// We then convert it to a byte slice to pass to the hex encoder.
		rAsBytes := []byte(string(r))
		// We convert the string to a hexadecimal to ensure that Go's exact byte representation is being given to MySQL.
		// This also allows us to bypass escape rules.
		sqlOutput, err := conn.Query(fmt.Sprintf(
			"SELECT HEX(WEIGHT_STRING(CONVERT(_utf8mb4 0x%s USING %s) COLLATE %s));",
			hex.EncodeToString(rAsBytes), charset, TestExtractCollation_collation))
		require.NoError(t, err)
		// The output is the sorting weight of the character. Lower weights sort before higher weights. The weight
		// is encoded as a binary string. WEIGHT_STRING is explicitly defined as not guaranteeing a stable output
		// between versions, but it will always return the proper relative weights if a weight is returned. For an
		// unknown reason, some characters do not return a weight, but still have a sort order, and such cases are
		// handled during comparisons.
		if len(sqlOutput) > 0 {
			runeToWeight[r] = sqlOutput
		}
		runeComparator.Insert(r)
	}

	// Write the output to a file
	file, err := os.OpenFile(TestExtractCollation_file, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	defer file.Close()
	_, err = file.WriteString(utils.RuneComparatorToGoFile(runeComparator, TestExtractCollation_collation))
	require.NoError(t, err)
	err = file.Sync()
	require.NoError(t, err)
}

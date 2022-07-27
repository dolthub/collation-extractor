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
	"strconv"
	"strings"
	"time"
)

// RangeMap is used to transcode from one encoding to another. During its construction from a RangeMapConstructor, one
// encoding was given as the input and the other as the output. It is assumed that the output encoding is the target
// encoding, therefore we decode the input to obtain the output. Encoding is bidirectional, therefore you can encode
// the output to obtain the original input.
type RangeMap struct {
	inputEntries  [][]rangeMapEntry
	outputEntries [][]rangeMapEntry
}

// rangeMapEntry is an entry within a RangeMap, which represents a range of valid inputs along with the possible
// outputs, along with the multiplier for each byte position.
type rangeMapEntry struct {
	inputRange  rangeBounds
	outputRange rangeBounds
	inputMults  []int
	outputMults []int
}

// Decode converts from the input encoding to the output encoding for the given data.
func (rm *RangeMap) Decode(data []byte) ([]byte, bool) {
	if len(data) > len(rm.inputEntries) {
		return nil, false
	}
	for _, entry := range rm.inputEntries[len(data)-1] {
		if entry.inputRange.contains(data) {
			outputData := make([]byte, len(entry.outputRange))
			increase := 0
			for i := len(entry.inputRange) - 1; i >= 0; i-- {
				increase += int(data[i]-entry.inputRange[i][0]) * entry.inputMults[i]
			}
			for i := 0; i < len(outputData); i++ {
				diff := increase / entry.outputMults[i]
				outputData[i] = entry.outputRange[i][0] + byte(diff)
				increase -= diff * entry.outputMults[i]
			}
			return outputData, true
		}
	}
	return nil, false
}

// Encode converts from the output encoding to the input encoding for the given data.
func (rm *RangeMap) Encode(data []byte) ([]byte, bool) {
	if len(data) > len(rm.outputEntries) {
		return nil, false
	}
	for _, entry := range rm.outputEntries[len(data)-1] {
		if entry.outputRange.contains(data) {
			inputData := make([]byte, len(entry.inputRange))
			increase := 0
			for i := len(entry.outputRange) - 1; i >= 0; i-- {
				increase += int(data[i]-entry.outputRange[i][0]) * entry.outputMults[i]
			}
			for i := 0; i < len(inputData); i++ {
				diff := increase / entry.inputMults[i]
				inputData[i] = entry.inputRange[i][0] + byte(diff)
				increase -= diff * entry.inputMults[i]
			}
			return inputData, true
		}
	}
	return nil, false
}

// RangeMapToGoFile returns the given RangeMap as a Go file for inclusion in an application.
func RangeMapToGoFile(rm *RangeMap, toUpper [][2]rune, toLower [][2]rune) string {
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf(`// Copyright %d Dolthub, Inc.
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

package nonexistentpackagename

//TODO: Change the package name, rename this variable, and add comment for documentation of the variable
var outputRangeMap = RangeMap{
	inputEntries: [][]rangeMapEntry{
`, time.Now().Year()))
	for _, entryLength := range rm.inputEntries {
		if len(entryLength) == 0 {
			sb.WriteString("\t\tnil,\n")
			continue
		}
		sb.WriteString("\t\t{\n")
		for _, entry := range entryLength {
			sb.WriteString(rm.entryToGoFile(entry))
		}
		sb.WriteString("\t\t},\n")
	}
	sb.WriteString(`	},
	outputEntries: [][]rangeMapEntry{
`)
	for _, entryLength := range rm.outputEntries {
		if len(entryLength) == 0 {
			sb.WriteString("\t\tnil,\n")
			continue
		}
		sb.WriteString("\t\t{\n")
		for _, entry := range entryLength {
			sb.WriteString(rm.entryToGoFile(entry))
		}
		sb.WriteString("\t\t},\n")
	}
	sb.WriteString(`	},
	toUpper: map[rune]rune{
`)
	for _, runes := range toUpper {
		sb.WriteString(fmt.Sprintf("\t\t%d: %d,\n", runes[0], runes[1]))
	}
	sb.WriteString(`	},
	toLower: map[rune]rune{
`)
	for _, runes := range toLower {
		sb.WriteString(fmt.Sprintf("\t\t%d: %d,\n", runes[0], runes[1]))
	}
	sb.WriteString(`	},
}
`)
	return sb.String()
}

func (*RangeMap) entryToGoFile(rme rangeMapEntry) string {
	inputMults := make([]string, len(rme.inputMults))
	outputMults := make([]string, len(rme.outputMults))
	for i, mult := range rme.inputMults {
		inputMults[i] = strconv.FormatInt(int64(mult), 10)
	}
	for i, mult := range rme.outputMults {
		outputMults[i] = strconv.FormatInt(int64(mult), 10)
	}
	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf(`			{
				inputRange:  %s,
				outputRange: %s,
				inputMults:  []int{%s},
				outputMults: []int{%s},
			},
`, rme.inputRange.goString(), rme.outputRange.goString(), strings.Join(inputMults, ", "), strings.Join(outputMults, ", ")))
	return sb.String()
}

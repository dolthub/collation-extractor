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
	"strings"
)

// RangeMapConstructor is used to construct a RangeMap, which will be used to find all range mappings from the input
// encoding to the output encoding.
type RangeMapConstructor struct {
	inputEnc  []rangeBounds
	outputEnc []rangeBounds
}

// rangeBounds represents the minimum and maximum values for each section of this specific range. The byte at index 0
// represents the minimum, while the byte at index 1 represents the maximum.
type rangeBounds [][2]byte

// NewRangeMapConstructor returns a new RangeMapConstructor.
func NewRangeMapConstructor() *RangeMapConstructor {
	return &RangeMapConstructor{}
}

// AddValidEncoding adds the given codepoints to the constructor. It is assumed that these two codepoints are equivalent
// in their respective encodings. It is also assumed that all codepoints are given in sorted order, whether that be
// ascending or descending. Lastly, it does not matter if the sorted codepoints start with the shortest of longest
// slice lengths. It only matters that all codepoints of a specific length are given before any other lengths are seen.
// All codepoints returned by CharacterSetEncodingIterator will be in the correct order, and is the recommended way to
// populate the constructor.
func (rc *RangeMapConstructor) AddValidEncoding(inputCodepoint []byte, outputCodepoint []byte) {
	if len(inputCodepoint) == 0 {
		return
	}
	newInputRange := make(rangeBounds, len(inputCodepoint))
	newOutputRange := make(rangeBounds, len(outputCodepoint))
	for i, val := range inputCodepoint {
		newInputRange[i] = [2]byte{val, val}
	}
	for i, val := range outputCodepoint {
		newOutputRange[i] = [2]byte{val, val}
	}
	rc.inputEnc = append(rc.inputEnc, newInputRange)
	rc.outputEnc = append(rc.outputEnc, newOutputRange)
}

// Map creates a RangeMap based on the codepoints given to this constructor.
func (rc *RangeMapConstructor) Map() *RangeMap {
	// We consolidate the ranges as we want to iterate through as few ranges as possible
	rc.consolidateRanges()
	// Largest encoding has a length of 4, so we set that here.
	rm := &RangeMap{make([][]rangeMapEntry, 4), make([][]rangeMapEntry, 4)}
	for rangeIdx, inputRange := range rc.inputEnc {
		outputRange := rc.outputEnc[rangeIdx]
		// Multipliers are equivalent to powers in a traditional number encoding. Let's use binary for example. The
		// least significant bit has a multiplier of 1. Three bits over and now that bit has a multiplier of 8. The same
		// core principle is applied here. Unlike in binary (and other standard numeric systems), we calculate the
		// multiplier based on the range. If the least significant byte (position 0) has a range of 20, then the next
		// byte (position 1) will have a multiplier of 20, as incrementing position 1 once is equivalent to adding
		// 20 to position 0. If position 1 has a range of 30, then position 2 has a multiplier of 600, which is 20 * 30.
		inputMults := make([]int, len(inputRange))
		outputMults := make([]int, len(outputRange))
		// The least significant byte has a multiplier of 1, therefore we start with 1
		mult := 1
		for i := len(inputRange) - 1; i >= 0; i-- {
			inputMults[i] = mult
			// We add 1 as we're using the number of valid values. If both the min and max are the same number then we
			// still have a single valid value.
			mult *= int(inputRange[i][1]-inputRange[i][0]) + 1
		}
		mult = 1
		for i := len(outputRange) - 1; i >= 0; i-- {
			outputMults[i] = mult
			mult *= int(outputRange[i][1]-outputRange[i][0]) + 1
		}

		entry := rangeMapEntry{
			inputRange:  inputRange,
			outputRange: outputRange,
			inputMults:  inputMults,
			outputMults: outputMults,
		}
		rm.inputEntries[len(inputRange)-1] = append(rm.inputEntries[len(inputRange)-1], entry)
		rm.outputEntries[len(outputRange)-1] = append(rm.outputEntries[len(outputRange)-1], entry)
	}
	return rm
}

// consolidateRanges is a highly inefficient way of reducing the number of ranges down to the absolute minimum. This
// loops repeatedly over newly created slices until no changes are made, similar to bubble sort. Although it's terrible,
// it works, and computers are fast enough that this takes only milliseconds (and only needs to run once).
//
// On each loop, we compare the next range set with the previous range set (both input and output). If both sets of
// ranges have only a single difference (or no differences), then we merge the current range set with the previous range
// set. If there are multiple differences, then we add the new range set. Differences represent changes that may be
// merged. Too many differences and the ranges are not mergeable. This ensures that there is a sequential mapping
// between the input and the output.
func (rc *RangeMapConstructor) consolidateRanges() {
	loop := true
	for loop {
		loop = false
		var newInputRanges []rangeBounds
		var newOutputRanges []rangeBounds
		for rangeIdx := 0; rangeIdx < len(rc.inputEnc); rangeIdx++ {
			currentInputRange := rc.inputEnc[rangeIdx]
			currentOutputRange := rc.outputEnc[rangeIdx]
			if len(newInputRanges) == 0 {
				newInputRanges = append(newInputRanges, currentInputRange)
				newOutputRanges = append(newOutputRanges, currentOutputRange)
				continue
			}
			lastInputRange := newInputRanges[len(newInputRanges)-1]
			lastOutputRange := newOutputRanges[len(newOutputRanges)-1]
			inputDifferences := lastInputRange.differences(currentInputRange)
			outputDifferences := lastOutputRange.differences(currentOutputRange)
			if inputDifferences <= 1 && outputDifferences <= 1 {
				lastInputRange.merge(currentInputRange)
				lastOutputRange.merge(currentOutputRange)
				loop = true
				continue
			} else {
				newInputRanges = append(newInputRanges, currentInputRange)
				newOutputRanges = append(newOutputRanges, currentOutputRange)
				continue
			}
		}
		rc.inputEnc = newInputRanges
		rc.outputEnc = newOutputRanges
	}
}

// boundsContains returns whether the right bounds are contained within the left bounds.
func (rangeBounds) boundsContains(l [2]byte, r [2]byte) bool {
	return l[0] <= r[0] && l[1] >= r[1]
}

// boundsAdjacent returns whether the right bounds are adjacent to the left bounds.
func (rangeBounds) boundsAdjacent(l [2]byte, r [2]byte) bool {
	return ((l[1]+1) == r[0] && l[1] != 255) || ((r[1]+1) == l[0] && r[1] != 255)
}

// boundsMinMax returns new bounds with the minimum and maximum values between both bounds.
func (rangeBounds) boundsMinMax(l [2]byte, r [2]byte) [2]byte {
	out := [2]byte{l[0], l[1]}
	if r[0] < out[0] {
		out[0] = r[0]
	}
	if r[1] > out[1] {
		out[1] = r[1]
	}
	return out
}

// contains returns whether the data falls within the range bounds. Assumes that the length of the data matches the
// length of the range bounds.
func (r rangeBounds) contains(data []byte) bool {
	for i := 0; i < len(r); i++ {
		if r[i][0] > data[i] || r[i][1] < data[i] {
			return false
		}
	}
	return true
}

// differences returns the number of allowable differences between the given range bounds and the calling range bounds.
// As we only merge on a single difference (or no differences), if the two range bounds are completely incompatible
// (such as not being adjacent or having different lengths) then we return a value of 2.
func (r rangeBounds) differences(other rangeBounds) int {
	if len(r) != len(other) {
		return 2
	}
	differences := 0
	for sectionIdx := 0; sectionIdx < len(r); sectionIdx++ {
		if !r.boundsContains(r[sectionIdx], other[sectionIdx]) {
			if r.boundsAdjacent(r[sectionIdx], other[sectionIdx]) {
				differences++
				continue
			} else {
				differences = 2
				break
			}
		}
	}
	return differences
}

// merge modifies the calling range bounds by setting its minimum and/or maximum to the given range bounds if they're
// more extreme than the current ones.
func (r rangeBounds) merge(other rangeBounds) {
	for sectionIdx := 0; sectionIdx < len(r); sectionIdx++ {
		r[sectionIdx] = r.boundsMinMax(r[sectionIdx], other[sectionIdx])
	}
}

// goString returns the range bounds as a string that would be valid in a Go application.
func (r rangeBounds) goString() string {
	sections := make([]string, len(r))
	for i := 0; i < len(sections); i++ {
		sections[i] = fmt.Sprintf("{%d, %d}", r[i][0], r[i][1])
	}
	return fmt.Sprintf("rangeBounds{%s}", strings.Join(sections, ", "))
}

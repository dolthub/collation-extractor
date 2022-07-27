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
	"time"
)

// RuneComparator stores runes by their relative weights, such that any rune may be compared to any other rune. This is
// useful for generating code that collations will depend on.
type RuneComparator struct {
	// The index of values is used as the weight. All runes on the same index (belonging to the same rune slice) have
	// the same weight. A greater weight (higher index) sorts after a lower weight. Rune slices are shuffled around to
	// preserve this relational ordering. This results in fairly expensive operations, which are a non-issue for our
	// use case.
	values     [][]rune
	comparator func(l rune, r rune) int
}

// NewRuneComparator returns a new RuneComparator.
func NewRuneComparator() *RuneComparator {
	return &RuneComparator{}
}

// Insert adds the given rune, calling the comparator to determine where to place it. SetComparator must be called
// before Insert is called, else a panic will occur. This assumes that runes are given in sequential order, which is
// necessary for file generation.
func (rc *RuneComparator) Insert(r rune) {
	if len(rc.values) == 0 {
		rc.values = append(rc.values, []rune{r})
		return
	}

	low := 0
	high := len(rc.values) - 1
	for high-low > 0 {
		mid := (high + low) / 2
		comp := rc.comparator(r, rc.values[mid][0])
		switch comp {
		case 1:
			low = mid + 1
		case -1:
			high = mid
		case 0:
			rc.values[mid] = append(rc.values[mid], r)
			return
		}
	}
	switch rc.comparator(r, rc.values[low][0]) {
	case 1:
		rc.insertNewRow(r, low+1)
	case -1:
		rc.insertNewRow(r, low)
	case 0:
		rc.values[low] = append(rc.values[low], r)
	}
}

// SetComparator sets the comparator that will be used during insertion. This must be set before Insert is called, else
// a panic will occur.
func (rc *RuneComparator) SetComparator(comparator func(l rune, r rune) int) {
	rc.comparator = comparator
}

// RuneComparatorToGoFile returns the given RuneComparator as a Go file for inclusion in an application.
func RuneComparatorToGoFile(rc *RuneComparator) string {
	// This struct is used only in this function, so we can avoid polluting the package
	type WeightRange struct {
		Weight int
		Lower  rune
		Upper  rune
	}

	fileSb := strings.Builder{}
	fileSb.WriteString(fmt.Sprintf(`// Copyright %d Dolthub, Inc.
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

//TODO: Change the package name, rename this function, and add comment for documentation of the function
func getRuneWeight(r rune) int32 {
	weight, ok := outputWeights[r]
	if ok {
		return weight
	}`, time.Now().Year()))
	mapSb := strings.Builder{}
	mapSb.WriteString("var outputWeights = map[rune]int32{\n")
	for weight, row := range rc.values {
		var rowWeightRanges []WeightRange
		for _, r := range row {
			if len(rowWeightRanges) == 0 {
				rowWeightRanges = append(rowWeightRanges, WeightRange{
					Weight: weight,
					Lower:  r,
					Upper:  r,
				})
				continue
			}
			if rowWeightRanges[len(rowWeightRanges)-1].Upper+1 == r {
				rowWeightRanges[len(rowWeightRanges)-1].Upper = r
				continue
			} else {
				rowWeightRanges = append(rowWeightRanges, WeightRange{
					Weight: weight,
					Lower:  r,
					Upper:  r,
				})
				continue
			}
		}

		// We either make map entries or a range entry for any given weight range
		for _, rowWeightRange := range rowWeightRanges {
			// Cutoff point that determines whether we do a range comparison or a map comparison. Decision is arbitrary.
			if rowWeightRange.Upper-rowWeightRange.Lower >= 25 {
				fileSb.WriteString(fmt.Sprintf(" else if r >= %d && r <= %d {\n\t\treturn %d\n\t}",
					rowWeightRange.Lower, rowWeightRange.Upper, rowWeightRange.Weight))
			} else {
				for i := rowWeightRange.Lower; i <= rowWeightRange.Upper; i++ {
					mapSb.WriteString(fmt.Sprintf("\t%d: %d,\n", i, weight))
				}
			}
		}
	}
	mapSb.WriteString("}\n")
	fileSb.WriteString(fmt.Sprintf(` else {
		return 2147483647
	}
}

//TODO: Rename this variable, and add comment for documentation of the variable
%s`, mapSb.String()))
	return fileSb.String()
}

// insertNewRow inserts a new row at the given index (containing the given rune as its only element) while pushing back
// the row already at that index (if one exists).
func (rc *RuneComparator) insertNewRow(r rune, idx int) {
	// If we're inserting after the last element then we may append
	if idx == len(rc.values) {
		rc.values = append(rc.values, []rune{r})
		return
	}

	// To insert at the given index, we create a new array while copying the old data into the correct positions.
	// This is highly inefficient, but we're aiming for correctness not efficiency.
	newValues := make([][]rune, len(rc.values)+1)
	copy(newValues, rc.values[:idx])
	newValues[idx] = []rune{r}
	copy(newValues[idx+1:], rc.values[idx:])
	rc.values = newValues
}

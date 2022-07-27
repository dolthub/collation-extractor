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
	"math"
	"unicode/utf8"
)

// UTF8Iter iterates over the entire valid range of unicode characters that Go supports.
type UTF8Iter struct {
	r     rune
	count int
	limit int
}

// NewUTF8Iter returns a new UTF8Iter.
func NewUTF8Iter() *UTF8Iter {
	// Negative numbers do not represent any valid runes so we start at 0.
	return &UTF8Iter{0, 0, math.MaxInt32}
}

// Next returns the next sequential rune. Returns false if there are no more runes to iterate through.
func (iter *UTF8Iter) Next() (rune, bool) {
	// Surrogates were taken from the `ValidRune` function in the `utf8` package
	const utf8SurrogateMin = 0xD800
	const utf8SurrogateMax = 0xDFFF
	// We return once we've reached the limit
	if iter.count >= iter.limit {
		return 0, false
	}
	// Negative numbers do not represent any valid runes
	if iter.r > utf8.MaxRune {
		return 0, false
	}
	if utf8SurrogateMin <= iter.r && iter.r <= utf8SurrogateMax {
		iter.r = utf8SurrogateMax + 1
	}
	iter.r++
	iter.count++
	return iter.r - 1, true
}

// SetIteratorLimit limits the number of returned values from the iterator. Useful for testing a smaller sample size
// (improving iteration speed).
func (iter *UTF8Iter) SetIteratorLimit(limit int) {
	iter.limit = limit
}

// MaxRune returns the rune with the highest valid value (as rune is an alias for `int32`).
func (iter *UTF8Iter) MaxRune() rune {
	return utf8.MaxRune
}

// Reset returns the iterator to its initial state.
func (iter *UTF8Iter) Reset() {
	iter.r = 0
	iter.count = 0
}

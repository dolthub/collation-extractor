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
	"sort"
)

// CharacterSetEncodingTree represents a character set's encoding. Leafs contain data, therefore a character may be
// decoded by processing bytes until data is found (or not further trees were returned, indicating an invalid byte
// sequence).
type CharacterSetEncodingTree struct {
	data  []byte
	nodes map[byte]*CharacterSetEncodingTree
	min   byte
	max   byte
}

// CharacterSetEncodingContinuation is used to control exactly when the tree continues its search. This allows for
// proper code generation.
type CharacterSetEncodingContinuation struct {
	tree      *CharacterSetEncodingTree
	depth     int
	inputFunc func(continuation CharacterSetEncodingContinuation, depth int, hasData bool, val byte, data []byte) error
}

// CharacterSetEncodingIterator iterates through a CharacterSetEncodingTree, returning all valid encodings. The
// encodings are ordered from shortest to longest (byte slice length), and also in ascending order.
type CharacterSetEncodingIterator struct {
	trees    []*CharacterSetEncodingTree
	progress []int
	depth    int
}

// NewCharacterSetEncodingTree returns a new CharacterSetEncodingTree.
func NewCharacterSetEncodingTree() *CharacterSetEncodingTree {
	return &CharacterSetEncodingTree{
		data:  nil,
		nodes: make(map[byte]*CharacterSetEncodingTree),
	}
}

// AddChild adds the given value to the tree, returning the newly created subtree (or, if the subtree already existed,
// the existing subtree).
func (cset *CharacterSetEncodingTree) AddChild(val byte) *CharacterSetEncodingTree {
	if len(cset.nodes) == 0 {
		cset.min = val
		cset.max = val
	} else if val < cset.min {
		cset.min = val
	} else if val > cset.max {
		cset.max = val
	}
	if subtree, ok := cset.nodes[val]; ok {
		return subtree
	}
	child := &CharacterSetEncodingTree{
		data:  nil,
		nodes: make(map[byte]*CharacterSetEncodingTree),
	}
	cset.nodes[val] = child
	return child
}

// SetData sets this tree's data to the given data. Returns false if this tree has subtrees, or data was set previously.
func (cset *CharacterSetEncodingTree) SetData(data []byte) bool {
	if len(cset.nodes) > 0 || cset.data != nil {
		return false
	}
	cset.data = data
	return true
}

// Child returns the subtree belonging to the given value. If the value has no subtree, then nil is returned.
func (cset *CharacterSetEncodingTree) Child(val byte) *CharacterSetEncodingTree {
	if cset == nil || cset.nodes == nil {
		return nil
	}
	return cset.nodes[val]
}

// Data returns the data contained in this tree. Data will only be present if there are no subtrees, and also if data
// was previously set to this tree.
func (cset *CharacterSetEncodingTree) Data() []byte {
	if cset == nil {
		return nil
	}
	return cset.data
}

// Iterator returns a CharacterSetEncodingIterator that will iterate over this CharacterSetEncodingTree, returning all
// valid encodings. The encodings are ordered from shortest to longest (byte slice length), and also in ascending order.
func (cset *CharacterSetEncodingTree) Iterator() *CharacterSetEncodingIterator {
	csei := &CharacterSetEncodingIterator{
		trees:    make([]*CharacterSetEncodingTree, 1, 4),
		progress: make([]int, 1, 4),
		depth:    0,
	}
	csei.trees[0] = cset
	csei.progress[0] = int(cset.min)
	return csei
}

// DFS iterates through the tree using depth-first-search, while passing each subtree's information to the given
// function. The root tree (i.e. the one that this function is being called on) has a depth of 0, and also has a value
// of 0 (as there is no value associated with the root). This iterates through the subtrees sorted by their value
// (ascending).
//
// This function is intended for code generation. It is the duty of the given function to continue the search by
// calling `Continue` on the continuation struct. This approach was chosen as nested code is often surrounded by outer
// code. For example, the contents of an `if` statement are preceded by the `if` keyword, condition, and opening brace,
// and proceeded by the closing brace. The code that handles the opening and closing braces would need to specify
// exactly when to continue with the contents of the `if` statement.
func (cset *CharacterSetEncodingTree) DFS(
	inputFunc func(continuation CharacterSetEncodingContinuation, depth int, hasData bool, val byte, data []byte) error) error {
	continuation := CharacterSetEncodingContinuation{
		tree:      cset,
		depth:     0,
		inputFunc: inputFunc,
	}
	return inputFunc(continuation, 0, cset.data != nil && len(cset.nodes) == 0, 0, cset.data)
}

// dfs is the inner function of DFS that actually handles the recursive logic.
func (cset *CharacterSetEncodingTree) dfs(currentDepth int,
	inputFunc func(continuation CharacterSetEncodingContinuation, depth int, hasData bool, val byte, data []byte) error) error {
	type subtree struct {
		val  byte
		tree *CharacterSetEncodingTree
	}
	sortedSubtrees := make([]subtree, 0, len(cset.nodes))
	for val, tree := range cset.nodes {
		sortedSubtrees = append(sortedSubtrees, subtree{val, tree})
	}
	sort.Slice(sortedSubtrees, func(i, j int) bool {
		return sortedSubtrees[i].val < sortedSubtrees[j].val
	})
	for _, st := range sortedSubtrees {
		continuation := CharacterSetEncodingContinuation{
			tree:      st.tree,
			depth:     currentDepth + 1,
			inputFunc: inputFunc,
		}
		err := inputFunc(continuation, currentDepth+1, st.tree.data != nil && len(st.tree.nodes) == 0, st.val, st.tree.data)
		if err != nil {
			return err
		}
	}
	return nil
}

// Continue continues the search.
func (cont *CharacterSetEncodingContinuation) Continue() error {
	return cont.tree.dfs(cont.depth, cont.inputFunc)
}

// Next returns the next sequential encoding. The first returned byte slice represents the input encoding that was used
// during the original tree's construction. The second returned byte slice represents the data that was given to leafs.
// Returns false if there are no more encodings to iterate through.
func (csei *CharacterSetEncodingIterator) Next() (inputEncoding []byte, outputEncoding []byte, ok bool) {
	// Iteration works in a few steps:
	// 1) Check the depth. If it is beyond the maximum possible encoding length (currently 4), then we return.
	// 2) Check if the progress on the current level is beyond the max valid encoding.
	//    a) If we are not at level zero, then we decrement our level is increment that level's progress.
	//    b) If we are at level zero, we increment the depth requirement and reset our progress.
	// 3) Grab the next valid subtree.
	//    a) If our level matches the depth then we check for data, returning if its found (in addition to incrementing
	//       the progress for the next loop). Otherwise, we just increment our progress.
	//    b) If our level is less than the depth, then we add a new level with the found subtree.
	for true {
		// Largest encoding is 4 bytes deep, so we can immediately return if we've gone beyond that
		if csei.depth >= 4 {
			return nil, nil, false
		}
		depth := csei.depth
		level := len(csei.trees) - 1
		tree := csei.trees[level]
		progress := csei.progress[level]

		// Here we check if the progress is beyond the max
		if progress > int(tree.max) {
			// Level 0 is as low as we can go, so we increment the depth instead
			if level == 0 {
				csei.depth++
				csei.progress[level] = int(tree.min)
				continue
			} else {
				csei.trees = csei.trees[:level]
				csei.progress = csei.progress[:level]
				csei.progress[level-1]++
				continue
			}
		}

		subtree, ok := tree.nodes[byte(progress)]
		if !ok {
			csei.progress[level]++
			continue
		}
		if level == depth {
			// Since the level matches the depth, we're checking for data (which will be leafs)
			if subtree.data != nil && len(subtree.nodes) == 0 {
				inputEncoding = make([]byte, len(csei.progress))
				for i := 0; i < len(inputEncoding); i++ {
					inputEncoding[i] = byte(csei.progress[i])
				}
				outputEncoding = make([]byte, len(subtree.data))
				copy(outputEncoding, subtree.data)
				csei.progress[level]++
				return inputEncoding, outputEncoding, true
			} else {
				csei.progress[level]++
				continue
			}
		} else {
			// Level is less than the depth
			if len(subtree.nodes) == 0 {
				// Our current subtree on this level has no subtrees of its own, so we increment our progress
				csei.progress[level]++
				continue
			} else {
				csei.trees = append(csei.trees, subtree)
				csei.progress = append(csei.progress, int(subtree.min))
				continue
			}
		}
	}
	return nil, nil, false
}

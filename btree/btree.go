package btree

import (
	"bytes"
	
	"github.com/JoshuaLim25/db"
)

// BTree represents a B+Tree structure
type BTree struct {
	root *Node
	size int
}

// New creates a new B+Tree
func New() *BTree {
	return &BTree{
		root: NewLeafNode(),
		size: 0,
	}
}

// Get retrieves a value by key
func (bt *BTree) Get(key []byte) (val []byte, ok bool) {
	if bt.root == nil {
		return nil, false
	}
	
	leaf := bt.findLeaf(key)
	index := bt.findKeyIndex(leaf, key)
	
	if index >= 0 && index < leaf.NumKeys && bytes.Equal(leaf.KeyAt(index), key) {
		return leaf.ValueAt(index), true
	}
	
	return nil, false
}

// Set inserts or updates a key-value pair
func (bt *BTree) Set(key, val []byte) {
	if bt.root == nil {
		bt.root = NewLeafNode()
	}
	
	leaf := bt.findLeaf(key)
	index := bt.findKeyIndex(leaf, key)
	
	// If key exists, update the value
	if index >= 0 && index < leaf.NumKeys && bytes.Equal(leaf.KeyAt(index), key) {
		leaf.Values[index] = val
		return
	}
	
	// Insert new key-value pair
	bt.insertIntoLeaf(leaf, key, val)
	bt.size++
}

// Delete removes a key-value pair
func (bt *BTree) Delete(key []byte) {
	if bt.root == nil {
		return
	}
	
	leaf := bt.findLeaf(key)
	index := bt.findKeyIndex(leaf, key)
	
	if index >= 0 && index < leaf.NumKeys && bytes.Equal(leaf.KeyAt(index), key) {
		bt.deleteFromLeaf(leaf, index)
		bt.size--
	}
}

// FindLarger returns an iterator for keys larger than the given key
func (bt *BTree) FindLarger(key []byte) db.Iterator {
	if bt.root == nil {
		return &BTreeIterator{current: nil, index: 0}
	}
	
	leaf := bt.findLeaf(key)
	index := bt.findKeyIndex(leaf, key)
	
	// Find the first key larger than the given key
	for index < leaf.NumKeys && bytes.Compare(leaf.KeyAt(index), key) <= 0 {
		index++
	}
	
	// If we've gone past the end of this leaf, move to next leaf
	if index >= leaf.NumKeys {
		leaf = leaf.Next
		index = 0
	}
	
	return &BTreeIterator{current: leaf, index: index}
}

// findLeaf navigates to the leaf node that should contain the given key
func (bt *BTree) findLeaf(key []byte) *Node {
	current := bt.root
	
	for current != nil && !current.IsLeaf() {
		index := bt.findChildIndex(current, key)
		child := current.ChildAt(index)
		if child == nil {
			break
		}
		current = child
	}
	
	return current
}

// findKeyIndex finds the position where key should be in the node
func (bt *BTree) findKeyIndex(node *Node, key []byte) int {
	if node == nil {
		return 0
	}
	
	for i := 0; i < node.NumKeys; i++ {
		nodeKey := node.KeyAt(i)
		if nodeKey == nil {
			continue
		}
		cmp := bytes.Compare(key, nodeKey)
		if cmp <= 0 {
			return i
		}
	}
	return node.NumKeys
}

// findChildIndex finds which child to follow for the given key
func (bt *BTree) findChildIndex(node *Node, key []byte) int {
	for i := 0; i < node.NumKeys; i++ {
		if bytes.Compare(key, node.KeyAt(i)) < 0 {
			return i
		}
	}
	return node.NumKeys
}

// insertIntoLeaf inserts a key-value pair into a leaf node
func (bt *BTree) insertIntoLeaf(leaf *Node, key, val []byte) {
	if leaf == nil {
		return // Safety check
	}
	
	index := bt.findKeyIndex(leaf, key)
	
	// If we're at capacity, we need to split first
	if leaf.NumKeys == MaxKeys {
		// We need to handle insertion during split differently
		bt.splitAndInsert(leaf, key, val, index)
		return
	}
	
	// Safety checks
	if index < 0 || index > MaxKeys {
		return
	}
	
	// Shift elements to make room
	for i := leaf.NumKeys; i > index && i > 0; i-- {
		if i < MaxKeys && i-1 >= 0 && i-1 < MaxKeys {
			leaf.Keys[i] = leaf.Keys[i-1]
			leaf.Values[i] = leaf.Values[i-1]
		}
	}
	
	if index < len(leaf.Keys) && index < len(leaf.Values) {
		leaf.Keys[index] = key
		leaf.Values[index] = val
		leaf.NumKeys++
	}
}

// deleteFromLeaf removes a key-value pair from a leaf node
func (bt *BTree) deleteFromLeaf(leaf *Node, index int) {
	// Shift elements to fill the gap
	for i := index; i < leaf.NumKeys-1; i++ {
		leaf.Keys[i] = leaf.Keys[i+1]
		leaf.Values[i] = leaf.Values[i+1]
	}
	
	leaf.NumKeys--
	
	// Handle underflow if necessary
	if leaf.IsUnderflow() && leaf.Parent != nil {
		bt.handleUnderflow(leaf)
	}
}

// splitAndInsert handles insertion into a full leaf by splitting first
func (bt *BTree) splitAndInsert(leaf *Node, key, val []byte, index int) {
	// Create temporary arrays to hold all keys+values including the new one
	allKeys := make([][]byte, MaxKeys+1)
	allValues := make([][]byte, MaxKeys+1)
	
	// Copy existing keys and values, inserting the new one at the right position
	copy(allKeys[:index], leaf.Keys[:index])
	copy(allValues[:index], leaf.Values[:index])
	
	allKeys[index] = key
	allValues[index] = val
	
	copy(allKeys[index+1:], leaf.Keys[index:leaf.NumKeys])
	copy(allValues[index+1:], leaf.Values[index:leaf.NumKeys])
	
	// Now split into two nodes
	newLeaf := NewLeafNode()
	midIndex := (MaxKeys + 1) / 2
	
	// Distribute keys between the two nodes
	for i := 0; i < midIndex; i++ {
		leaf.Keys[i] = allKeys[i]
		leaf.Values[i] = allValues[i]
	}
	leaf.NumKeys = midIndex
	
	for i := midIndex; i < MaxKeys+1; i++ {
		newLeaf.Keys[i-midIndex] = allKeys[i]
		newLeaf.Values[i-midIndex] = allValues[i]
	}
	newLeaf.NumKeys = MaxKeys + 1 - midIndex
	
	// Clear remaining slots in original leaf
	for i := midIndex; i < MaxKeys; i++ {
		leaf.Keys[i] = nil
		leaf.Values[i] = nil
	}
	
	// Update next pointers
	newLeaf.Next = leaf.Next
	leaf.Next = newLeaf
	
	// Insert the new leaf's first key into parent
	if leaf.Parent == nil {
		// Create new root
		newRoot := NewInternalNode()
		newRoot.Keys[0] = newLeaf.Keys[0]
		newRoot.Children[0] = leaf
		newRoot.Children[1] = newLeaf
		newRoot.NumKeys = 1
		
		leaf.Parent = newRoot
		newLeaf.Parent = newRoot
		bt.root = newRoot
	} else {
		bt.insertIntoParent(leaf.Parent, newLeaf.Keys[0], newLeaf)
	}
}

// splitLeaf splits a full leaf node
func (bt *BTree) splitLeaf(leaf *Node) {
	newLeaf := NewLeafNode()
	midIndex := MaxKeys / 2
	
	// Move half the keys to the new leaf (need to handle the overflow case)
	totalKeys := leaf.NumKeys
	for i := midIndex; i < totalKeys; i++ {
		newLeaf.Keys[i-midIndex] = leaf.Keys[i]
		newLeaf.Values[i-midIndex] = leaf.Values[i]
		leaf.Keys[i] = nil
		leaf.Values[i] = nil
	}
	
	leaf.NumKeys = midIndex
	newLeaf.NumKeys = totalKeys - midIndex
	
	// Update next pointers
	newLeaf.Next = leaf.Next
	leaf.Next = newLeaf
	
	// Insert the new leaf's first key into parent
	if leaf.Parent == nil {
		// Create new root
		newRoot := NewInternalNode()
		newRoot.Keys[0] = newLeaf.Keys[0]
		newRoot.Children[0] = leaf
		newRoot.Children[1] = newLeaf
		newRoot.NumKeys = 1
		
		leaf.Parent = newRoot
		newLeaf.Parent = newRoot
		bt.root = newRoot
	} else {
		bt.insertIntoParent(leaf.Parent, newLeaf.Keys[0], newLeaf)
	}
}

// insertIntoParent inserts a key and child pointer into an internal node
func (bt *BTree) insertIntoParent(parent *Node, key []byte, rightChild *Node) {
	rightChild.Parent = parent
	index := bt.findKeyIndex(parent, key)
	
	// Shift keys and children
	for i := parent.NumKeys; i > index; i-- {
		parent.Keys[i] = parent.Keys[i-1]
		parent.Children[i+1] = parent.Children[i]
	}
	
	parent.Keys[index] = key
	parent.Children[index+1] = rightChild
	parent.NumKeys++
	
	// Split if necessary
	if parent.IsFull() {
		bt.splitInternal(parent)
	}
}

// splitInternal splits a full internal node
func (bt *BTree) splitInternal(node *Node) {
	newNode := NewInternalNode()
	midIndex := MaxKeys / 2
	
	// Move half the keys and children to the new node
	for i := midIndex + 1; i < MaxKeys; i++ {
		newNode.Keys[i-midIndex-1] = node.Keys[i]
		newNode.Children[i-midIndex-1] = node.Children[i]
		node.Keys[i] = nil
		node.Children[i] = nil
		
		// Update parent pointers
		if newNode.Children[i-midIndex-1] != nil {
			newNode.Children[i-midIndex-1].Parent = newNode
		}
	}
	
	// Move the last child
	newNode.Children[MaxKeys/2] = node.Children[MaxKeys]
	if newNode.Children[MaxKeys/2] != nil {
		newNode.Children[MaxKeys/2].Parent = newNode
	}
	node.Children[MaxKeys] = nil
	
	// The middle key goes up to parent
	middleKey := node.Keys[midIndex]
	node.Keys[midIndex] = nil
	
	node.NumKeys = midIndex
	newNode.NumKeys = MaxKeys - midIndex - 1
	
	// Insert into parent
	if node.Parent == nil {
		// Create new root
		newRoot := NewInternalNode()
		newRoot.Keys[0] = middleKey
		newRoot.Children[0] = node
		newRoot.Children[1] = newNode
		newRoot.NumKeys = 1
		
		node.Parent = newRoot
		newNode.Parent = newRoot
		bt.root = newRoot
	} else {
		bt.insertIntoParent(node.Parent, middleKey, newNode)
	}
}

// handleUnderflow handles node underflow during deletion
func (bt *BTree) handleUnderflow(node *Node) {
	// Implementation for handling underflow (merge or redistribute)
	// This is a simplified version - full implementation would handle
	// borrowing from siblings and merging nodes
}

// Size returns the number of key-value pairs in the tree
func (bt *BTree) Size() int {
	return bt.size
}
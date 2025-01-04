package btree

// BTreeIterator implements the Iterator interface for B+Tree
type BTreeIterator struct {
	current *Node
	index   int
}

// Next returns the next key-value pair
func (it *BTreeIterator) Next() (key, val []byte) {
	if it.current == nil || it.index >= it.current.NumKeys {
		return nil, nil
	}
	
	key = it.current.KeyAt(it.index)
	val = it.current.ValueAt(it.index)
	
	// Advance to next position
	it.index++
	
	// If we've reached the end of this leaf, move to next leaf
	if it.index >= it.current.NumKeys && it.current.Next != nil {
		it.current = it.current.Next
		it.index = 0
	}
	
	return key, val
}

// ContainsNext returns true if there are more key-value pairs
func (it *BTreeIterator) ContainsNext() bool {
	if it.current == nil {
		return false
	}
	
	// Check if we have more keys in current leaf
	if it.index < it.current.NumKeys {
		return true
	}
	
	// Check if there's a next leaf with keys
	return it.current.Next != nil && it.current.Next.NumKeys > 0
}

// Ensure BTreeIterator implements the Iterator interface
var _ Iterator = (*BTreeIterator)(nil)
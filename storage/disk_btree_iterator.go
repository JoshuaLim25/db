package storage

import "github.com/JoshuaLim25/db/btree"

// DiskBTreeIterator implements the Iterator interface for disk-based B+Tree
type DiskBTreeIterator struct {
	dbt     *DiskBTree
	current PageID
	index   int
}

// Next returns the next key-value pair
func (it *DiskBTreeIterator) Next() (key, val []byte) {
	if it.current == InvalidPageID {
		return nil, nil
	}
	
	node, err := it.dbt.loadNode(it.current)
	if err != nil || it.index >= node.NumKeys {
		return nil, nil
	}
	
	key = node.KeyAt(it.index)
	val = node.ValueAt(it.index)
	
	// Advance to next position
	it.index++
	
	// If we've reached the end of this leaf, we'd move to next leaf
	// For now, we'll just stop (simplified implementation)
	if it.index >= node.NumKeys {
		it.current = InvalidPageID
	}
	
	return key, val
}

// ContainsNext returns true if there are more key-value pairs
func (it *DiskBTreeIterator) ContainsNext() bool {
	if it.current == InvalidPageID {
		return false
	}
	
	node, err := it.dbt.loadNode(it.current)
	if err != nil {
		return false
	}
	
	// Check if we have more keys in current leaf
	return it.index < node.NumKeys
}

// Ensure DiskBTreeIterator implements the Iterator interface
var _ btree.Iterator = (*DiskBTreeIterator)(nil)
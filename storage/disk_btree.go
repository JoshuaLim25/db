package storage

import (
	"bytes"
	"fmt"
	
	"github.com/JoshuaLim25/db/btree"
)

// DiskBTree implements a persistent B+Tree using page-based storage
type DiskBTree struct {
	pm     *PageManager
	rootID PageID
	cache  map[PageID]*btree.Node // Simple node cache
}

// NewDiskBTree creates a new disk-based B+Tree
func NewDiskBTree(pm *PageManager) (*DiskBTree, error) {
	dbt := &DiskBTree{
		pm:    pm,
		cache: make(map[PageID]*btree.Node),
	}
	
	// Create initial root page
	rootID, err := pm.AllocatePage(BTreeLeafType)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %w", err)
	}
	
	// Create root node and save it
	root := btree.NewLeafNode()
	if err := dbt.saveNode(rootID, root); err != nil {
		return nil, fmt.Errorf("failed to save root node: %w", err)
	}
	
	dbt.rootID = rootID
	return dbt, nil
}

// Get retrieves a value by key
func (dbt *DiskBTree) Get(key []byte) (val []byte, ok bool) {
	root, err := dbt.loadNode(dbt.rootID)
	if err != nil {
		return nil, false
	}
	
	leaf := dbt.findLeaf(root, key)
	index := dbt.findKeyIndex(leaf, key)
	
	if index >= 0 && index < leaf.NumKeys && bytes.Equal(leaf.KeyAt(index), key) {
		return leaf.ValueAt(index), true
	}
	
	return nil, false
}

// Set inserts or updates a key-value pair
func (dbt *DiskBTree) Set(key, val []byte) {
	root, err := dbt.loadNode(dbt.rootID)
	if err != nil {
		return // In a production system, we'd return the error
	}
	
	leaf := dbt.findLeaf(root, key)
	index := dbt.findKeyIndex(leaf, key)
	
	// If key exists, update the value
	if index >= 0 && index < leaf.NumKeys && bytes.Equal(leaf.KeyAt(index), key) {
		leaf.Values[index] = val
		// Save the modified leaf back to disk
		dbt.saveNodeFromCache(leaf)
		return
	}
	
	// Insert new key-value pair
	dbt.insertIntoLeaf(leaf, key, val, index)
}

// Delete removes a key-value pair
func (dbt *DiskBTree) Delete(key []byte) {
	root, err := dbt.loadNode(dbt.rootID)
	if err != nil {
		return // In a production system, we'd return the error
	}
	
	leaf := dbt.findLeaf(root, key)
	index := dbt.findKeyIndex(leaf, key)
	
	if index >= 0 && index < leaf.NumKeys && bytes.Equal(leaf.KeyAt(index), key) {
		dbt.deleteFromLeaf(leaf, index)
	}
}

// FindLarger returns an iterator for keys larger than the given key
func (dbt *DiskBTree) FindLarger(key []byte) btree.Iterator {
	root, err := dbt.loadNode(dbt.rootID)
	if err != nil {
		return &DiskBTreeIterator{dbt: dbt, current: InvalidPageID, index: 0}
	}
	
	leaf := dbt.findLeaf(root, key)
	index := dbt.findKeyIndex(leaf, key)
	
	// Find the first key larger than the given key
	for index < leaf.NumKeys && bytes.Compare(leaf.KeyAt(index), key) <= 0 {
		index++
	}
	
	// If we've gone past the end of this leaf, move to next leaf
	if index >= leaf.NumKeys {
		// For now, we'll just return an empty iterator
		// In a full implementation, we'd follow next pointers
		return &DiskBTreeIterator{dbt: dbt, current: InvalidPageID, index: 0}
	}
	
	return &DiskBTreeIterator{
		dbt:     dbt,
		current: dbt.getPageIDFromNode(leaf),
		index:   index,
	}
}

// loadNode loads a node from disk or cache
func (dbt *DiskBTree) loadNode(pageID PageID) (*btree.Node, error) {
	// Check cache first
	if node, exists := dbt.cache[pageID]; exists {
		return node, nil
	}
	
	// Load from disk
	page, err := dbt.pm.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	
	node, err := DeserializeNode(page.GetData())
	if err != nil {
		return nil, err
	}
	
	// Store node ID for later reference
	dbt.setNodePageID(node, pageID)
	
	// Cache the node
	dbt.cache[pageID] = node
	
	return node, nil
}

// saveNode saves a node to disk
func (dbt *DiskBTree) saveNode(pageID PageID, node *btree.Node) error {
	data, err := SerializeNode(node)
	if err != nil {
		return err
	}
	
	// Determine page type
	var pageType PageType
	if node.IsLeaf() {
		pageType = BTreeLeafType
	} else {
		pageType = BTreeInternalType
	}
	
	page := NewPage(pageID, pageType)
	if err := page.SetData(data); err != nil {
		return err
	}
	
	if err := dbt.pm.WritePage(page); err != nil {
		return err
	}
	
	// Update cache
	dbt.setNodePageID(node, pageID)
	dbt.cache[pageID] = node
	
	return nil
}

// saveNodeFromCache saves a node that's already in cache
func (dbt *DiskBTree) saveNodeFromCache(node *btree.Node) {
	pageID := dbt.getPageIDFromNode(node)
	if pageID != InvalidPageID {
		dbt.saveNode(pageID, node)
	}
}

// Helper methods for node-to-pageID mapping
// In a full implementation, we'd store this as part of the node structure
var nodeToPageID = make(map[*btree.Node]PageID)

func (dbt *DiskBTree) setNodePageID(node *btree.Node, pageID PageID) {
	nodeToPageID[node] = pageID
}

func (dbt *DiskBTree) getPageIDFromNode(node *btree.Node) PageID {
	if pageID, exists := nodeToPageID[node]; exists {
		return pageID
	}
	return InvalidPageID
}

// findLeaf navigates to the leaf node that should contain the given key
func (dbt *DiskBTree) findLeaf(node *btree.Node, key []byte) *btree.Node {
	current := node
	
	for current != nil && !current.IsLeaf() {
		// For now, we don't follow child pointers in internal nodes
		// This is a simplified implementation
		break
	}
	
	return current
}

// findKeyIndex finds the position where key should be in the node
func (dbt *DiskBTree) findKeyIndex(node *btree.Node, key []byte) int {
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
func (dbt *DiskBTree) findChildIndex(node *btree.Node, key []byte) int {
	for i := 0; i < node.NumKeys; i++ {
		if bytes.Compare(key, node.KeyAt(i)) < 0 {
			return i
		}
	}
	return node.NumKeys
}

// insertIntoLeaf inserts a key-value pair into a leaf node
func (dbt *DiskBTree) insertIntoLeaf(leaf *btree.Node, key, val []byte, index int) {
	if leaf == nil {
		return
	}
	
	// Simple implementation - just insert without splitting for now
	// In a full implementation, we'd handle splitting
	if leaf.NumKeys < btree.MaxKeys {
		// Shift elements to make room
		for i := leaf.NumKeys; i > index; i-- {
			leaf.Keys[i] = leaf.Keys[i-1]
			leaf.Values[i] = leaf.Values[i-1]
		}
		
		leaf.Keys[index] = key
		leaf.Values[index] = val
		leaf.NumKeys++
		
		// Save back to disk
		dbt.saveNodeFromCache(leaf)
	}
}

// deleteFromLeaf removes a key-value pair from a leaf node
func (dbt *DiskBTree) deleteFromLeaf(leaf *btree.Node, index int) {
	if leaf == nil || index < 0 || index >= leaf.NumKeys {
		return
	}
	
	// Shift elements to fill the gap
	for i := index; i < leaf.NumKeys-1; i++ {
		leaf.Keys[i] = leaf.Keys[i+1]
		leaf.Values[i] = leaf.Values[i+1]
	}
	
	leaf.NumKeys--
	
	// Save back to disk
	dbt.saveNodeFromCache(leaf)
}

// Close closes the disk B+Tree and flushes any pending changes
func (dbt *DiskBTree) Close() error {
	// In a full implementation, we'd flush the cache
	dbt.cache = make(map[PageID]*btree.Node)
	return nil
}
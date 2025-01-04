package storage

import (
	"encoding/binary"
	"fmt"
	
	"github.com/JoshuaLim25/db/btree"
)

// SerializeNode converts a B+Tree node to bytes for storage in a page
func SerializeNode(node *btree.Node) ([]byte, error) {
	if node == nil {
		return nil, fmt.Errorf("cannot serialize nil node")
	}
	
	buf := make([]byte, 0, PageSize-PageHeaderSize)
	
	// Write node type (1 byte)
	var nodeType byte = 0
	if node.IsLeaf() {
		nodeType = 1
	}
	buf = append(buf, nodeType)
	
	// Write number of keys (4 bytes)
	numKeysBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(numKeysBytes, uint32(node.NumKeys))
	buf = append(buf, numKeysBytes...)
	
	// Write keys and values/children
	for i := 0; i < node.NumKeys; i++ {
		key := node.KeyAt(i)
		if key == nil {
			return nil, fmt.Errorf("nil key at index %d", i)
		}
		
		// Write key length and data
		keyLenBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(keyLenBytes, uint32(len(key)))
		buf = append(buf, keyLenBytes...)
		buf = append(buf, key...)
		
		if node.IsLeaf() {
			// Write value for leaf nodes
			val := node.ValueAt(i)
			if val == nil {
				val = []byte{} // Empty value instead of nil
			}
			
			valLenBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(valLenBytes, uint32(len(val)))
			buf = append(buf, valLenBytes...)
			buf = append(buf, val...)
		}
	}
	
	// For internal nodes, write child page IDs
	// For now, we'll store placeholder values since we haven't implemented disk B+Tree yet
	if !node.IsLeaf() {
		for i := 0; i <= node.NumKeys; i++ {
			childPageBytes := make([]byte, 4)
			// Placeholder - will be implemented when we create disk-based B+Tree
			binary.LittleEndian.PutUint32(childPageBytes, 0)
			buf = append(buf, childPageBytes...)
		}
	} else {
		// For leaf nodes, write next page pointer
		nextPageBytes := make([]byte, 4)
		// Placeholder - will be implemented when we create disk-based B+Tree
		binary.LittleEndian.PutUint32(nextPageBytes, 0)
		buf = append(buf, nextPageBytes...)
	}
	
	return buf, nil
}

// DeserializeNode converts bytes back to a B+Tree node
func DeserializeNode(data []byte) (*btree.Node, error) {
	if len(data) < 5 { // At least node type + num keys
		return nil, fmt.Errorf("data too short for node deserialization")
	}
	
	offset := 0
	
	// Read node type
	nodeType := data[offset]
	offset++
	
	// Read number of keys
	numKeys := binary.LittleEndian.Uint32(data[offset:offset+4])
	offset += 4
	
	// Create node
	var node *btree.Node
	if nodeType == 1 {
		node = btree.NewLeafNode()
	} else {
		node = btree.NewInternalNode()
	}
	
	// Read keys and values/children
	for i := 0; i < int(numKeys); i++ {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("insufficient data for key length")
		}
		
		// Read key
		keyLen := binary.LittleEndian.Uint32(data[offset:offset+4])
		offset += 4
		
		if offset+int(keyLen) > len(data) {
			return nil, fmt.Errorf("insufficient data for key")
		}
		
		key := make([]byte, keyLen)
		copy(key, data[offset:offset+int(keyLen)])
		offset += int(keyLen)
		
		node.Keys[i] = key
		
		if node.IsLeaf() {
			// Read value for leaf nodes
			if offset+4 > len(data) {
				return nil, fmt.Errorf("insufficient data for value length")
			}
			
			valLen := binary.LittleEndian.Uint32(data[offset:offset+4])
			offset += 4
			
			if offset+int(valLen) > len(data) {
				return nil, fmt.Errorf("insufficient data for value")
			}
			
			val := make([]byte, valLen)
			copy(val, data[offset:offset+int(valLen)])
			offset += int(valLen)
			
			node.Values[i] = val
		}
	}
	
	node.NumKeys = int(numKeys)
	
	// Skip child/next page pointers for now (will implement in disk B+Tree)
	// This is just to make the deserialization complete for testing
	
	return node, nil
}

// EstimateNodeSize estimates the serialized size of a node
func EstimateNodeSize(node *btree.Node) int {
	size := 5 // node type (1) + num keys (4)
	
	for i := 0; i < node.NumKeys; i++ {
		key := node.KeyAt(i)
		if key != nil {
			size += 4 + len(key) // key length + key data
		}
		
		if node.IsLeaf() {
			val := node.ValueAt(i)
			if val != nil {
				size += 4 + len(val) // value length + value data
			} else {
				size += 4 // just length for empty value
			}
		}
	}
	
	// Child pointers or next page pointer
	if node.IsLeaf() {
		size += 4 // next page pointer
	} else {
		size += 4 * (node.NumKeys + 1) // child page pointers
	}
	
	return size
}
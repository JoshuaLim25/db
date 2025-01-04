package storage

import (
	"testing"
	
	"github.com/JoshuaLim25/db/btree"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSerializeLeafNode(t *testing.T) {
	// Create a leaf node with some data
	node := btree.NewLeafNode()
	
	node.Keys[0] = []byte("apple")
	node.Values[0] = []byte("fruit")
	node.Keys[1] = []byte("banana") 
	node.Values[1] = []byte("yellow")
	node.NumKeys = 2
	
	// Serialize
	data, err := SerializeNode(node)
	require.NoError(t, err)
	assert.NotNil(t, data)
	
	// Deserialize
	newNode, err := DeserializeNode(data)
	require.NoError(t, err)
	
	// Verify
	assert.True(t, newNode.IsLeaf())
	assert.Equal(t, 2, newNode.NumKeys)
	assert.Equal(t, []byte("apple"), newNode.KeyAt(0))
	assert.Equal(t, []byte("fruit"), newNode.ValueAt(0))
	assert.Equal(t, []byte("banana"), newNode.KeyAt(1))
	assert.Equal(t, []byte("yellow"), newNode.ValueAt(1))
}

func TestSerializeInternalNode(t *testing.T) {
	// Create an internal node with some keys
	node := btree.NewInternalNode()
	
	node.Keys[0] = []byte("middle")
	node.Keys[1] = []byte("zebra")
	node.NumKeys = 2
	
	// Serialize
	data, err := SerializeNode(node)
	require.NoError(t, err)
	assert.NotNil(t, data)
	
	// Deserialize  
	newNode, err := DeserializeNode(data)
	require.NoError(t, err)
	
	// Verify
	assert.False(t, newNode.IsLeaf())
	assert.Equal(t, 2, newNode.NumKeys)
	assert.Equal(t, []byte("middle"), newNode.KeyAt(0))
	assert.Equal(t, []byte("zebra"), newNode.KeyAt(1))
}

func TestSerializeEmptyNode(t *testing.T) {
	// Create empty leaf node
	node := btree.NewLeafNode()
	
	// Serialize
	data, err := SerializeNode(node)
	require.NoError(t, err)
	
	// Deserialize
	newNode, err := DeserializeNode(data)
	require.NoError(t, err)
	
	// Verify
	assert.True(t, newNode.IsLeaf())
	assert.Equal(t, 0, newNode.NumKeys)
}

func TestSerializeNodeWithEmptyValues(t *testing.T) {
	// Create leaf node with empty values
	node := btree.NewLeafNode()
	
	node.Keys[0] = []byte("key1")
	node.Values[0] = []byte("") // Empty value
	node.Keys[1] = []byte("key2")
	node.Values[1] = nil // Nil value (should be handled as empty)
	node.NumKeys = 2
	
	// Serialize
	data, err := SerializeNode(node)
	require.NoError(t, err)
	
	// Deserialize
	newNode, err := DeserializeNode(data)
	require.NoError(t, err)
	
	// Verify
	assert.Equal(t, 2, newNode.NumKeys)
	assert.Equal(t, []byte("key1"), newNode.KeyAt(0))
	assert.Equal(t, []byte(""), newNode.ValueAt(0))
	assert.Equal(t, []byte("key2"), newNode.KeyAt(1))
	assert.Equal(t, []byte(""), newNode.ValueAt(1)) // Nil should become empty
}

func TestSerializeNilNode(t *testing.T) {
	_, err := SerializeNode(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "nil node")
}

func TestDeserializeInvalidData(t *testing.T) {
	// Test with too short data
	_, err := DeserializeNode([]byte{1, 2, 3})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "too short")
	
	// Test with truncated data
	_, err = DeserializeNode([]byte{1, 0, 0, 0, 1, 0, 0, 0, 5}) // Claims 1 key but data is truncated
	assert.Error(t, err)
}

func TestEstimateNodeSize(t *testing.T) {
	// Create a test node
	node := btree.NewLeafNode()
	node.Keys[0] = []byte("test")    // 4 bytes
	node.Values[0] = []byte("value") // 5 bytes
	node.NumKeys = 1
	
	estimated := EstimateNodeSize(node)
	
	// Expected: 1 (type) + 4 (numkeys) + 4 (keylen) + 4 (key) + 4 (vallen) + 5 (val) + 4 (next) = 26
	expected := 1 + 4 + 4 + 4 + 4 + 5 + 4
	assert.Equal(t, expected, estimated)
}

func TestNodeFitsInPage(t *testing.T) {
	// Test that reasonable nodes fit in pages
	node := btree.NewLeafNode()
	
	// Add some keys and values
	for i := 0; i < 4; i++ { // MaxKeys = 4 in btree package
		key := []byte("key_" + string(rune('0'+i)))
		val := []byte("value_" + string(rune('0'+i)))
		node.Keys[i] = key
		node.Values[i] = val
		node.NumKeys++
	}
	
	// Serialize and check size
	data, err := SerializeNode(node)
	require.NoError(t, err)
	
	availableSpace := PageSize - PageHeaderSize
	assert.Less(t, len(data), availableSpace, "Node should fit in page")
	
	// Test that we can store it in a page
	page := NewPage(1, BTreeLeafType)
	err = page.SetData(data)
	assert.NoError(t, err, "Serialized node should fit in page")
}
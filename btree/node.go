package btree

const (
	// MaxKeys defines the maximum number of keys per node
	// This determines the branching factor of the B+Tree
	MaxKeys = 4
	
	// MinKeys is the minimum number of keys (except for root)
	MinKeys = MaxKeys / 2
)

// NodeType distinguishes between internal and leaf nodes
type NodeType int

const (
	LeafNode NodeType = iota
	InternalNode
)

// Node represents a B+Tree node (either internal or leaf)
type Node struct {
	Type     NodeType
	Keys     [][]byte   // Keys stored in this node
	Values   [][]byte   // Values (only used in leaf nodes)
	Children []*Node    // Child pointers (only used in internal nodes)
	Next     *Node      // Next leaf node pointer (only used in leaf nodes)
	Parent   *Node      // Parent node pointer
	NumKeys  int        // Current number of keys
}

// NewLeafNode creates a new leaf node
func NewLeafNode() *Node {
	return &Node{
		Type:     LeafNode,
		Keys:     make([][]byte, MaxKeys),
		Values:   make([][]byte, MaxKeys),
		Children: nil,
		Next:     nil,
		Parent:   nil,
		NumKeys:  0,
	}
}

// NewInternalNode creates a new internal node
func NewInternalNode() *Node {
	return &Node{
		Type:     InternalNode,
		Keys:     make([][]byte, MaxKeys),
		Values:   nil,
		Children: make([]*Node, MaxKeys+1), // Internal nodes have MaxKeys+1 children
		Next:     nil,
		Parent:   nil,
		NumKeys:  0,
	}
}

// IsLeaf returns true if this is a leaf node
func (n *Node) IsLeaf() bool {
	return n.Type == LeafNode
}

// IsFull returns true if the node is at capacity
func (n *Node) IsFull() bool {
	return n.NumKeys == MaxKeys
}

// IsUnderflow returns true if the node has fewer than minimum keys
// Root nodes and empty nodes are not considered underflow
func (n *Node) IsUnderflow() bool {
	// Root nodes don't have underflow constraints
	if n.Parent == nil {
		return false
	}
	return n.NumKeys < MinKeys
}

// KeyAt returns the key at the given index
func (n *Node) KeyAt(index int) []byte {
	if index < 0 || index >= n.NumKeys {
		return nil
	}
	return n.Keys[index]
}

// ValueAt returns the value at the given index (leaf nodes only)
func (n *Node) ValueAt(index int) []byte {
	if !n.IsLeaf() || index < 0 || index >= n.NumKeys {
		return nil
	}
	return n.Values[index]
}

// ChildAt returns the child at the given index (internal nodes only)
func (n *Node) ChildAt(index int) *Node {
	if n.IsLeaf() || index < 0 || index > n.NumKeys {
		return nil
	}
	return n.Children[index]
}
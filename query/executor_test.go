package query

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Mock implementations for testing

type MockDatabase struct {
	tables map[string]*MockTable
}

func NewMockDatabase() *MockDatabase {
	return &MockDatabase{
		tables: make(map[string]*MockTable),
	}
}

func (m *MockDatabase) GetTable(tableName string) (Table, error) {
	if table, exists := m.tables[tableName]; exists {
		return table, nil
	}
	return nil, fmt.Errorf("table %s does not exist", tableName)
}

func (m *MockDatabase) CreateTable(tableName string) (Table, error) {
	if _, exists := m.tables[tableName]; exists {
		return nil, fmt.Errorf("table %s already exists", tableName)
	}
	
	table := &MockTable{
		name: tableName,
		data: make(map[string]string),
	}
	m.tables[tableName] = table
	return table, nil
}

type MockTable struct {
	name string
	data map[string]string
}

func (m *MockTable) Insert(key, value []byte) error {
	m.data[string(key)] = string(value)
	return nil
}

func (m *MockTable) Select(key []byte) ([]byte, bool) {
	if value, exists := m.data[string(key)]; exists {
		return []byte(value), true
	}
	return nil, false
}

func (m *MockTable) Update(key, value []byte) error {
	if _, exists := m.data[string(key)]; !exists {
		return fmt.Errorf("key not found: %s", key)
	}
	m.data[string(key)] = string(value)
	return nil
}

func (m *MockTable) Delete(key []byte) error {
	if _, exists := m.data[string(key)]; !exists {
		return fmt.Errorf("key not found: %s", key)
	}
	delete(m.data, string(key))
	return nil
}

func (m *MockTable) Scan(startKey []byte) Iterator {
	return &MockIterator{
		data:    m.data,
		started: false,
	}
}

func (m *MockTable) Name() string {
	return m.name
}

type MockIterator struct {
	data    map[string]string
	started bool
	keys    []string
	index   int
}

func (m *MockIterator) Next() (key, val []byte) {
	if !m.started {
		m.started = true
		m.keys = make([]string, 0, len(m.data))
		for k := range m.data {
			m.keys = append(m.keys, k)
		}
		m.index = 0
	}
	
	if m.index >= len(m.keys) {
		return nil, nil
	}
	
	key = []byte(m.keys[m.index])
	val = []byte(m.data[m.keys[m.index]])
	m.index++
	
	return key, val
}

func (m *MockIterator) ContainsNext() bool {
	if !m.started {
		return len(m.data) > 0
	}
	return m.index < len(m.keys)
}

func TestExecutorInsert(t *testing.T) {
	db := NewMockDatabase()
	_, err := db.CreateTable("users")
	assert.NoError(t, err)
	
	// Test INSERT
	result := ExecuteSQL(db, "INSERT INTO users VALUES ('john', 'john@example.com')")
	assert.True(t, result.Success)
	assert.NoError(t, result.Error)
	assert.Contains(t, result.Message, "Inserted 1 row")
	
	// Verify the data was inserted
	table, err := db.GetTable("users")
	assert.NoError(t, err)
	
	value, found := table.Select([]byte("john"))
	assert.True(t, found)
	assert.Equal(t, "john@example.com", string(value))
}

func TestExecutorSelect(t *testing.T) {
	db := NewMockDatabase()
	table, err := db.CreateTable("users")
	assert.NoError(t, err)
	
	// Insert test data
	err = table.Insert([]byte("john"), []byte("john@example.com"))
	assert.NoError(t, err)
	err = table.Insert([]byte("jane"), []byte("jane@example.com"))
	assert.NoError(t, err)
	
	// Test SELECT with WHERE clause
	result := ExecuteSQL(db, "SELECT * FROM users WHERE id = 'john'")
	assert.True(t, result.Success)
	assert.NoError(t, result.Error)
	assert.Len(t, result.Rows, 1)
	
	// Test SELECT without WHERE clause (scan all)
	result = ExecuteSQL(db, "SELECT * FROM users")
	assert.True(t, result.Success)
	assert.NoError(t, result.Error)
	assert.Len(t, result.Rows, 2)
}

func TestExecutorUpdate(t *testing.T) {
	db := NewMockDatabase()
	table, err := db.CreateTable("users")
	assert.NoError(t, err)
	
	// Insert test data
	err = table.Insert([]byte("john"), []byte("john@example.com"))
	assert.NoError(t, err)
	
	// Test UPDATE
	result := ExecuteSQL(db, "UPDATE users SET email = 'newemail@example.com' WHERE id = 'john'")
	assert.True(t, result.Success)
	assert.NoError(t, result.Error)
	assert.Contains(t, result.Message, "Updated 1 rows")
	
	// Verify the data was updated
	value, found := table.Select([]byte("john"))
	assert.True(t, found)
	assert.Contains(t, string(value), "newemail@example.com")
}

func TestExecutorDelete(t *testing.T) {
	db := NewMockDatabase()
	table, err := db.CreateTable("users")
	assert.NoError(t, err)
	
	// Insert test data
	err = table.Insert([]byte("john"), []byte("john@example.com"))
	assert.NoError(t, err)
	
	// Test DELETE
	result := ExecuteSQL(db, "DELETE FROM users WHERE id = 'john'")
	assert.True(t, result.Success)
	assert.NoError(t, result.Error)
	assert.Contains(t, result.Message, "Deleted 1 rows")
	
	// Verify the data was deleted
	_, found := table.Select([]byte("john"))
	assert.False(t, found)
}

func TestExecutorErrors(t *testing.T) {
	db := NewMockDatabase()
	
	// Test SELECT from non-existent table
	result := ExecuteSQL(db, "SELECT * FROM nonexistent")
	assert.False(t, result.Success)
	assert.Error(t, result.Error)
	assert.Contains(t, result.Error.Error(), "does not exist")
	
	// Test invalid SQL
	result = ExecuteSQL(db, "INVALID SQL")
	assert.False(t, result.Success)
	assert.Error(t, result.Error)
	assert.Contains(t, result.Error.Error(), "parse error")
}
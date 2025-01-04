package db

import (
	"fmt"
	"sync"
	
	"github.com/JoshuaLim25/db/storage"
)

// Table represents a database table backed by a B+Tree
type Table struct {
	name  string
	btree *storage.DiskBTree
	mu    sync.RWMutex
}

// NewTable creates a new table with the given name
func NewTable(name string, pm *storage.PageManager) (*Table, error) {
	btree, err := storage.NewDiskBTree(pm)
	if err != nil {
		return nil, fmt.Errorf("failed to create B+Tree for table %s: %w", name, err)
	}
	
	return &Table{
		name:  name,
		btree: btree,
	}, nil
}

// Name returns the table name
func (t *Table) Name() string {
	return t.name
}

// Insert inserts a key-value pair into the table
func (t *Table) Insert(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	t.btree.Set(key, value)
	return nil
}

// Select retrieves a value by key from the table
func (t *Table) Select(key []byte) ([]byte, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	return t.btree.Get(key)
}

// Update updates a key with a new value
func (t *Table) Update(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	// Check if key exists first
	if _, exists := t.btree.Get(key); !exists {
		return fmt.Errorf("key not found: %s", key)
	}
	
	t.btree.Set(key, value)
	return nil
}

// Delete removes a key-value pair from the table
func (t *Table) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	// Check if key exists first
	if _, exists := t.btree.Get(key); !exists {
		return fmt.Errorf("key not found: %s", key)
	}
	
	t.btree.Delete(key)
	return nil
}

// Scan returns an iterator for keys larger than the given key
func (t *Table) Scan(startKey []byte) storage.Iterator {
	t.mu.RLock()
	defer t.mu.RUnlock()
	
	return t.btree.FindLarger(startKey)
}

// Close closes the table and flushes any pending changes
func (t *Table) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	
	return t.btree.Close()
}

// Database represents a collection of tables
type Database struct {
	name   string
	pm     *storage.PageManager
	tables map[string]*Table
	mu     sync.RWMutex
}

// NewDatabase creates a new database with the given name and file
func NewDatabase(name, filename string) (*Database, error) {
	pm, err := storage.NewPageManager(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create page manager: %w", err)
	}
	
	return &Database{
		name:   name,
		pm:     pm,
		tables: make(map[string]*Table),
	}, nil
}

// Name returns the database name
func (db *Database) Name() string {
	return db.name
}

// CreateTable creates a new table with the given name
func (db *Database) CreateTable(tableName string) (*Table, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	if _, exists := db.tables[tableName]; exists {
		return nil, fmt.Errorf("table %s already exists", tableName)
	}
	
	table, err := NewTable(tableName, db.pm)
	if err != nil {
		return nil, err
	}
	
	db.tables[tableName] = table
	return table, nil
}

// GetTable retrieves a table by name
func (db *Database) GetTable(tableName string) (*Table, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	table, exists := db.tables[tableName]
	if !exists {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}
	
	return table, nil
}

// DropTable removes a table
func (db *Database) DropTable(tableName string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	table, exists := db.tables[tableName]
	if !exists {
		return fmt.Errorf("table %s does not exist", tableName)
	}
	
	// Close the table first
	if err := table.Close(); err != nil {
		return fmt.Errorf("failed to close table %s: %w", tableName, err)
	}
	
	// Remove from tables map
	delete(db.tables, tableName)
	return nil
}

// ListTables returns a list of all table names
func (db *Database) ListTables() []string {
	db.mu.RLock()
	defer db.mu.RUnlock()
	
	names := make([]string, 0, len(db.tables))
	for name := range db.tables {
		names = append(names, name)
	}
	return names
}

// Close closes the database and all tables
func (db *Database) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	
	// Close all tables first
	for _, table := range db.tables {
		if err := table.Close(); err != nil {
			return fmt.Errorf("failed to close table %s: %w", table.name, err)
		}
	}
	
	// Close the page manager
	if err := db.pm.Close(); err != nil {
		return fmt.Errorf("failed to close page manager: %w", err)
	}
	
	return nil
}
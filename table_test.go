package db

import (
	"os"
	"testing"
	
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	
	"github.com/JoshuaLim25/db/storage"
)

func TestTableBasicOperations(t *testing.T) {
	tempFile := "test_table.dat"
	defer os.Remove(tempFile)
	
	db, err := NewDatabase("testdb", tempFile)
	require.NoError(t, err)
	defer db.Close()
	
	// Create table
	table, err := db.CreateTable("users")
	require.NoError(t, err)
	assert.Equal(t, "users", table.Name())
	
	// Insert data
	err = table.Insert([]byte("user1"), []byte("John Doe"))
	require.NoError(t, err)
	
	err = table.Insert([]byte("user2"), []byte("Jane Smith"))
	require.NoError(t, err)
	
	// Select data
	value, exists := table.Select([]byte("user1"))
	assert.True(t, exists, "user1 should exist")
	assert.Equal(t, []byte("John Doe"), value, "user1 should have correct value")
	
	value, exists = table.Select([]byte("user2"))
	assert.True(t, exists, "user2 should exist")
	assert.Equal(t, []byte("Jane Smith"), value, "user2 should have correct value")
	
	// Select non-existent
	_, exists = table.Select([]byte("user3"))
	assert.False(t, exists, "user3 should not exist")
}

func TestTableUpdate(t *testing.T) {
	tempFile := "test_table_update.dat"
	defer os.Remove(tempFile)
	
	db, err := NewDatabase("testdb", tempFile)
	require.NoError(t, err)
	defer db.Close()
	
	table, err := db.CreateTable("users")
	require.NoError(t, err)
	
	// Insert initial data
	err = table.Insert([]byte("user1"), []byte("John Doe"))
	require.NoError(t, err)
	
	// Update existing key
	err = table.Update([]byte("user1"), []byte("John Smith"))
	require.NoError(t, err)
	
	// Verify update
	value, exists := table.Select([]byte("user1"))
	assert.True(t, exists, "user1 should still exist")
	assert.Equal(t, []byte("John Smith"), value, "user1 should have updated value")
	
	// Try to update non-existent key
	err = table.Update([]byte("user2"), []byte("Jane Doe"))
	assert.Error(t, err, "updating non-existent key should fail")
	assert.Contains(t, err.Error(), "key not found", "error should mention key not found")
}

func TestTableDelete(t *testing.T) {
	tempFile := "test_table_delete.dat"
	defer os.Remove(tempFile)
	
	db, err := NewDatabase("testdb", tempFile)
	require.NoError(t, err)
	defer db.Close()
	
	table, err := db.CreateTable("users")
	require.NoError(t, err)
	
	// Insert data
	err = table.Insert([]byte("user1"), []byte("John Doe"))
	require.NoError(t, err)
	
	err = table.Insert([]byte("user2"), []byte("Jane Smith"))
	require.NoError(t, err)
	
	// Delete existing key
	err = table.Delete([]byte("user1"))
	require.NoError(t, err)
	
	// Verify deletion
	_, exists := table.Select([]byte("user1"))
	assert.False(t, exists, "user1 should be deleted")
	
	// Other key should still exist
	_, exists = table.Select([]byte("user2"))
	assert.True(t, exists, "user2 should still exist")
	
	// Try to delete non-existent key
	err = table.Delete([]byte("user3"))
	assert.Error(t, err, "deleting non-existent key should fail")
	assert.Contains(t, err.Error(), "key not found", "error should mention key not found")
}

func TestDatabaseTableManagement(t *testing.T) {
	tempFile := "test_database.dat"
	defer os.Remove(tempFile)
	
	db, err := NewDatabase("testdb", tempFile)
	require.NoError(t, err)
	defer db.Close()
	
	assert.Equal(t, "testdb", db.Name())
	
	// Initially no tables
	tables := db.ListTables()
	assert.Empty(t, tables, "database should start with no tables")
	
	// Create tables
	table1, err := db.CreateTable("users")
	require.NoError(t, err)
	assert.Equal(t, "users", table1.Name())
	
	table2, err := db.CreateTable("products")
	require.NoError(t, err)
	assert.Equal(t, "products", table2.Name())
	
	// List tables
	tables = db.ListTables()
	assert.Len(t, tables, 2, "should have 2 tables")
	assert.Contains(t, tables, "users", "should contain users table")
	assert.Contains(t, tables, "products", "should contain products table")
	
	// Get table
	retrievedTable, err := db.GetTable("users")
	require.NoError(t, err)
	assert.Equal(t, "users", retrievedTable.Name())
	
	// Try to create duplicate table
	_, err = db.CreateTable("users")
	assert.Error(t, err, "creating duplicate table should fail")
	assert.Contains(t, err.Error(), "already exists", "error should mention table exists")
	
	// Try to get non-existent table
	_, err = db.GetTable("nonexistent")
	assert.Error(t, err, "getting non-existent table should fail")
	assert.Contains(t, err.Error(), "does not exist", "error should mention table doesn't exist")
}

func TestDatabaseDropTable(t *testing.T) {
	tempFile := "test_database_drop.dat"
	defer os.Remove(tempFile)
	
	db, err := NewDatabase("testdb", tempFile)
	require.NoError(t, err)
	defer db.Close()
	
	// Create table
	_, err = db.CreateTable("temp_table")
	require.NoError(t, err)
	
	// Verify table exists
	tables := db.ListTables()
	assert.Contains(t, tables, "temp_table", "table should exist")
	
	// Drop table
	err = db.DropTable("temp_table")
	require.NoError(t, err)
	
	// Verify table is gone
	tables = db.ListTables()
	assert.NotContains(t, tables, "temp_table", "table should be dropped")
	
	// Try to get dropped table
	_, err = db.GetTable("temp_table")
	assert.Error(t, err, "getting dropped table should fail")
	
	// Try to drop non-existent table
	err = db.DropTable("nonexistent")
	assert.Error(t, err, "dropping non-existent table should fail")
}

func TestTableScan(t *testing.T) {
	tempFile := "test_table_scan.dat"
	defer os.Remove(tempFile)
	
	db, err := NewDatabase("testdb", tempFile)
	require.NoError(t, err)
	defer db.Close()
	
	table, err := db.CreateTable("items")
	require.NoError(t, err)
	
	// Insert some data
	items := map[string]string{
		"apple":  "red fruit",
		"banana": "yellow fruit",
		"cherry": "small red fruit",
		"date":   "sweet fruit",
	}
	
	for key, value := range items {
		err = table.Insert([]byte(key), []byte(value))
		require.NoError(t, err)
	}
	
	// Test scan
	iter := table.Scan([]byte("banana"))
	assert.NotNil(t, iter, "scan should return an iterator")
	
	// Test that iterator implements the interface
	var _ storage.Iterator = iter
}
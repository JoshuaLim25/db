package query

import (
	"fmt"
)

// Database interface for executing queries against
type Database interface {
	GetTable(tableName string) (Table, error)
	CreateTable(tableName string) (Table, error)
}

// Table interface for table operations
type Table interface {
	Insert(key, value []byte) error
	Select(key []byte) ([]byte, bool)
	Update(key, value []byte) error
	Delete(key []byte) error
	Scan(startKey []byte) Iterator
	Name() string
}

// Iterator interface for scanning results
type Iterator interface {
	Next() (key, val []byte)
	ContainsNext() bool
}

// QueryResult represents the result of executing a query
type QueryResult struct {
	Success bool
	Message string
	Rows    []map[string]string // For SELECT queries
	Error   error
}

// Executor executes parsed SQL statements
type Executor struct {
	db Database
}

// NewExecutor creates a new query executor
func NewExecutor(db Database) *Executor {
	return &Executor{db: db}
}

// Execute executes a parsed SQL statement and returns the result
func (e *Executor) Execute(stmt Statement) *QueryResult {
	switch s := stmt.(type) {
	case *SelectStatement:
		return e.executeSelect(s)
	case *InsertStatement:
		return e.executeInsert(s)
	case *UpdateStatement:
		return e.executeUpdate(s)
	case *DeleteStatement:
		return e.executeDelete(s)
	default:
		return &QueryResult{
			Success: false,
			Error:   fmt.Errorf("unsupported statement type"),
		}
	}
}

// executeSelect executes a SELECT statement
func (e *Executor) executeSelect(stmt *SelectStatement) *QueryResult {
	table, err := e.db.GetTable(stmt.TableName)
	if err != nil {
		return &QueryResult{Success: false, Error: err}
	}

	var rows []map[string]string

	if stmt.Where != nil {
		// Handle WHERE clause - simplified to only handle single key lookups
		if comp, ok := stmt.Where.(*ComparisonExpression); ok && comp.Operator == "=" {
			key := []byte(comp.Right)
			if value, found := table.Select(key); found {
				if e.matchesColumns(stmt.Columns, comp.Left, string(key), string(value)) {
					row := make(map[string]string)
					row[comp.Left] = string(key)
					row["value"] = string(value) // Simplified - in real DB this would parse structured data
					rows = append(rows, row)
				}
			}
		}
	} else {
		// No WHERE clause - scan all records
		iter := table.Scan([]byte(""))
		for iter.ContainsNext() {
			key, value := iter.Next()
			if key != nil {
				row := make(map[string]string)
				row["key"] = string(key)
				row["value"] = string(value)
				rows = append(rows, row)
			}
		}
	}

	return &QueryResult{
		Success: true,
		Message: fmt.Sprintf("Selected %d rows", len(rows)),
		Rows:    rows,
	}
}

// executeInsert executes an INSERT statement
func (e *Executor) executeInsert(stmt *InsertStatement) *QueryResult {
	table, err := e.db.GetTable(stmt.TableName)
	if err != nil {
		return &QueryResult{Success: false, Error: err}
	}

	// For simplicity, use the first value as key and concatenate others as value
	// In a real implementation, we'd have proper column mapping
	if len(stmt.Values) == 0 || len(stmt.Values[0]) == 0 {
		return &QueryResult{Success: false, Error: fmt.Errorf("no values provided")}
	}

	values := stmt.Values[0]
	key := []byte(values[0])
	
	// Concatenate remaining values as the stored value
	var value string
	if len(values) > 1 {
		for i, v := range values[1:] {
			if i > 0 {
				value += "|"
			}
			value += v
		}
	} else {
		value = values[0] // If only one value, use it as both key and value
	}

	if err := table.Insert(key, []byte(value)); err != nil {
		return &QueryResult{Success: false, Error: err}
	}

	return &QueryResult{
		Success: true,
		Message: fmt.Sprintf("Inserted 1 row into %s", stmt.TableName),
	}
}

// executeUpdate executes an UPDATE statement
func (e *Executor) executeUpdate(stmt *UpdateStatement) *QueryResult {
	table, err := e.db.GetTable(stmt.TableName)
	if err != nil {
		return &QueryResult{Success: false, Error: err}
	}

	updatedRows := 0

	if stmt.Where != nil {
		// Handle WHERE clause - simplified to only handle single key lookups
		if comp, ok := stmt.Where.(*ComparisonExpression); ok && comp.Operator == "=" {
			key := []byte(comp.Right)
			
			// Build new value from SET assignments
			var newValue string
			for col, val := range stmt.Set {
				if len(newValue) > 0 {
					newValue += "|"
				}
				newValue += col + ":" + val
			}
			
			if err := table.Update(key, []byte(newValue)); err != nil {
				return &QueryResult{Success: false, Error: err}
			}
			updatedRows = 1
		}
	} else {
		// No WHERE clause - update all records (dangerous, but simplified)
		return &QueryResult{
			Success: false,
			Error:   fmt.Errorf("UPDATE without WHERE clause not supported in this implementation"),
		}
	}

	return &QueryResult{
		Success: true,
		Message: fmt.Sprintf("Updated %d rows in %s", updatedRows, stmt.TableName),
	}
}

// executeDelete executes a DELETE statement
func (e *Executor) executeDelete(stmt *DeleteStatement) *QueryResult {
	table, err := e.db.GetTable(stmt.TableName)
	if err != nil {
		return &QueryResult{Success: false, Error: err}
	}

	deletedRows := 0

	if stmt.Where != nil {
		// Handle WHERE clause - simplified to only handle single key lookups
		if comp, ok := stmt.Where.(*ComparisonExpression); ok && comp.Operator == "=" {
			key := []byte(comp.Right)
			
			if err := table.Delete(key); err != nil {
				return &QueryResult{Success: false, Error: err}
			}
			deletedRows = 1
		}
	} else {
		// No WHERE clause - delete all records (dangerous, but simplified)
		return &QueryResult{
			Success: false,
			Error:   fmt.Errorf("DELETE without WHERE clause not supported in this implementation"),
		}
	}

	return &QueryResult{
		Success: true,
		Message: fmt.Sprintf("Deleted %d rows from %s", deletedRows, stmt.TableName),
	}
}

// matchesColumns checks if the returned data matches the requested columns
func (e *Executor) matchesColumns(requestedColumns []string, keyColumn, keyValue, storedValue string) bool {
	if len(requestedColumns) == 1 && requestedColumns[0] == "*" {
		return true
	}
	
	// Simplified column matching
	for _, col := range requestedColumns {
		if col == keyColumn {
			return true
		}
	}
	return false
}

// ExecuteSQL is a convenience function that parses and executes a SQL string
func ExecuteSQL(db Database, sql string) *QueryResult {
	stmt, err := ParseSQL(sql)
	if err != nil {
		return &QueryResult{Success: false, Error: fmt.Errorf("parse error: %w", err)}
	}
	
	executor := NewExecutor(db)
	return executor.Execute(stmt)
}
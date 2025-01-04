package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/JoshuaLim25/db"
	"github.com/JoshuaLim25/db/query"
)

// DatabaseWrapper wraps our table.go Database to implement the query interfaces
type DatabaseWrapper struct {
	db *db.Database
}

func (dw *DatabaseWrapper) GetTable(tableName string) (query.Table, error) {
	table, err := dw.db.GetTable(tableName)
	if err != nil {
		return nil, err
	}
	return &TableWrapper{table: table}, nil
}

func (dw *DatabaseWrapper) CreateTable(tableName string) (query.Table, error) {
	table, err := dw.db.CreateTable(tableName)
	if err != nil {
		return nil, err
	}
	return &TableWrapper{table: table}, nil
}

// TableWrapper wraps our table.go Table to implement the query interfaces
type TableWrapper struct {
	table *db.Table
}

func (tw *TableWrapper) Insert(key, value []byte) error {
	return tw.table.Insert(key, value)
}

func (tw *TableWrapper) Select(key []byte) ([]byte, bool) {
	return tw.table.Select(key)
}

func (tw *TableWrapper) Update(key, value []byte) error {
	return tw.table.Update(key, value)
}

func (tw *TableWrapper) Delete(key []byte) error {
	return tw.table.Delete(key)
}

func (tw *TableWrapper) Scan(startKey []byte) query.Iterator {
	iter := tw.table.Scan(startKey)
	return &IteratorWrapper{iterator: iter}
}

func (tw *TableWrapper) Name() string {
	return tw.table.Name()
}

// IteratorWrapper wraps our storage iterator to implement the query iterator interface
type IteratorWrapper struct {
	iterator IteratorImpl
}

func (iw *IteratorWrapper) Next() (key, val []byte) {
	return iw.iterator.Next()
}

func (iw *IteratorWrapper) ContainsNext() bool {
	return iw.iterator.ContainsNext()
}

// IteratorImpl interface to match our storage iterator
type IteratorImpl interface {
	Next() (key, val []byte)
	ContainsNext() bool
}

func main() {
	fmt.Println("🗄️  Simple Database (B+Tree + SQL)")
	fmt.Println("Commands: CREATE TABLE name, SELECT/INSERT/UPDATE/DELETE, .quit")
	fmt.Println("Example: CREATE TABLE users")
	fmt.Println("         INSERT INTO users VALUES ('john', 'john@example.com')")
	fmt.Println("         SELECT * FROM users")
	fmt.Println()

	// Create database
	database, err := db.NewDatabase("testdb", "testdb.dat")
	if err != nil {
		fmt.Printf("Error creating database: %v\n", err)
		return
	}
	defer database.Close()

	// Wrap for query interface
	dbWrapper := &DatabaseWrapper{db: database}

	scanner := bufio.NewScanner(os.Stdin)
	
	for {
		fmt.Print("db> ")
		
		if !scanner.Scan() {
			break
		}
		
		input := strings.TrimSpace(scanner.Text())
		
		if input == "" {
			continue
		}
		
		if input == ".quit" || input == "quit" || input == "exit" {
			fmt.Println("Goodbye!")
			break
		}
		
		// Handle CREATE TABLE separately since it's not in our SQL parser yet
		if strings.HasPrefix(strings.ToUpper(input), "CREATE TABLE ") {
			parts := strings.Fields(input)
			if len(parts) >= 3 {
				tableName := parts[2]
				_, err := dbWrapper.CreateTable(tableName)
				if err != nil {
					fmt.Printf("Error: %v\n", err)
				} else {
					fmt.Printf("Table '%s' created successfully.\n", tableName)
				}
			} else {
				fmt.Println("Usage: CREATE TABLE tablename")
			}
			continue
		}
		
		// Execute SQL
		result := query.ExecuteSQL(dbWrapper, input)
		
		if result.Success {
			fmt.Println(result.Message)
			if len(result.Rows) > 0 {
				fmt.Println("\nResults:")
				for _, row := range result.Rows {
					fmt.Printf("  %v\n", row)
				}
			}
		} else {
			fmt.Printf("Error: %v\n", result.Error)
		}
	}
	
	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading input: %v\n", err)
	}
}
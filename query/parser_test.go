package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSelectStatement(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *SelectStatement
	}{
		{
			name:  "select all from table",
			input: "SELECT * FROM users",
			expected: &SelectStatement{
				Columns:   []string{"*"},
				TableName: "users",
			},
		},
		{
			name:  "select specific columns",
			input: "SELECT name, email FROM users",
			expected: &SelectStatement{
				Columns:   []string{"name", "email"},
				TableName: "users",
			},
		},
		{
			name:  "select with where clause",
			input: "SELECT * FROM users WHERE id = '123'",
			expected: &SelectStatement{
				Columns:   []string{"*"},
				TableName: "users",
				Where: &ComparisonExpression{
					Left:     "id",
					Operator: "=",
					Right:    "123",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQL(tt.input)
			assert.NoError(t, err)
			assert.IsType(t, &SelectStatement{}, stmt)
			
			selectStmt := stmt.(*SelectStatement)
			assert.Equal(t, tt.expected.Columns, selectStmt.Columns)
			assert.Equal(t, tt.expected.TableName, selectStmt.TableName)
			
			if tt.expected.Where != nil {
				assert.NotNil(t, selectStmt.Where)
			}
		})
	}
}

func TestParseInsertStatement(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *InsertStatement
	}{
		{
			name:  "insert with values",
			input: "INSERT INTO users VALUES ('john', 'john@example.com')",
			expected: &InsertStatement{
				TableName: "users",
				Values:    [][]string{{"john", "john@example.com"}},
			},
		},
		{
			name:  "insert with columns",
			input: "INSERT INTO users (name, email) VALUES ('john', 'john@example.com')",
			expected: &InsertStatement{
				TableName: "users",
				Columns:   []string{"name", "email"},
				Values:    [][]string{{"john", "john@example.com"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQL(tt.input)
			assert.NoError(t, err)
			assert.IsType(t, &InsertStatement{}, stmt)
			
			insertStmt := stmt.(*InsertStatement)
			assert.Equal(t, tt.expected.TableName, insertStmt.TableName)
			assert.Equal(t, tt.expected.Columns, insertStmt.Columns)
			assert.Equal(t, tt.expected.Values, insertStmt.Values)
		})
	}
}

func TestParseUpdateStatement(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *UpdateStatement
	}{
		{
			name:  "update with set",
			input: "UPDATE users SET name = 'john'",
			expected: &UpdateStatement{
				TableName: "users",
				Set:       map[string]string{"name": "john"},
			},
		},
		{
			name:  "update with where clause",
			input: "UPDATE users SET name = 'john' WHERE id = '123'",
			expected: &UpdateStatement{
				TableName: "users",
				Set:       map[string]string{"name": "john"},
				Where: &ComparisonExpression{
					Left:     "id",
					Operator: "=",
					Right:    "123",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQL(tt.input)
			assert.NoError(t, err)
			assert.IsType(t, &UpdateStatement{}, stmt)
			
			updateStmt := stmt.(*UpdateStatement)
			assert.Equal(t, tt.expected.TableName, updateStmt.TableName)
			assert.Equal(t, tt.expected.Set, updateStmt.Set)
			
			if tt.expected.Where != nil {
				assert.NotNil(t, updateStmt.Where)
			}
		})
	}
}

func TestParseDeleteStatement(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected *DeleteStatement
	}{
		{
			name:  "delete from table",
			input: "DELETE FROM users",
			expected: &DeleteStatement{
				TableName: "users",
			},
		},
		{
			name:  "delete with where clause",
			input: "DELETE FROM users WHERE id = '123'",
			expected: &DeleteStatement{
				TableName: "users",
				Where: &ComparisonExpression{
					Left:     "id",
					Operator: "=",
					Right:    "123",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseSQL(tt.input)
			assert.NoError(t, err)
			assert.IsType(t, &DeleteStatement{}, stmt)
			
			deleteStmt := stmt.(*DeleteStatement)
			assert.Equal(t, tt.expected.TableName, deleteStmt.TableName)
			
			if tt.expected.Where != nil {
				assert.NotNil(t, deleteStmt.Where)
			}
		})
	}
}

func TestLexer(t *testing.T) {
	input := "SELECT * FROM users WHERE id = '123'"
	
	expected := []TokenType{
		SELECT, ASTERISK, FROM, IDENTIFIER, WHERE, IDENTIFIER, EQUAL, STRING, EOF,
	}
	
	lexer := NewLexer(input)
	
	for i, expectedType := range expected {
		tok := lexer.NextToken()
		assert.Equal(t, expectedType, tok.Type, "token %d - expected %v, got %v", i, expectedType, tok.Type)
	}
}
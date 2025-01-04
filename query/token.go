package query

import "strings"

// TokenType represents the type of a SQL token
type TokenType int

const (
	// Special tokens
	ILLEGAL TokenType = iota
	EOF
	
	// Identifiers and literals
	IDENTIFIER
	STRING
	NUMBER
	
	// Keywords
	SELECT
	INSERT
	UPDATE
	DELETE
	FROM
	INTO
	VALUES
	SET
	WHERE
	AND
	OR
	
	// Operators and delimiters
	EQUAL      // =
	COMMA      // ,
	SEMICOLON  // ;
	LPAREN     // (
	RPAREN     // )
	ASTERISK   // *
)

// Token represents a SQL token
type Token struct {
	Type    TokenType
	Literal string
	Pos     int
}

// keywords maps string literals to their token types
var keywords = map[string]TokenType{
	"SELECT": SELECT,
	"INSERT": INSERT,
	"UPDATE": UPDATE,
	"DELETE": DELETE,
	"FROM":   FROM,
	"INTO":   INTO,
	"VALUES": VALUES,
	"SET":    SET,
	"WHERE":  WHERE,
	"AND":    AND,
	"OR":     OR,
}

// LookupIdent checks whether an identifier is a keyword
func LookupIdent(ident string) TokenType {
	if tok, ok := keywords[strings.ToUpper(ident)]; ok {
		return tok
	}
	return IDENTIFIER
}
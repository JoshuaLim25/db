package query

import (
	"fmt"
	"strings"
)

// Parser parses SQL statements
type Parser struct {
	l *Lexer
	
	curToken  Token
	peekToken Token
}

// New creates a new parser instance
func NewParser(l *Lexer) *Parser {
	p := &Parser{l: l}
	
	// Read two tokens, so curToken and peekToken are both set
	p.nextToken()
	p.nextToken()
	
	return p
}

// nextToken advances both curToken and peekToken
func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

// Parse parses a SQL statement and returns the AST
func (p *Parser) Parse() (Statement, error) {
	switch p.curToken.Type {
	case SELECT:
		return p.parseSelectStatement()
	case INSERT:
		return p.parseInsertStatement()
	case UPDATE:
		return p.parseUpdateStatement()
	case DELETE:
		return p.parseDeleteStatement()
	default:
		return nil, fmt.Errorf("unexpected token: %s", p.curToken.Literal)
	}
}

// parseSelectStatement parses a SELECT statement
func (p *Parser) parseSelectStatement() (*SelectStatement, error) {
	stmt := &SelectStatement{}
	
	// We're already on SELECT token, no need to expect it
	if p.curToken.Type != SELECT {
		return nil, fmt.Errorf("expected SELECT")
	}
	
	// Parse columns
	if p.peekToken.Type == ASTERISK {
		p.nextToken()
		stmt.Columns = []string{"*"}
	} else {
		columns, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = columns
	}
	
	// Expect FROM
	if !p.expectPeek(FROM) {
		return nil, fmt.Errorf("expected FROM")
	}
	
	// Parse table name
	if !p.expectPeek(IDENTIFIER) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.curToken.Literal
	
	// Optional WHERE clause
	if p.peekToken.Type == WHERE {
		p.nextToken()
		where, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}
	
	return stmt, nil
}

// parseInsertStatement parses an INSERT statement
func (p *Parser) parseInsertStatement() (*InsertStatement, error) {
	stmt := &InsertStatement{}
	
	// We're already on INSERT token, no need to expect it
	if p.curToken.Type != INSERT {
		return nil, fmt.Errorf("expected INSERT")
	}
	
	// Expect INTO
	if !p.expectPeek(INTO) {
		return nil, fmt.Errorf("expected INTO")
	}
	
	// Parse table name
	if !p.expectPeek(IDENTIFIER) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.curToken.Literal
	
	// Optional column list
	if p.peekToken.Type == LPAREN {
		p.nextToken() // consume (
		columns, err := p.parseColumnList()
		if err != nil {
			return nil, err
		}
		stmt.Columns = columns
		
		if !p.expectPeek(RPAREN) {
			return nil, fmt.Errorf("expected )")
		}
	}
	
	// Expect VALUES
	if !p.expectPeek(VALUES) {
		return nil, fmt.Errorf("expected VALUES")
	}
	
	// Parse values
	if !p.expectPeek(LPAREN) {
		return nil, fmt.Errorf("expected (")
	}
	
	values, err := p.parseValueList()
	if err != nil {
		return nil, err
	}
	stmt.Values = [][]string{values}
	
	if !p.expectPeek(RPAREN) {
		return nil, fmt.Errorf("expected )")
	}
	
	return stmt, nil
}

// parseUpdateStatement parses an UPDATE statement
func (p *Parser) parseUpdateStatement() (*UpdateStatement, error) {
	stmt := &UpdateStatement{Set: make(map[string]string)}
	
	// We're already on UPDATE token, no need to expect it
	if p.curToken.Type != UPDATE {
		return nil, fmt.Errorf("expected UPDATE")
	}
	
	// Parse table name
	if !p.expectPeek(IDENTIFIER) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.curToken.Literal
	
	// Expect SET
	if !p.expectPeek(SET) {
		return nil, fmt.Errorf("expected SET")
	}
	
	// Parse SET assignments
	for {
		if !p.expectPeek(IDENTIFIER) {
			return nil, fmt.Errorf("expected column name")
		}
		column := p.curToken.Literal
		
		if !p.expectPeek(EQUAL) {
			return nil, fmt.Errorf("expected =")
		}
		
		if !p.expectPeek(STRING) && !p.expectPeek(NUMBER) {
			return nil, fmt.Errorf("expected value")
		}
		value := p.curToken.Literal
		
		stmt.Set[column] = value
		
		if p.peekToken.Type != COMMA {
			break
		}
		p.nextToken() // consume comma
	}
	
	// Optional WHERE clause
	if p.peekToken.Type == WHERE {
		p.nextToken()
		where, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}
	
	return stmt, nil
}

// parseDeleteStatement parses a DELETE statement
func (p *Parser) parseDeleteStatement() (*DeleteStatement, error) {
	stmt := &DeleteStatement{}
	
	// We're already on DELETE token, no need to expect it
	if p.curToken.Type != DELETE {
		return nil, fmt.Errorf("expected DELETE")
	}
	
	// Expect FROM
	if !p.expectPeek(FROM) {
		return nil, fmt.Errorf("expected FROM")
	}
	
	// Parse table name
	if !p.expectPeek(IDENTIFIER) {
		return nil, fmt.Errorf("expected table name")
	}
	stmt.TableName = p.curToken.Literal
	
	// Optional WHERE clause
	if p.peekToken.Type == WHERE {
		p.nextToken()
		where, err := p.parseWhereClause()
		if err != nil {
			return nil, err
		}
		stmt.Where = where
	}
	
	return stmt, nil
}

// parseColumnList parses a comma-separated list of column names
func (p *Parser) parseColumnList() ([]string, error) {
	var columns []string
	
	if !p.expectPeek(IDENTIFIER) {
		return nil, fmt.Errorf("expected column name")
	}
	columns = append(columns, p.curToken.Literal)
	
	for p.peekToken.Type == COMMA {
		p.nextToken() // consume comma
		if !p.expectPeek(IDENTIFIER) {
			return nil, fmt.Errorf("expected column name")
		}
		columns = append(columns, p.curToken.Literal)
	}
	
	return columns, nil
}

// parseValueList parses a comma-separated list of values
func (p *Parser) parseValueList() ([]string, error) {
	var values []string
	
	if p.peekToken.Type != STRING && p.peekToken.Type != NUMBER {
		return nil, fmt.Errorf("expected value")
	}
	p.nextToken()
	values = append(values, p.curToken.Literal)
	
	for p.peekToken.Type == COMMA {
		p.nextToken() // consume comma
		if p.peekToken.Type != STRING && p.peekToken.Type != NUMBER {
			return nil, fmt.Errorf("expected value")
		}
		p.nextToken()
		values = append(values, p.curToken.Literal)
	}
	
	return values, nil
}

// parseWhereClause parses a WHERE clause
func (p *Parser) parseWhereClause() (Expression, error) {
	return p.parseExpression()
}

// parseExpression parses a SQL expression
func (p *Parser) parseExpression() (Expression, error) {
	left, err := p.parseComparisonExpression()
	if err != nil {
		return nil, err
	}
	
	for p.peekToken.Type == AND || p.peekToken.Type == OR {
		operator := p.peekToken.Literal
		p.nextToken() // consume operator
		
		right, err := p.parseComparisonExpression()
		if err != nil {
			return nil, err
		}
		
		left = &BinaryExpression{
			Left:     left,
			Operator: strings.ToUpper(operator),
			Right:    right,
		}
	}
	
	return left, nil
}

// parseComparisonExpression parses a comparison expression (col = 'value')
func (p *Parser) parseComparisonExpression() (Expression, error) {
	if !p.expectPeek(IDENTIFIER) {
		return nil, fmt.Errorf("expected column name")
	}
	left := p.curToken.Literal
	
	if !p.expectPeek(EQUAL) {
		return nil, fmt.Errorf("expected =")
	}
	operator := p.curToken.Literal
	
	if !p.expectPeek(STRING) && !p.expectPeek(NUMBER) {
		return nil, fmt.Errorf("expected value")
	}
	right := p.curToken.Literal
	
	return &ComparisonExpression{
		Left:     left,
		Operator: operator,
		Right:    right,
	}, nil
}

// expectPeek checks the peek token type and advances if it matches
func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekToken.Type == t {
		p.nextToken()
		return true
	}
	return false
}

// ParseSQL is a convenience function that parses a SQL string
func ParseSQL(sql string) (Statement, error) {
	lexer := NewLexer(sql)
	parser := NewParser(lexer)
	return parser.Parse()
}
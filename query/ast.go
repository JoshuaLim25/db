package query

// Statement represents a SQL statement
type Statement interface {
	String() string
}

// SelectStatement represents a SELECT query
type SelectStatement struct {
	Columns   []string // columns to select, "*" for all
	TableName string
	Where     Expression // optional WHERE clause
}

func (s *SelectStatement) String() string {
	return "SELECT"
}

// InsertStatement represents an INSERT query
type InsertStatement struct {
	TableName string
	Columns   []string
	Values    [][]string // each inner slice is a row of values
}

func (i *InsertStatement) String() string {
	return "INSERT"
}

// UpdateStatement represents an UPDATE query
type UpdateStatement struct {
	TableName string
	Set       map[string]string // column -> value mapping
	Where     Expression        // optional WHERE clause
}

func (u *UpdateStatement) String() string {
	return "UPDATE"
}

// DeleteStatement represents a DELETE query
type DeleteStatement struct {
	TableName string
	Where     Expression // optional WHERE clause
}

func (d *DeleteStatement) String() string {
	return "DELETE"
}

// Expression represents a SQL expression
type Expression interface {
	String() string
}

// ComparisonExpression represents a comparison (e.g., col = 'value')
type ComparisonExpression struct {
	Left     string
	Operator string
	Right    string
}

func (c *ComparisonExpression) String() string {
	return c.Left + " " + c.Operator + " " + c.Right
}

// BinaryExpression represents a binary operation (AND/OR)
type BinaryExpression struct {
	Left     Expression
	Operator string
	Right    Expression
}

func (b *BinaryExpression) String() string {
	return "(" + b.Left.String() + " " + b.Operator + " " + b.Right.String() + ")"
}
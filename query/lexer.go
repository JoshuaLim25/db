package query

// Lexer tokenizes SQL strings
type Lexer struct {
	input        string
	position     int  // current position in input (points to current char)
	readPosition int  // current reading position in input (after current char)
	ch           byte // current char under examination
}

// New creates a new lexer instance
func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()
	return l
}

// readChar reads the next character and advances our position in the input
func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0 // ASCII NUL character represents "EOF"
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
}

// peekChar returns the next character without advancing our position
func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}
	return l.input[l.readPosition]
}

// NextToken scans the input and returns the next token
func (l *Lexer) NextToken() Token {
	var tok Token
	
	l.skipWhitespace()
	
	switch l.ch {
	case '=':
		tok = Token{Type: EQUAL, Literal: string(l.ch), Pos: l.position}
	case ',':
		tok = Token{Type: COMMA, Literal: string(l.ch), Pos: l.position}
	case ';':
		tok = Token{Type: SEMICOLON, Literal: string(l.ch), Pos: l.position}
	case '(':
		tok = Token{Type: LPAREN, Literal: string(l.ch), Pos: l.position}
	case ')':
		tok = Token{Type: RPAREN, Literal: string(l.ch), Pos: l.position}
	case '*':
		tok = Token{Type: ASTERISK, Literal: string(l.ch), Pos: l.position}
	case '\'':
		tok.Type = STRING
		tok.Literal = l.readString()
		tok.Pos = l.position
		// Note: readString positions us at the closing quote, 
		// we need to advance past it for the next token
		l.readChar()
		return tok
	case 0:
		tok.Literal = ""
		tok.Type = EOF
		tok.Pos = l.position
	default:
		if isLetter(l.ch) {
			tok.Pos = l.position
			tok.Literal = l.readIdentifier()
			tok.Type = LookupIdent(tok.Literal)
			return tok
		} else if isDigit(l.ch) {
			tok.Type = NUMBER
			tok.Pos = l.position
			tok.Literal = l.readNumber()
			return tok
		} else {
			tok = Token{Type: ILLEGAL, Literal: string(l.ch), Pos: l.position}
		}
	}
	
	l.readChar()
	return tok
}

// readIdentifier reads an identifier or keyword
func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readNumber reads a numeric literal
func (l *Lexer) readNumber() string {
	position := l.position
	for isDigit(l.ch) {
		l.readChar()
	}
	return l.input[position:l.position]
}

// readString reads a string literal enclosed in single quotes
func (l *Lexer) readString() string {
	position := l.position + 1
	for {
		l.readChar()
		if l.ch == '\'' || l.ch == 0 {
			break
		}
	}
	return l.input[position:l.position]
}

// skipWhitespace skips whitespace characters
func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

// isLetter checks if the character is a letter
func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

// isDigit checks if the character is a digit
func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}
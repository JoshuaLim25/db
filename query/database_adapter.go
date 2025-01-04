package query

// DatabaseAdapter adapts the db.Database to the query.Database interface
type DatabaseAdapter struct {
	database DatabaseImpl
}

// DatabaseImpl represents the actual database implementation from the db package
type DatabaseImpl interface {
	GetTable(tableName string) (TableImpl, error)
	CreateTable(tableName string) (TableImpl, error)
}

// TableImpl represents the actual table implementation from the db package
type TableImpl interface {
	Insert(key, value []byte) error
	Select(key []byte) ([]byte, bool)
	Update(key, value []byte) error
	Delete(key []byte) error
	Scan(startKey []byte) IteratorImpl
	Name() string
}

// IteratorImpl represents the actual iterator implementation from the db package
type IteratorImpl interface {
	Next() (key, val []byte)
	ContainsNext() bool
}

// NewDatabaseAdapter creates a new database adapter
func NewDatabaseAdapter(db DatabaseImpl) *DatabaseAdapter {
	return &DatabaseAdapter{database: db}
}

// GetTable implements the Database interface
func (da *DatabaseAdapter) GetTable(tableName string) (Table, error) {
	table, err := da.database.GetTable(tableName)
	if err != nil {
		return nil, err
	}
	return &TableAdapter{table: table}, nil
}

// CreateTable implements the Database interface
func (da *DatabaseAdapter) CreateTable(tableName string) (Table, error) {
	table, err := da.database.CreateTable(tableName)
	if err != nil {
		return nil, err
	}
	return &TableAdapter{table: table}, nil
}

// TableAdapter adapts a db.Table to the query.Table interface
type TableAdapter struct {
	table TableImpl
}

// Insert implements the Table interface
func (ta *TableAdapter) Insert(key, value []byte) error {
	return ta.table.Insert(key, value)
}

// Select implements the Table interface
func (ta *TableAdapter) Select(key []byte) ([]byte, bool) {
	return ta.table.Select(key)
}

// Update implements the Table interface
func (ta *TableAdapter) Update(key, value []byte) error {
	return ta.table.Update(key, value)
}

// Delete implements the Table interface
func (ta *TableAdapter) Delete(key []byte) error {
	return ta.table.Delete(key)
}

// Scan implements the Table interface
func (ta *TableAdapter) Scan(startKey []byte) Iterator {
	iter := ta.table.Scan(startKey)
	return &IteratorAdapter{iterator: iter}
}

// Name implements the Table interface
func (ta *TableAdapter) Name() string {
	return ta.table.Name()
}

// IteratorAdapter adapts a storage.Iterator to the query.Iterator interface
type IteratorAdapter struct {
	iterator IteratorImpl
}

// Next implements the Iterator interface
func (ia *IteratorAdapter) Next() (key, val []byte) {
	return ia.iterator.Next()
}

// ContainsNext implements the Iterator interface
func (ia *IteratorAdapter) ContainsNext() bool {
	return ia.iterator.ContainsNext()
}
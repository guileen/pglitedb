package interfaces

// IndexDefinitionInterface defines the minimal interface for an index definition
type IndexDefinitionInterface interface {
	GetID() int64
	GetColumns() []string
}

// TableDefinitionInterface defines the minimal interface for a table definition
type TableDefinitionInterface interface {
	GetIndexes() []IndexDefinitionInterface
}

// IndexDefinition implements IndexDefinitionInterface
type IndexDefinition struct {
	ID      int64    `json:"id"`
	Columns []string `json:"columns"`
}

// GetID returns the index ID
func (i *IndexDefinition) GetID() int64 {
	return i.ID
}

// GetColumns returns the index columns
func (i *IndexDefinition) GetColumns() []string {
	return i.Columns
}

// TableDefinition implements TableDefinitionInterface
type TableDefinition struct {
	Indexes []IndexDefinitionInterface `json:"indexes"`
}

// GetIndexes returns the table indexes
func (t *TableDefinition) GetIndexes() []IndexDefinitionInterface {
	return t.Indexes
}
package interfaces

// Value represents a column value
type Value struct {
	Data interface{}
	Type string
}

// RecordInterface defines the minimal interface for a record needed by filter evaluation
type RecordInterface interface {
	GetData() map[string]*Value
}
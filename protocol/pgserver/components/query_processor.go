package components

// QueryProcessorInterface defines the interface for query processing
type QueryProcessorInterface interface {
	ProcessQuery(query string) (interface{}, error)
}
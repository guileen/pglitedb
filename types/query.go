package types

import (
	"time"
)

// QueryOptions represents query options
// Used by both internal components and external clients
type QueryOptions struct {
	Columns []string               `json:"columns,omitempty"`
	Where   map[string]interface{} `json:"where,omitempty"`
	OrderBy []string               `json:"order_by,omitempty"`
	Limit   *int                   `json:"limit,omitempty"`
	Offset  *int                   `json:"offset,omitempty"`
	Include []string               `json:"include,omitempty"` // Related tables to include
	Count   bool                   `json:"count,omitempty"`
	Head    bool                   `json:"head,omitempty"` // Return only count
	Debug   bool                   `json:"debug,omitempty"`
}

// QueryResult represents the result of a query
// Used by both internal components and external clients
type QueryResult struct {
	// Basic fields for external clients
	Rows    []map[string]interface{} `json:"rows"`
	Count   int64                    `json:"count,omitempty"`
	HasMore bool                     `json:"has_more,omitempty"`
	
	// Extended fields for internal use
	Records  interface{}   `json:"-"` // Will be []*table.Record in implementation
	Duration time.Duration `json:"-"` 
	Query    string        `json:"-"`
	Params   []interface{} `json:"-"`
	Debug    interface{}   `json:"-"`
	Limit    *int          `json:"-"`
	Offset   *int          `json:"-"`
}
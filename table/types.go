package table

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// ColumnType represents the data type of a table column
type ColumnType string

const (
	ColumnTypeString    ColumnType = "string"
	ColumnTypeNumber    ColumnType = "number"
	ColumnTypeBoolean   ColumnType = "boolean"
	ColumnTypeDate      ColumnType = "date"
	ColumnTypeTimestamp ColumnType = "timestamp"
	ColumnTypeJSON      ColumnType = "json"
	ColumnTypeUUID      ColumnType = "uuid"
	ColumnTypeText      ColumnType = "text"
	ColumnTypeBinary    ColumnType = "binary"
)

// ColumnDefinition defines a table column
type ColumnDefinition struct {
	Name        string     `json:"name" yaml:"name"`
	Type        ColumnType `json:"type" yaml:"type"`
	Nullable    bool       `json:"nullable" yaml:"nullable"`
	PrimaryKey  bool       `json:"primary_key" yaml:"primary_key"`
	Unique      bool       `json:"unique" yaml:"unique"`
	Default     *Value     `json:"default,omitempty" yaml:"default,omitempty"`
	MaxLength   *int       `json:"max_length,omitempty" yaml:"max_length,omitempty"`
	Precision   *int       `json:"precision,omitempty" yaml:"precision,omitempty"`
	Scale       *int       `json:"scale,omitempty" yaml:"scale,omitempty"`
	Description string     `json:"description,omitempty" yaml:"description,omitempty"`
}

// Value represents a column value
type Value struct {
	Data interface{} `json:"data"`
	Type ColumnType  `json:"type"`
}

// Scan implements sql.Scanner interface
func (v *Value) Scan(value interface{}) error {
	if value == nil {
		v.Data = nil
		return nil
	}

	switch val := value.(type) {
	case string:
		v.Data = val
		if v.Type == "" {
			v.Type = ColumnTypeString
		}
	case []byte:
		v.Data = string(val)
		if v.Type == "" {
			v.Type = ColumnTypeString
		}
	case int, int32, int64, float32, float64:
		v.Data = val
		if v.Type == "" {
			v.Type = ColumnTypeNumber
		}
	case bool:
		v.Data = val
		if v.Type == "" {
			v.Type = ColumnTypeBoolean
		}
	case time.Time:
		v.Data = val
		if v.Type == "" {
			v.Type = ColumnTypeTimestamp
		}
	default:
		v.Data = val
		if v.Type == "" {
			v.Type = ColumnTypeJSON
		}
	}

	return nil
}

// Value implements driver.Valuer interface
func (v Value) Value() (driver.Value, error) {
	return v.Data, nil
}

// MarshalJSON implements json.Marshaler
func (v Value) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.Data)
}

// UnmarshalJSON implements json.Unmarshaler
func (v *Value) UnmarshalJSON(data []byte) error {
	return json.Unmarshal(data, &v.Data)
}

// TableDefinition represents a complete table definition
type TableDefinition struct {
	ID          string             `json:"id" yaml:"id"`
	Name        string             `json:"name" yaml:"name"`
	Schema      string             `json:"schema,omitempty" yaml:"schema,omitempty"`
	Columns     []ColumnDefinition `json:"columns" yaml:"columns"`
	Indexes     []IndexDefinition  `json:"indexes,omitempty" yaml:"indexes,omitempty"`
	Constraints []ConstraintDef    `json:"constraints,omitempty" yaml:"constraints,omitempty"`
	RLS         bool               `json:"rls" yaml:"rls"`
	Description string             `json:"description,omitempty" yaml:"description,omitempty"`
	Settings    TableSettings      `json:"settings,omitempty" yaml:"settings,omitempty"`
	CreatedAt   time.Time          `json:"created_at" yaml:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at" yaml:"updated_at"`
	Version     int                `json:"version" yaml:"version"`
}

// IndexDefinition defines a table index
type IndexDefinition struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
	Type    string   `json:"type"` // "btree", "hash", "gin", "gist"
}

// ConstraintDef defines a table constraint
type ConstraintDef struct {
	Name            string        `json:"name"`
	Type            string        `json:"type"` // "foreign_key", "check", "unique"
	Columns         []string      `json:"columns"`
	Reference       *ReferenceDef `json:"reference,omitempty"`
	CheckExpression string        `json:"check_expression,omitempty"`
}

// ReferenceDef defines a foreign key reference
type ReferenceDef struct {
	Table    string   `json:"table"`
	Columns  []string `json:"columns"`
	OnDelete string   `json:"on_delete"`
	OnUpdate string   `json:"on_update"`
}

// TableSettings contains table-specific settings
type TableSettings struct {
	EnableRLS        bool                   `json:"enable_rls"`
	RowLevelSecurity map[string][]RLSPolicy `json:"row_level_security,omitempty"`
	Triggers         []TriggerDef           `json:"triggers,omitempty"`
	Audit            AuditSettings          `json:"audit,omitempty"`
	Cache            CacheSettings          `json:"cache,omitempty"`
}

// RLSPolicy defines a row-level security policy
type RLSPolicy struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Using       string    `json:"using"` // SELECT policy
	Check       string    `json:"check"` // INSERT/UPDATE/DELETE policy
	Roles       []string  `json:"roles"`
	Apply       string    `json:"apply"` // "all", "select", "insert", "update", "delete"
	Description string    `json:"description"`
	Enabled     bool      `json:"enabled"`
	CreatedAt   time.Time `json:"created_at"`
}

// TriggerDef defines a database trigger
type TriggerDef struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Table     string    `json:"table"`
	Events    []string  `json:"events"`   // "INSERT", "UPDATE", "DELETE"
	Timing    string    `json:"timing"`   // "BEFORE", "AFTER", "INSTEAD OF"
	Function  string    `json:"function"` // Function to call
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

// AuditSettings defines audit configuration
type AuditSettings struct {
	Enabled    bool     `json:"enabled"`
	Operations []string `json:"operations"` // "INSERT", "UPDATE", "DELETE", "SELECT"
	Columns    []string `json:"columns"`    // Columns to track, empty = all
	Retention  string   `json:"retention"`  // How long to keep audit logs
}

// CacheSettings defines cache configuration
type CacheSettings struct {
	Enabled      bool     `json:"enabled"`
	TTL          string   `json:"ttl"`           // Cache TTL
	MaxSize      int      `json:"max_size"`      // Maximum cache size
	InvalidateOn []string `json:"invalidate_on"` // When to invalidate cache
}

// Record represents a table record
type Record struct {
	ID        string                 `json:"id"`
	Table     string                 `json:"table"`
	Data      map[string]*Value      `json:"data"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`
	Version   int                    `json:"version"`
}

// QueryBuilder builds database queries
type QueryBuilder interface {
	Select(columns ...string) QueryBuilder
	Where(condition string, args ...interface{}) QueryBuilder
	WhereIn(column string, values []interface{}) QueryBuilder
	WhereNull(column string) QueryBuilder
	WhereNotNull(column string) QueryBuilder
	OrderBy(column string, direction ...string) QueryBuilder
	GroupBy(columns ...string) QueryBuilder
	Having(condition string, args ...interface{}) QueryBuilder
	Limit(limit int) QueryBuilder
	Offset(offset int) QueryBuilder
	Join(table, condition string) QueryBuilder
	LeftJoin(table, condition string) QueryBuilder
	RightJoin(table, condition string) QueryBuilder

	Build() (string, []interface{}, error)
	Count() (int64, error)
	First() (*Record, error)
	Get() ([]*Record, error)
	Create(data map[string]interface{}) (*Record, error)
	CreateBatch(records []map[string]interface{}) ([]*Record, error)
	Update(data map[string]interface{}) error
	Delete() error
}

// Migration represents a database migration
type Migration struct {
	ID          string     `json:"id"`
	Version     string     `json:"version"`
	Name        string     `json:"name"`
	Description string     `json:"description"`
	UpSQL       string     `json:"up_sql"`
	DownSQL     string     `json:"down_sql"`
	Checksum    string     `json:"checksum"`
	Applied     bool       `json:"applied"`
	AppliedAt   *time.Time `json:"applied_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
}

// OperationType represents the type of operation
type OperationType string

const (
	OperationTypeInsert OperationType = "INSERT"
	OperationTypeUpdate OperationType = "UPDATE"
	OperationTypeDelete OperationType = "DELETE"
	OperationTypeSelect OperationType = "SELECT"
)

// ChangeLog tracks changes to records
type ChangeLog struct {
	ID        string                 `json:"id"`
	Table     string                 `json:"table"`
	RecordID  string                 `json:"record_id"`
	Operation OperationType          `json:"operation"`
	UserID    string                 `json:"user_id,omitempty"`
	Before    map[string]*Value      `json:"before,omitempty"`
	After     map[string]*Value      `json:"after,omitempty"`
	Changed   []string               `json:"changed"` // List of changed columns
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt time.Time              `json:"created_at"`
}

// ValidationRule defines a field validation rule
type ValidationRule struct {
	Name    string      `json:"name"`
	Rule    string      `json:"rule"`    // "required", "min_length", "max_length", "regex", "unique", etc.
	Value   interface{} `json:"value"`   // Rule parameter
	Message string      `json:"message"` // Error message
	Enabled bool        `json:"enabled"`
}

// ColumnValidation extends column definition with validation
type ColumnValidation struct {
	ColumnDefinition
	Validations []ValidationRule `json:"validations,omitempty"`
}

// TableSchema represents the complete table schema
type TableSchema struct {
	Definition    TableDefinition    `json:"definition"`
	Validations   []ColumnValidation `json:"validations,omitempty"`
	Relationships []Relationship     `json:"relationships,omitempty"`
	CreatedAt     time.Time          `json:"created_at"`
}

// Relationship defines table relationships
type Relationship struct {
	ID            string `json:"id"`
	Type          string `json:"type"` // "one_to_one", "one_to_many", "many_to_many"
	SourceTable   string `json:"source_table"`
	TargetTable   string `json:"target_table"`
	SourceColumn  string `json:"source_column"`
	TargetColumn  string `json:"target_column"`
	JunctionTable string `json:"junction_table,omitempty"`
	OnDelete      string `json:"on_delete"` // "cascade", "restrict", "set_null", "set_default"
	Description   string `json:"description"`
}

// QueryOptions represents query options
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
type QueryResult struct {
	Records  []*Record     `json:"records"`
	Count    int64         `json:"count,omitempty"`
	Limit    *int          `json:"limit,omitempty"`
	Offset   *int          `json:"offset,omitempty"`
	HasMore  bool          `json:"has_more,omitempty"`
	Duration time.Duration `json:"duration"`
	Query    string        `json:"query,omitempty"`
	Params   []interface{} `json:"params,omitempty"`
	Debug    interface{}   `json:"debug,omitempty"`
}

// Error types
var (
	ErrTableNotFound    = fmt.Errorf("table not found")
	ErrColumnNotFound   = fmt.Errorf("column not found")
	ErrRecordNotFound   = fmt.Errorf("record not found")
	ErrDuplicateRecord  = fmt.Errorf("duplicate record")
	ErrInvalidOperation = fmt.Errorf("invalid operation")
	ErrPermissionDenied = fmt.Errorf("permission denied")
	ErrValidationFailed = fmt.Errorf("validation failed")
)

// TypeError represents a type conversion or validation error
type TypeError struct {
	Message string
}

func (e *TypeError) Error() string {
	return e.Message
}

// Helper functions

// GetColumnTypeFromGoType maps Go types to column types
func GetColumnTypeFromGoType(typ string) ColumnType {
	switch typ {
	case "string":
		return ColumnTypeString
	case "int", "int32", "int64", "float32", "float64":
		return ColumnTypeNumber
	case "bool":
		return ColumnTypeBoolean
	case "time.Time":
		return ColumnTypeTimestamp
	default:
		return ColumnTypeJSON
	}
}

// IsValidColumnType checks if a column type is valid
func IsValidColumnType(typ ColumnType) bool {
	switch typ {
	case ColumnTypeString, ColumnTypeNumber, ColumnTypeBoolean,
		ColumnTypeDate, ColumnTypeTimestamp, ColumnTypeJSON,
		ColumnTypeUUID, ColumnTypeText, ColumnTypeBinary:
		return true
	default:
		return false
	}
}

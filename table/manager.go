package table

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Manager manages database tables and operations
type Manager struct {
	db      *sql.DB
	cache   CacheInterface
	rls     RLSInterface
	audit   AuditInterface
	schemas map[string]*TableSchema
	mu      sync.RWMutex
}

// Config represents the table manager configuration
type Config struct {
	DatabaseURL    string        `yaml:"database_url"`
	MaxConnections int           `yaml:"max_connections"`
	EnableCache    bool          `yaml:"enable_cache"`
	CacheTTL       time.Duration `yaml:"cache_ttl"`
	EnableRLS      bool          `yaml:"enable_rls"`
	EnableAudit    bool          `yaml:"enable_audit"`
	AutoMigrate    bool          `yaml:"auto_migrate"`
	DefaultSchema  string        `yaml:"default_schema"`
	MigrationPath  string        `yaml:"migration_path"`
}

// NewManager creates a new table manager
func NewManager(config *Config) (*Manager, error) {
	// Connect to database
	db, err := sql.Open("sqlite3", config.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(config.MaxConnections)
	db.SetMaxIdleConns(config.MaxConnections / 2)

	manager := &Manager{
		db:      db,
		schemas: make(map[string]*TableSchema),
	}

	// Note: Storage integration will be added later

	// Initialize cache if enabled
	if config.EnableCache {
		manager.cache = NewMemoryCache(config.CacheTTL)
	}

	// Initialize RLS if enabled
	if config.EnableRLS {
		manager.rls = NewRLSEngine(db)
	}

	// Initialize audit if enabled
	if config.EnableAudit {
		manager.audit = NewAuditLogger(db)
	}

	// Load existing schemas
	if err := manager.loadSchemas(); err != nil {
		return nil, fmt.Errorf("failed to load schemas: %w", err)
	}

	return manager, nil
}

// CreateTable creates a new table
func (m *Manager) CreateTable(ctx context.Context, schema *TableDefinition) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Validate schema
	if err := m.validateSchema(schema); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}

	// Generate CREATE TABLE SQL
	sql, err := m.generateCreateTableSQL(schema)
	if err != nil {
		return fmt.Errorf("failed to generate SQL: %w", err)
	}

	// Execute SQL
	if _, err := m.db.ExecContext(ctx, sql); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Create indexes
	for _, index := range schema.Indexes {
		if err := m.createIndex(ctx, schema.Name, index); err != nil {
			return fmt.Errorf("failed to create index %s: %w", index.Name, err)
		}
	}

	// Create constraints
	for _, constraint := range schema.Constraints {
		if err := m.createConstraint(ctx, schema.Name, constraint); err != nil {
			return fmt.Errorf("failed to create constraint %s: %w", constraint.Name, err)
		}
	}

	// Enable RLS if configured
	if schema.RLS {
		if _, err := m.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", schema.Name)); err != nil {
			return fmt.Errorf("failed to enable RLS: %w", err)
		}
	}

	// Store schema
	tableSchema := &TableSchema{
		Definition: *schema,
		CreatedAt:  time.Now(),
	}
	m.schemas[schema.Name] = tableSchema

	// Save schema to storage
	if err := m.saveSchema(tableSchema); err != nil {
		return fmt.Errorf("failed to save schema: %w", err)
	}

	return nil
}

// GetTable retrieves a table schema
func (m *Manager) GetTable(ctx context.Context, name string) (*TableSchema, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	schema, exists := m.schemas[name]
	if !exists {
		return nil, ErrTableNotFound
	}

	return schema, nil
}

// ListTables returns all table schemas
func (m *Manager) ListTables(ctx context.Context) ([]*TableSchema, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	schemas := make([]*TableSchema, 0, len(m.schemas))
	for _, schema := range m.schemas {
		schemas = append(schemas, schema)
	}

	return schemas, nil
}

// DropTable drops a table
func (m *Manager) DropTable(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.schemas[name]; !exists {
		return ErrTableNotFound
	}

	// Drop table
	if _, err := m.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name)); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// Remove from memory
	delete(m.schemas, name)

	// Remove from storage
	if err := m.deleteSchema(name); err != nil {
		return fmt.Errorf("failed to delete schema: %w", err)
	}

	return nil
}

// AlterTable modifies an existing table
func (m *Manager) AlterTable(ctx context.Context, name string, alterations []Alteration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	schema, exists := m.schemas[name]
	if !exists {
		return ErrTableNotFound
	}

	// Apply alterations
	for _, alteration := range alterations {
		if err := m.applyAlteration(ctx, schema, alteration); err != nil {
			return fmt.Errorf("failed to apply alteration: %w", err)
		}
	}

	// Update schema version
	schema.Definition.Version++
	schema.Definition.UpdatedAt = time.Now()

	// Save updated schema
	if err := m.saveSchema(schema); err != nil {
		return fmt.Errorf("failed to save updated schema: %w", err)
	}

	return nil
}

// Query creates a new query builder for a table
func (m *Manager) Query(table string) QueryBuilder {
	return NewQueryBuilder(m, table)
}

// Insert inserts a new record
func (m *Manager) Insert(ctx context.Context, table string, data map[string]interface{}) (*Record, error) {
	// Get table schema
	schema, err := m.GetTable(ctx, table)
	if err != nil {
		return nil, err
	}

	// Validate data
	if err := m.validateRecord(schema, data); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Check RLS policies
	if m.rls != nil && schema.Definition.RLS {
		if allowed, err := m.rls.CheckInsertPermission(ctx, table, data); err != nil {
			return nil, fmt.Errorf("RLS check failed: %w", err)
		} else if !allowed {
			return nil, ErrPermissionDenied
		}
	}

	// Generate ID if not provided
	if _, exists := data["id"]; !exists {
		data["id"] = generateUUID()
	}

	// Set timestamps
	now := time.Now()
	data["created_at"] = now
	data["updated_at"] = now

	// Build INSERT query
	query := NewQueryBuilder(m, table)
	record, err := query.Create(data)
	if err != nil {
		return nil, fmt.Errorf("failed to insert record: %w", err)
	}

	// Audit log
	if m.audit != nil {
		if err := m.audit.LogInsert(ctx, table, record.ID, data); err != nil {
			// Log error but don't fail the operation
			fmt.Printf("Failed to log insert: %v\n", err)
		}
	}

	// Invalidate cache
	if m.cache != nil {
		m.cache.Invalidate(table)
	}

	return record, nil
}

// Update updates a record
func (m *Manager) Update(ctx context.Context, table string, id string, data map[string]interface{}) (*Record, error) {
	// Get table schema
	schema, err := m.GetTable(ctx, table)
	if err != nil {
		return nil, err
	}

	// Get existing record
	existing, err := m.Query(table).Where("id = ?", id).First()
	if err != nil {
		return nil, fmt.Errorf("failed to get existing record: %w", err)
	}

	// Validate data
	if err := m.validateRecord(schema, data); err != nil {
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	// Check RLS policies
	if m.rls != nil && schema.Definition.RLS {
		if allowed, err := m.rls.CheckUpdatePermission(ctx, table, id, data); err != nil {
			return nil, fmt.Errorf("RLS check failed: %w", err)
		} else if !allowed {
			return nil, ErrPermissionDenied
		}
	}

	// Set updated timestamp
	data["updated_at"] = time.Now()

	// Build UPDATE query
	query := NewQueryBuilder(m, table).Where("id = ?", id)
	if err := query.Update(data); err != nil {
		return nil, fmt.Errorf("failed to update record: %w", err)
	}

	// Get updated record
	updated, err := query.First()
	if err != nil {
		return nil, fmt.Errorf("failed to get updated record: %w", err)
	}

	// Audit log
	if m.audit != nil {
		// Convert Value maps to interface maps for audit
		beforeData := make(map[string]interface{})
		for k, v := range existing.Data {
			beforeData[k] = v.Data
		}
		afterData := make(map[string]interface{})
		for k, v := range updated.Data {
			afterData[k] = v.Data
		}
		if err := m.audit.LogUpdate(ctx, table, id, beforeData, afterData); err != nil {
			// Log error but don't fail the operation
			fmt.Printf("Failed to log update: %v\n", err)
		}
	}

	// Invalidate cache
	if m.cache != nil {
		m.cache.Invalidate(table)
	}

	return updated, nil
}

// Delete deletes a record
func (m *Manager) Delete(ctx context.Context, table string, id string) error {
	// Get table schema
	schema, err := m.GetTable(ctx, table)
	if err != nil {
		return err
	}

	// Get existing record for audit
	existing, err := m.Query(table).Where("id = ?", id).First()
	if err != nil && err != ErrRecordNotFound {
		return fmt.Errorf("failed to get existing record: %w", err)
	}

	// Check RLS policies
	if m.rls != nil && schema.Definition.RLS {
		if allowed, err := m.rls.CheckDeletePermission(ctx, table, id); err != nil {
			return fmt.Errorf("RLS check failed: %w", err)
		} else if !allowed {
			return ErrPermissionDenied
		}
	}

	// Build DELETE query
	query := NewQueryBuilder(m, table).Where("id = ?", id)
	if err := query.Delete(); err != nil {
		return fmt.Errorf("failed to delete record: %w", err)
	}

	// Audit log
	if m.audit != nil && existing != nil {
		// Convert Value map to interface map for audit
		data := make(map[string]interface{})
		for k, v := range existing.Data {
			data[k] = v.Data
		}
		if err := m.audit.LogDelete(ctx, table, id, data); err != nil {
			// Log error but don't fail the operation
			fmt.Printf("Failed to log delete: %v\n", err)
		}
	}

	// Invalidate cache
	if m.cache != nil {
		m.cache.Invalidate(table)
	}

	return nil
}

// Select queries records from a table
func (m *Manager) Select(ctx context.Context, table string, options *QueryOptions) (*QueryResult, error) {
	// Get table schema
	schema, err := m.GetTable(ctx, table)
	if err != nil {
		return nil, err
	}

	// Build query
	query := NewQueryBuilder(m, table)

	// Apply options
	if options != nil {
		if len(options.Columns) > 0 {
			query = query.Select(options.Columns...)
		}
		if options.Where != nil {
			for column, value := range options.Where {
				query = query.Where(fmt.Sprintf("%s = ?", column), value)
			}
		}
		if len(options.OrderBy) > 0 {
			for _, orderBy := range options.OrderBy {
				parts := strings.Split(orderBy, " ")
				if len(parts) == 2 {
					query = query.OrderBy(parts[0], parts[1])
				} else {
					query = query.OrderBy(parts[0])
				}
			}
		}
		if options.Limit != nil {
			query = query.Limit(*options.Limit)
		}
		if options.Offset != nil {
			query = query.Offset(*options.Offset)
		}
	}

	// Check RLS policies for SELECT
	if m.rls != nil && schema.Definition.RLS {
		// This would integrate with the query builder to add RLS clauses
		if err := m.rls.ApplySelectPolicies(ctx, table, query); err != nil {
			return nil, fmt.Errorf("failed to apply RLS policies: %w", err)
		}
	}

	// Execute query
	start := time.Now()
	records, err := query.Get()
	duration := time.Since(start)

	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Build result
	result := &QueryResult{
		Records:  records,
		Duration: duration,
	}

	// Get count if requested
	if options != nil && options.Count {
		count, err := query.Count()
		if err != nil {
			return nil, fmt.Errorf("failed to get count: %w", err)
		}
		result.Count = count
	}

	// Set pagination info
	if options != nil && options.Limit != nil && options.Offset != nil {
		result.Limit = options.Limit
		result.Offset = options.Offset
		if result.Count > 0 {
			result.HasMore = int64(*options.Offset)+int64(len(records)) < result.Count
		}
	}

	// Audit log for SELECT if audit is enabled
	if m.audit != nil {
		if err := m.audit.LogSelect(ctx, table, options); err != nil {
			// Log error but don't fail the operation
			fmt.Printf("Failed to log select: %v\n", err)
		}
	}

	return result, nil
}

// Close closes the table manager and cleans up resources
func (m *Manager) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

// Private helper methods

func (m *Manager) validateSchema(schema *TableDefinition) error {
	if schema.Name == "" {
		return fmt.Errorf("table name is required")
	}

	if len(schema.Columns) == 0 {
		return fmt.Errorf("table must have at least one column")
	}

	// Check for primary key
	hasPrimaryKey := false
	for _, col := range schema.Columns {
		if col.PrimaryKey {
			hasPrimaryKey = true
			break
		}
	}

	if !hasPrimaryKey {
		// Add default id column
		schema.Columns = append([]ColumnDefinition{{
			Name:       "id",
			Type:       ColumnTypeUUID,
			PrimaryKey: true,
			Nullable:   false,
			Unique:     true,
		}}, schema.Columns...)
	}

	return nil
}

func (m *Manager) generateCreateTableSQL(schema *TableDefinition) (string, error) {
	var columns []string

	for _, col := range schema.Columns {
		columnDef := fmt.Sprintf("%s %s", col.Name, m.getColumnTypeSQL(col.Type))

		if !col.Nullable {
			columnDef += " NOT NULL"
		}

		if col.Unique {
			columnDef += " UNIQUE"
		}

		if col.Default != nil {
			columnDef += fmt.Sprintf(" DEFAULT %s", m.getDefaultValueSQL(col.Default))
		}

		columns = append(columns, columnDef)
	}

	sql := fmt.Sprintf("CREATE TABLE %s (\n  %s\n)",
		schema.Name,
		strings.Join(columns, ",\n  "))

	return sql, nil
}

func (m *Manager) getColumnTypeSQL(typ ColumnType) string {
	switch typ {
	case ColumnTypeString:
		return "TEXT"
	case ColumnTypeNumber:
		return "REAL"
	case ColumnTypeBoolean:
		return "BOOLEAN"
	case ColumnTypeDate, ColumnTypeTimestamp:
		return "TIMESTAMP"
	case ColumnTypeJSON:
		return "JSON"
	case ColumnTypeUUID:
		return "TEXT"
	case ColumnTypeText:
		return "TEXT"
	case ColumnTypeBinary:
		return "BLOB"
	default:
		return "TEXT"
	}
}

func (m *Manager) getDefaultValueSQL(value *Value) string {
	if value == nil || value.Data == nil {
		return "NULL"
	}

	switch v := value.Data.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Interfaces for dependencies

type CacheInterface interface {
	Get(key string) (interface{}, bool)
	Set(key string, value interface{}, ttl time.Duration)
	Invalidate(pattern string)
}

type RLSInterface interface {
	CheckInsertPermission(ctx context.Context, table string, data map[string]interface{}) (bool, error)
	CheckUpdatePermission(ctx context.Context, table string, id string, data map[string]interface{}) (bool, error)
	CheckDeletePermission(ctx context.Context, table string, id string) (bool, error)
	ApplySelectPolicies(ctx context.Context, table string, query QueryBuilder) error
}

type AuditInterface interface {
	LogInsert(ctx context.Context, table string, id string, data map[string]interface{}) error
	LogUpdate(ctx context.Context, table string, id string, before, after map[string]interface{}) error
	LogDelete(ctx context.Context, table string, id string, data map[string]interface{}) error
	LogSelect(ctx context.Context, table string, options *QueryOptions) error
}

// generateUUID generates a new UUID
func generateUUID() string {
	return uuid.New().String()
}

// NewMemoryCache creates a simple memory cache implementation
func NewMemoryCache(ttl time.Duration) CacheInterface {
	return &memoryCache{
		items: make(map[string]*cacheItem),
		ttl:   ttl,
	}
}

type memoryCache struct {
	items map[string]*cacheItem
	mu    sync.RWMutex
	ttl   time.Duration
}

type cacheItem struct {
	value  interface{}
	expiry time.Time
}

func (c *memoryCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	item, exists := c.items[key]
	if !exists || time.Now().After(item.expiry) {
		return nil, false
	}

	return item.value, true
}

func (c *memoryCache) Set(key string, value interface{}, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ttl == 0 {
		ttl = c.ttl
	}

	c.items[key] = &cacheItem{
		value:  value,
		expiry: time.Now().Add(ttl),
	}
}

func (c *memoryCache) Invalidate(pattern string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key := range c.items {
		if strings.Contains(key, pattern) {
			delete(c.items, key)
		}
	}
}

// NewRLSEngine creates a basic RLS engine implementation
func NewRLSEngine(db *sql.DB) RLSInterface {
	return &rlsEngine{db: db}
}

type rlsEngine struct {
	db *sql.DB
}

func (r *rlsEngine) CheckInsertPermission(ctx context.Context, table string, data map[string]interface{}) (bool, error) {
	// Basic implementation - always allow for now
	// In production, this would check RLS policies
	return true, nil
}

func (r *rlsEngine) CheckUpdatePermission(ctx context.Context, table string, id string, data map[string]interface{}) (bool, error) {
	return true, nil
}

func (r *rlsEngine) CheckDeletePermission(ctx context.Context, table string, id string) (bool, error) {
	return true, nil
}

func (r *rlsEngine) ApplySelectPolicies(ctx context.Context, table string, query QueryBuilder) error {
	// Basic implementation - no additional filtering
	return nil
}

// NewAuditLogger creates a basic audit logger implementation
func NewAuditLogger(db *sql.DB) AuditInterface {
	return &auditLogger{db: db}
}

type auditLogger struct {
	db *sql.DB
}

func (a *auditLogger) LogInsert(ctx context.Context, table string, id string, data map[string]interface{}) error {
	// Basic implementation - log to console for now
	fmt.Printf("AUDIT: INSERT %s %s\n", table, id)
	return nil
}

func (a *auditLogger) LogUpdate(ctx context.Context, table string, id string, before, after map[string]interface{}) error {
	fmt.Printf("AUDIT: UPDATE %s %s\n", table, id)
	return nil
}

func (a *auditLogger) LogDelete(ctx context.Context, table string, id string, data map[string]interface{}) error {
	fmt.Printf("AUDIT: DELETE %s %s\n", table, id)
	return nil
}

func (a *auditLogger) LogSelect(ctx context.Context, table string, options *QueryOptions) error {
	// Optional: can be noisy, so we might not log all selects
	return nil
}

// loadSchemas loads existing table schemas from storage
func (m *Manager) loadSchemas() error {
	// For now, we'll initialize with empty schemas
	// In a real implementation, this would load from a metadata table
	return nil
}

// createIndex creates a database index
func (m *Manager) createIndex(ctx context.Context, tableName string, index IndexDefinition) error {
	var indexType string
	if index.Type != "" {
		indexType = " USING " + index.Type
	}

	sql := fmt.Sprintf("CREATE %s INDEX %s ON %s (%s)",
		map[bool]string{true: "UNIQUE", false: ""}[index.Unique],
		index.Name,
		tableName,
		strings.Join(index.Columns, ", "))

	if indexType != "" {
		sql += indexType
	}

	_, err := m.db.ExecContext(ctx, sql)
	return err
}

// createConstraint creates a database constraint
func (m *Manager) createConstraint(ctx context.Context, tableName string, constraint ConstraintDef) error {
	var sql string
	switch constraint.Type {
	case "foreign_key":
		if constraint.Reference == nil {
			return fmt.Errorf("foreign key constraint requires reference")
		}
		sql = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE %s ON UPDATE %s",
			tableName, constraint.Name, strings.Join(constraint.Columns, ","),
			constraint.Reference.Table, strings.Join(constraint.Reference.Columns, ","),
			constraint.Reference.OnDelete, constraint.Reference.OnUpdate)
	case "unique":
		sql = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s UNIQUE (%s)",
			tableName, constraint.Name, strings.Join(constraint.Columns, ","))
	case "check":
		sql = fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s)",
			tableName, constraint.Name, constraint.CheckExpression)
	default:
		return fmt.Errorf("unsupported constraint type: %s", constraint.Type)
	}

	_, err := m.db.ExecContext(ctx, sql)
	return err
}

// saveSchema saves table schema to storage
func (m *Manager) saveSchema(schema *TableSchema) error {
	// For now, schemas are stored in memory
	// In a real implementation, this would save to a metadata table
	return nil
}

// deleteSchema removes table schema from storage
func (m *Manager) deleteSchema(tableName string) error {
	// For now, schemas are stored in memory
	// In a real implementation, this would delete from a metadata table
	return nil
}

// applyAlteration applies a table alteration
func (m *Manager) applyAlteration(ctx context.Context, schema *TableSchema, alteration Alteration) error {
	// This would implement different types of alterations
	// For now, return a placeholder error
	return fmt.Errorf("alterations not yet implemented: %T", alteration)
}

// validateRecord validates a record against schema
func (m *Manager) validateRecord(schema *TableSchema, data map[string]interface{}) error {
	// Basic validation - check required fields
	for _, col := range schema.Definition.Columns {
		if !col.Nullable {
			if _, exists := data[col.Name]; !exists {
				return fmt.Errorf("required field %s is missing", col.Name)
			}
		}
	}
	return nil
}

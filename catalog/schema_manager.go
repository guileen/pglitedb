package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/guileen/pglitedb/catalog/internal"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

const (
	schemaKeyPrefix = "\x00schema\x00"
	viewKeyPrefix   = "\x00view\x00"
)

type schemaManager struct {
	engine engine.StorageEngine
	kv     storage.KV
	cache  *internal.SchemaCache
}

func newSchemaManager(eng engine.StorageEngine, kv storage.KV, cache *internal.SchemaCache) SchemaManager {
	return &schemaManager{
		engine: eng,
		kv:     kv,
		cache:  cache,
	}
}

func (m *schemaManager) CreateTable(ctx context.Context, tenantID int64, def *types.TableDefinition) error {
	key := makeTableKey(tenantID, def.Name)
	
	if _, _, exists := m.cache.Get(key); exists {
		return fmt.Errorf("table %s already exists", def.Name)
	}

	// Process SERIAL types and convert them to their underlying types
	for i := range def.Columns {
		col := &def.Columns[i]
		if types.IsSerialType(col.Type) {
			// Convert SERIAL to underlying integer type
			col.Type = types.MapSerialType(col.Type)
			// Mark as auto-increment (no need to add PrimaryKey here, assume it's set by caller)
		}
		
		if !types.IsValidColumnType(col.Type) {
			return fmt.Errorf("invalid column type '%s' for column '%s'", col.Type, col.Name)
		}
	}

	tableID, err := m.engine.NextTableID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("generate table id: %w", err)
	}

	def.CreatedAt = time.Now()
	def.UpdatedAt = time.Now()
	if def.Version == 0 {
		def.Version = 1
	}

	if m.kv != nil {
		if err := m.persistSchema(ctx, tenantID, def.Name, def); err != nil {
			return err
		}
	}

	m.cache.Set(key, def, tableID)

	return nil
}

func (m *schemaManager) DropTable(ctx context.Context, tenantID int64, tableName string) error {
	key := makeTableKey(tenantID, tableName)
	
	if _, _, exists := m.cache.Get(key); !exists {
		return types.ErrTableNotFound
	}

	if m.kv != nil {
		schemaKey := []byte(fmt.Sprintf("%s%d:%s", schemaKeyPrefix, tenantID, tableName))
		if err := m.kv.Delete(ctx, schemaKey); err != nil {
			return fmt.Errorf("delete schema: %w", err)
		}
	}

	m.cache.Delete(key)

	return nil
}

func (m *schemaManager) GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*types.TableDefinition, error) {
	key := makeTableKey(tenantID, tableName)
	
	schema, _, exists := m.cache.Get(key)
	if !exists {
		return nil, types.ErrTableNotFound
	}

	return schema, nil
}

func (m *schemaManager) AlterTable(ctx context.Context, tenantID int64, tableName string, changes *AlterTableChanges) error {
	key := makeTableKey(tenantID, tableName)
	
	schema, tableID, exists := m.cache.Get(key)
	if !exists {
		return types.ErrTableNotFound
	}

	newDef := *schema

	if changes.AddColumns != nil {
		for _, col := range changes.AddColumns {
			if !types.IsValidColumnType(col.Type) {
				return fmt.Errorf("invalid column type '%s' for column '%s'", col.Type, col.Name)
			}
		}
		newDef.Columns = append(newDef.Columns, changes.AddColumns...)
	}

	if changes.DropColumns != nil {
		newColumns := make([]types.ColumnDefinition, 0, len(newDef.Columns))
		for _, col := range newDef.Columns {
			drop := false
			for _, dropName := range changes.DropColumns {
				if col.Name == dropName {
					drop = true
					break
				}
			}
			if !drop {
				newColumns = append(newColumns, col)
			}
		}
		newDef.Columns = newColumns
	}

	if changes.ModifyColumns != nil {
		// Update existing columns with new definitions
		for _, modifiedCol := range changes.ModifyColumns {
			found := false
			for i := range newDef.Columns {
				if newDef.Columns[i].Name == modifiedCol.Name {
					// Validate the new column type
					if !types.IsValidColumnType(modifiedCol.Type) {
						return fmt.Errorf("invalid column type '%s' for column '%s'", modifiedCol.Type, modifiedCol.Name)
					}
					newDef.Columns[i] = modifiedCol
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("column '%s' not found", modifiedCol.Name)
			}
		}
	}

	if changes.AddIndexes != nil {
		newDef.Indexes = append(newDef.Indexes, changes.AddIndexes...)
	}

	if changes.DropIndexes != nil {
		newIndexes := make([]types.IndexDefinition, 0, len(newDef.Indexes))
		for _, idx := range newDef.Indexes {
			drop := false
			for _, dropName := range changes.DropIndexes {
				if idx.Name == dropName {
					drop = true
					break
				}
			}
			if !drop {
				newIndexes = append(newIndexes, idx)
			}
		}
		newDef.Indexes = newIndexes
	}

	// Handle constraint changes
	if changes.AddConstraints != nil {
		newDef.Constraints = append(newDef.Constraints, changes.AddConstraints...)
	}

	if changes.DropConstraints != nil {
		newConstraints := make([]types.ConstraintDef, 0, len(newDef.Constraints))
		for _, constraint := range newDef.Constraints {
			drop := false
			for _, dropName := range changes.DropConstraints {
				if constraint.Name == dropName {
					drop = true
					break
				}
			}
			if !drop {
				newConstraints = append(newConstraints, constraint)
			}
		}
		newDef.Constraints = newConstraints
	}

	newDef.Version++
	newDef.UpdatedAt = time.Now()

	if m.kv != nil {
		if err := m.persistSchema(ctx, tenantID, tableName, &newDef); err != nil {
			return err
		}
	}

	m.cache.Set(key, &newDef, tableID)

	return nil
}

func (m *schemaManager) ListTables(ctx context.Context, tenantID int64) ([]*types.TableDefinition, error) {
	var tables []*types.TableDefinition
	prefix := fmt.Sprintf("%d:", tenantID)

	m.cache.Range(func(key string, schema *types.TableDefinition, tableID int64) bool {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			tables = append(tables, schema)
		}
		return true
	})

	return tables, nil
}

func (m *schemaManager) LoadSchemas(ctx context.Context) error {
	if m.kv == nil {
		return nil
	}

	iter := m.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: []byte(schemaKeyPrefix),
		UpperBound: []byte(schemaKeyPrefix + "\xff"),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var def types.TableDefinition
		if err := json.Unmarshal(iter.Value(), &def); err != nil {
			return fmt.Errorf("unmarshal schema: %w", err)
		}

		keyStr := string(iter.Key())
		keyStr = keyStr[len(schemaKeyPrefix):]

		var tenantID int64
		var tableName string
		if _, err := fmt.Sscanf(keyStr, "%d:%s", &tenantID, &tableName); err != nil {
			return fmt.Errorf("parse schema key: %w", err)
		}

		key := makeTableKey(tenantID, def.Name)

		tableID, err := m.engine.NextTableID(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("generate table id: %w", err)
		}

		m.cache.Set(key, &def, tableID-1)
	}

	return iter.Error()
}

func (m *schemaManager) persistSchema(ctx context.Context, tenantID int64, tableName string, def *types.TableDefinition) error {
	schemaBytes, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("marshal schema: %w", err)
	}

	schemaKey := []byte(fmt.Sprintf("%s%d:%s", schemaKeyPrefix, tenantID, tableName))
	if err := m.kv.Set(ctx, schemaKey, schemaBytes); err != nil {
		return fmt.Errorf("persist schema: %w", err)
	}

	return nil
}

// ValidateConstraint validates a constraint against the table schema
func (m *schemaManager) ValidateConstraint(ctx context.Context, tenantID int64, tableName string, constraint *types.ConstraintDef) error {
	schema, _, err := m.getTableDefinition(tenantID, tableName)
	if err != nil {
		return err
	}

	switch constraint.Type {
	case "foreign_key":
		if constraint.Reference == nil {
			return fmt.Errorf("foreign key constraint must have a reference")
		}
		
		// Check that all columns exist in the table
		for _, col := range constraint.Columns {
			found := false
			for _, tableCol := range schema.Columns {
				if tableCol.Name == col {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("column '%s' not found in table", col)
			}
		}
		
		// Check that referenced table exists
		refSchema, _, err := m.getTableDefinition(tenantID, constraint.Reference.Table)
		if err != nil {
			return fmt.Errorf("referenced table '%s' not found", constraint.Reference.Table)
		}
		
		// Check that referenced columns exist
		for _, refCol := range constraint.Reference.Columns {
			found := false
			for _, tableCol := range refSchema.Columns {
				if tableCol.Name == refCol {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("referenced column '%s' not found in table '%s'", refCol, constraint.Reference.Table)
			}
		}
	case "check":
		if constraint.CheckExpression == "" {
			return fmt.Errorf("check constraint must have an expression")
		}
		// Basic validation - in a real implementation, we would parse and validate the expression
	case "unique":
		// Check that all columns exist in the table
		for _, col := range constraint.Columns {
			found := false
			for _, tableCol := range schema.Columns {
				if tableCol.Name == col {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("column '%s' not found in table", col)
			}
		}
	case "primary_key":
		// Check that all columns exist in the table
		for _, col := range constraint.Columns {
			found := false
			for _, tableCol := range schema.Columns {
				if tableCol.Name == col {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("column '%s' not found in table", col)
			}
		}
		
		// Check that the columns don't already have a primary key constraint
		for _, existingConstraint := range schema.Constraints {
			if existingConstraint.Type == "primary_key" {
				return fmt.Errorf("table already has a primary key constraint")
			}
		}
	default:
		return fmt.Errorf("unsupported constraint type: %s", constraint.Type)
	}

	return nil
}

// ValidateAllConstraints validates all constraints in a table
func (m *schemaManager) ValidateAllConstraints(ctx context.Context, tenantID int64, tableName string) error {
	schema, _, err := m.getTableDefinition(tenantID, tableName)
	if err != nil {
		return err
	}

	// Validate each constraint
	for _, constraint := range schema.Constraints {
		if err := m.ValidateConstraint(ctx, tenantID, tableName, &constraint); err != nil {
			return fmt.Errorf("constraint '%s' validation failed: %w", constraint.Name, err)
		}
	}

	return nil
}

// getTableDefinition is a helper method to get table definition
func (m *schemaManager) getTableDefinition(tenantID int64, tableName string) (*types.TableDefinition, int64, error) {
	key := makeTableKey(tenantID, tableName)
	
	schema, tableID, exists := m.cache.Get(key)
	if !exists {
		return nil, 0, types.ErrTableNotFound
	}
	
	return schema, tableID, nil
}

func makeTableKey(tenantID int64, tableName string) string {
	return fmt.Sprintf("%d:%s", tenantID, tableName)
}

// CreateView creates a new view
func (m *schemaManager) CreateView(ctx context.Context, tenantID int64, viewName string, query string, replace bool) error {
	key := makeViewKey(tenantID, viewName)
	
	// Check if view already exists
	exists := m.cache.Exists(key)
	if exists && !replace {
		return fmt.Errorf("view %s already exists", viewName)
	}
	
	viewDef := &types.ViewDefinition{
		Name:      viewName,
		Query:     query,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	
	if m.kv != nil {
		if err := m.persistView(ctx, tenantID, viewName, viewDef); err != nil {
			return err
		}
	}
	
	// For views, we'll use a simple ID scheme
	viewID := int64(0) // Views don't need real IDs in our system
	m.cache.Set(key, viewDef, viewID)
	
	return nil
}

// DropView drops an existing view
func (m *schemaManager) DropView(ctx context.Context, tenantID int64, viewName string) error {
	key := makeViewKey(tenantID, viewName)
	
	if !m.cache.Exists(key) {
		return fmt.Errorf("view %s not found", viewName)
	}
	
	if m.kv != nil {
		viewKey := []byte(fmt.Sprintf("%s%d:%s", viewKeyPrefix, tenantID, viewName))
		if err := m.kv.Delete(ctx, viewKey); err != nil {
			return fmt.Errorf("delete view: %w", err)
		}
	}
	
	m.cache.Delete(key)
	
	return nil
}

// GetViewDefinition retrieves a view definition
func (m *schemaManager) GetViewDefinition(ctx context.Context, tenantID int64, viewName string) (*types.ViewDefinition, error) {
	key := makeViewKey(tenantID, viewName)
	
	viewDef, exists := m.cache.GetView(key)
	if !exists {
		return nil, fmt.Errorf("view %s not found", viewName)
	}
	
	return viewDef, nil
}

// persistView persists a view definition to storage
func (m *schemaManager) persistView(ctx context.Context, tenantID int64, viewName string, def *types.ViewDefinition) error {
	viewBytes, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("marshal view: %w", err)
	}

	viewKey := []byte(fmt.Sprintf("%s%d:%s", viewKeyPrefix, tenantID, viewName))
	if err := m.kv.Set(ctx, viewKey, viewBytes); err != nil {
		return fmt.Errorf("persist view: %w", err)
	}

	return nil
}

func makeViewKey(tenantID int64, viewName string) string {
	return fmt.Sprintf("%d:%s", tenantID, viewName)
}
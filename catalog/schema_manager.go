package catalog

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/guileen/pglitedb/catalog/errors"
	"github.com/guileen/pglitedb/catalog/internal"
	"github.com/guileen/pglitedb/catalog/persistence"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

// mapPostgreSQLTypeToInternal maps PostgreSQL type names to internal column types
func mapPostgreSQLTypeToInternal(pgType string) string {
	originalType := pgType
	switch pgType {
	case "int4":
		pgType = "integer"
	case "int2":
		pgType = "smallint"
	case "int8":
		pgType = "bigint"
	case "float4":
		pgType = "real"
	case "float8":
		pgType = "double"
	case "bool":
		pgType = "boolean"
	case "varchar", "character varying":
		pgType = "varchar"
	case "char", "character", "bpchar":
		pgType = "char"
	case "timestamp", "timestamp without time zone":
		pgType = "timestamp"
	case "serial":
		pgType = "serial"
	case "bigserial":
		pgType = "bigserial"
	case "smallserial":
		pgType = "smallserial"
	default:
		// Return the original type if no mapping is found
	}
	
	// Debug logging
	if originalType != pgType {
		fmt.Printf("Mapped PostgreSQL type '%s' to internal type '%s'\n", originalType, pgType)
	}
	
	return pgType
}

const (
	schemaKeyPrefix = "\x00schema\x00"
	viewKeyPrefix   = "\x00view\x00"
)

type schemaManager struct {
	idGen     engineTypes.IDGeneration
	kv        storage.KV
	cache     *internal.SchemaCache
	persister *persistence.Persister
}

func newSchemaManager(idGen engineTypes.IDGeneration, kv storage.KV, cache *internal.SchemaCache) SchemaManager {
	return &schemaManager{
		idGen:     idGen,
		kv:        kv,
		cache:     cache,
		persister: persistence.NewPersister(kv),
	}
}

func (m *schemaManager) CreateTable(ctx context.Context, tenantID int64, def *types.TableDefinition) error {
	key := makeTableKey(tenantID, def.Name)
	
	if _, _, exists := m.cache.Get(key); exists {
		return errors.ErrTableAlreadyExists
	}

	// Process SERIAL types and convert them to their underlying types
	for i := range def.Columns {
		col := &def.Columns[i]
		if types.IsSerialType(col.Type) {
			// Convert SERIAL to underlying integer type
			col.Type = types.MapSerialType(col.Type)
			// Mark as auto-increment (no need to add PrimaryKey here, assume it's set by caller)
		}
		
		// Map PostgreSQL type names to our internal type names
		originalType := string(col.Type)
		mappedType := mapPostgreSQLTypeToInternal(originalType)
		col.Type = types.ColumnType(mappedType)
		
		// Debug logging
		if originalType != mappedType {
			fmt.Printf("Mapped column type from '%s' to '%s' for column '%s'\n", originalType, mappedType, col.Name)
		}
		
		fmt.Printf("Validating column '%s' with type '%s'\n", col.Name, col.Type)
		if !types.IsValidColumnType(col.Type) {
			fmt.Printf("Invalid column type '%s' for column '%s'\n", col.Type, col.Name)
			return errors.Wrap(nil, "invalid_column_type", "invalid column type '%s' for column '%s'", col.Type, col.Name)
		}
	}

	tableID, err := m.idGen.NextTableID(ctx, tenantID)
	if err != nil {
		return errors.Wrap(err, "generate_table_id_failed", "generate table id: %v", err)
	}

	def.CreatedAt = time.Now()
	def.UpdatedAt = time.Now()
	if def.Version == 0 {
		def.Version = 1
	}

	if m.kv != nil {
		if err := m.persister.PersistSchema(ctx, tenantID, def.Name, def); err != nil {
			return errors.Wrap(err, "persist_schema_failed", "persist schema failed: %v", err)
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
		if err := m.persister.DeleteSchema(ctx, tenantID, tableName); err != nil {
			return errors.Wrap(err, "delete_schema_failed", "delete schema: %v", err)
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
				return errors.Wrap(nil, "invalid_column_type", "invalid column type '%s' for column '%s'", col.Type, col.Name)
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
						return errors.Wrap(nil, "invalid_column_type", "invalid column type '%s' for column '%s'", modifiedCol.Type, modifiedCol.Name)
					}
					newDef.Columns[i] = modifiedCol
					found = true
					break
				}
			}
			if !found {
				return errors.Wrap(nil, "column_not_found", "column '%s' not found", modifiedCol.Name)
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
		if err := m.persister.PersistSchema(ctx, tenantID, tableName, &newDef); err != nil {
			return errors.Wrap(err, "persist_schema_failed", "persist schema failed: %v", err)
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

	return m.persister.LoadSchemas(ctx, func(tenantID int64, tableName string, def *types.TableDefinition) error {
		key := makeTableKey(tenantID, tableName)

		tableID, err := m.idGen.NextTableID(ctx, tenantID)
		if err != nil {
			log.Printf("Warning: failed to generate table id, skipping: %v", err)
			return nil
		}

		m.cache.Set(key, def, tableID-1)
		log.Printf("Loaded schema for table %s (tenant %d)", tableName, tenantID)
		return nil
	})
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
			return errors.ErrInvalidConstraint
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
				return errors.Wrap(nil, "column_not_found", "referenced column '%s' not found in table '%s'", refCol, constraint.Reference.Table)
			}
		}
	case "check":
		if constraint.CheckExpression == "" {
			return errors.ErrInvalidConstraint
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
				return errors.ErrColumnNotFound
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
				return errors.ErrColumnNotFound
			}
		}
		
		// Check that the columns don't already have a primary key constraint
		for _, existingConstraint := range schema.Constraints {
			if existingConstraint.Type == "primary_key" {
				return errors.ErrPrimaryKeyConstraintViolation
			}
		}
	default:
		return errors.Wrap(nil, "unsupported_constraint_type", "unsupported constraint type: %s", constraint.Type)
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
			return errors.Wrap(err, "constraint_validation_failed", "constraint '%s' validation failed: %v", constraint.Name, err)
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
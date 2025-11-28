package informationschema

import (
	"context"
	
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/guileen/pglitedb/types"
)

// Provider implements the InformationSchemaProvider interface
type Provider struct {
	manager interfaces.TableManager
}

// NewProvider creates a new information_schema provider
func NewProvider(manager interfaces.TableManager) *Provider {
	return &Provider{
		manager: manager,
	}
}

// QueryTables implements the information_schema.tables query
func (p *Provider) QueryTables(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	
	tables, err := p.manager.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		if filterTableName, ok := filter["table_name"].(string); ok {
			if table.Name != filterTableName {
				continue
			}
		}
		
		row := []interface{}{
			"def",
			"public",
			table.Name,
			"BASE TABLE",
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
		}
		rows = append(rows, row)
	}
	
	columns := []types.ColumnInfo{
		{Name: "table_catalog", Type: types.ColumnTypeText},
		{Name: "table_schema", Type: types.ColumnTypeText},
		{Name: "table_name", Type: types.ColumnTypeText},
		{Name: "table_type", Type: types.ColumnTypeText},
		{Name: "self_referencing_column_name", Type: types.ColumnTypeText},
		{Name: "reference_generation", Type: types.ColumnTypeText},
		{Name: "user_defined_type_catalog", Type: types.ColumnTypeText},
		{Name: "user_defined_type_schema", Type: types.ColumnTypeText},
		{Name: "user_defined_type_name", Type: types.ColumnTypeText},
		{Name: "is_insertable_into", Type: types.ColumnTypeText},
		{Name: "is_typed", Type: types.ColumnTypeText},
	}
	
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// QueryColumns implements the information_schema.columns query
func (p *Provider) QueryColumns(ctx context.Context, filter map[string]interface{}) (*types.QueryResult, error) {
	tenantID := int64(1)
	
	filterTableName, hasTableFilter := filter["table_name"].(string)
	
	tables, err := p.manager.ListTables(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	
	rows := make([][]interface{}, 0)
	for _, table := range tables {
		if hasTableFilter && table.Name != filterTableName {
			continue
		}
		
		tableSchema, err := p.manager.GetTableDefinition(ctx, tenantID, table.Name)
		if err != nil {
			continue
		}
		
		for ordinal, col := range tableSchema.Columns {
			dataType := mapTypeToSQL(col.Type)
			
			row := []interface{}{
				"def",
				"public",
				table.Name,
				col.Name,
				ordinal + 1,
				col.Default,
				boolToYesNo(col.Nullable),
				dataType,
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			}
			rows = append(rows, row)
		}
	}
	
	columns := []types.ColumnInfo{
		{Name: "table_catalog", Type: types.ColumnTypeText},
		{Name: "table_schema", Type: types.ColumnTypeText},
		{Name: "table_name", Type: types.ColumnTypeText},
		{Name: "column_name", Type: types.ColumnTypeText},
		{Name: "ordinal_position", Type: types.ColumnTypeInteger},
		{Name: "column_default", Type: types.ColumnTypeText},
		{Name: "is_nullable", Type: types.ColumnTypeText},
		{Name: "data_type", Type: types.ColumnTypeText},
		{Name: "character_maximum_length", Type: types.ColumnTypeInteger},
		{Name: "character_octet_length", Type: types.ColumnTypeInteger},
		{Name: "numeric_precision", Type: types.ColumnTypeInteger},
		{Name: "numeric_scale", Type: types.ColumnTypeInteger},
		{Name: "datetime_precision", Type: types.ColumnTypeInteger},
		{Name: "character_set_name", Type: types.ColumnTypeText},
	}
	
	return &types.QueryResult{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// mapTypeToSQL maps internal column types to SQL standard types
func mapTypeToSQL(colType types.ColumnType) string {
	switch colType {
	case types.ColumnTypeInteger:
		return "integer"
	case types.ColumnTypeBigInt:
		return "bigint"
	case types.ColumnTypeSmallInt:
		return "smallint"
	case types.ColumnTypeText:
		return "text"
	case types.ColumnTypeVarchar:
		return "character varying"
	case types.ColumnTypeBoolean:
		return "boolean"
	case types.ColumnTypeTimestamp:
		return "timestamp without time zone"
	case types.ColumnTypeNumeric:
		return "numeric"
	case types.ColumnTypeJSONB:
		return "jsonb"
	default:
		return "text"
	}
}

// boolToYesNo converts a boolean value to YES/NO string
func boolToYesNo(val bool) string {
	if val {
		return "YES"
	}
	return "NO"
}
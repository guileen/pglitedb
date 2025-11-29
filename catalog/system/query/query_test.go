package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSystemTableQuery(t *testing.T) {
	t.Run("ValidInformationSchemaTables", func(t *testing.T) {
		// Test parsing valid information_schema.tables query
		query := ParseSystemTableQuery("information_schema.tables")
		assert.NotNil(t, query)
		assert.Equal(t, "information_schema", query.Schema)
		assert.Equal(t, "tables", query.TableName)
		assert.Nil(t, query.Filter)
	})

	t.Run("ValidPgCatalogTables", func(t *testing.T) {
		// Test parsing valid pg_catalog.pg_tables query
		query := ParseSystemTableQuery("pg_catalog.pg_tables")
		assert.NotNil(t, query)
		assert.Equal(t, "pg_catalog", query.Schema)
		assert.Equal(t, "pg_tables", query.TableName)
		assert.Nil(t, query.Filter)
	})

	t.Run("ValidInformationSchemaColumns", func(t *testing.T) {
		// Test parsing valid information_schema.columns query
		query := ParseSystemTableQuery("information_schema.columns")
		assert.NotNil(t, query)
		assert.Equal(t, "information_schema", query.Schema)
		assert.Equal(t, "columns", query.TableName)
		assert.Nil(t, query.Filter)
	})

	t.Run("ValidPgCatalogColumns", func(t *testing.T) {
		// Test parsing valid pg_catalog.pg_columns query
		query := ParseSystemTableQuery("pg_catalog.pg_columns")
		assert.NotNil(t, query)
		assert.Equal(t, "pg_catalog", query.Schema)
		assert.Equal(t, "pg_columns", query.TableName)
		assert.Nil(t, query.Filter)
	})

	t.Run("InvalidFormatSinglePart", func(t *testing.T) {
		// Test parsing invalid single part query
		query := ParseSystemTableQuery("invalid")
		assert.Nil(t, query)
	})

	t.Run("InvalidFormatThreeParts", func(t *testing.T) {
		// Test parsing invalid three part query
		query := ParseSystemTableQuery("schema.table.extra")
		assert.Nil(t, query)
	})

	t.Run("EmptyString", func(t *testing.T) {
		// Test parsing empty string
		query := ParseSystemTableQuery("")
		assert.Nil(t, query)
	})

	t.Run("OnlyDot", func(t *testing.T) {
		// Test parsing only dot
		query := ParseSystemTableQuery(".")
		assert.NotNil(t, query) // Current implementation accepts this as valid: "".""
		assert.Equal(t, "", query.Schema)
		assert.Equal(t, "", query.TableName)
	})

	t.Run("LeadingDot", func(t *testing.T) {
		// Test parsing leading dot
		query := ParseSystemTableQuery(".table")
		assert.NotNil(t, query) // Current implementation accepts this as valid: ""."table"
		assert.Equal(t, "", query.Schema)
		assert.Equal(t, "table", query.TableName)
	})

	t.Run("TrailingDot", func(t *testing.T) {
		// Test parsing trailing dot
		query := ParseSystemTableQuery("schema.")
		assert.NotNil(t, query) // Current implementation accepts this as valid: "schema".""
		assert.Equal(t, "schema", query.Schema)
		assert.Equal(t, "", query.TableName)
	})

	t.Run("MultipleDots", func(t *testing.T) {
		// Test parsing multiple dots
		query := ParseSystemTableQuery("schema..table")
		assert.Nil(t, query)
	})

	t.Run("WhitespaceHandling", func(t *testing.T) {
		// Test parsing with whitespace
		query := ParseSystemTableQuery(" information_schema . tables ")
		assert.NotNil(t, query) // Whitespace is not trimmed in current implementation
		assert.Equal(t, " information_schema ", query.Schema)
		assert.Equal(t, " tables ", query.TableName)
	})

	t.Run("CaseSensitivity", func(t *testing.T) {
		// Test case sensitivity
		query := ParseSystemTableQuery("INFORMATION_SCHEMA.TABLES")
		assert.NotNil(t, query)
		assert.Equal(t, "INFORMATION_SCHEMA", query.Schema)
		assert.Equal(t, "TABLES", query.TableName)
	})

	t.Run("SpecialCharacters", func(t *testing.T) {
		// Test special characters
		query := ParseSystemTableQuery("schema-name.table_name")
		assert.NotNil(t, query)
		assert.Equal(t, "schema-name", query.Schema)
		assert.Equal(t, "table_name", query.TableName)
	})

	t.Run("NumericCharacters", func(t *testing.T) {
		// Test numeric characters
		query := ParseSystemTableQuery("schema1.table2")
		assert.NotNil(t, query)
		assert.Equal(t, "schema1", query.Schema)
		assert.Equal(t, "table2", query.TableName)
	})

	t.Run("UnderscoreHandling", func(t *testing.T) {
		// Test underscore handling
		query := ParseSystemTableQuery("pg_catalog.pg_stat_user_tables")
		assert.NotNil(t, query)
		assert.Equal(t, "pg_catalog", query.Schema)
		assert.Equal(t, "pg_stat_user_tables", query.TableName)
	})
}

func TestSystemTableQueryStructure(t *testing.T) {
	t.Run("SystemTableQueryFields", func(t *testing.T) {
		// Test that SystemTableQuery struct has the expected fields
		query := &SystemTableQuery{
			Schema:    "test_schema",
			TableName: "test_table",
			Filter:    map[string]interface{}{"key": "value"},
		}

		assert.Equal(t, "test_schema", query.Schema)
		assert.Equal(t, "test_table", query.TableName)
		assert.Equal(t, map[string]interface{}{"key": "value"}, query.Filter)
	})

	t.Run("SystemTableQueryEmptyFilter", func(t *testing.T) {
		// Test SystemTableQuery with nil filter
		query := &SystemTableQuery{
			Schema:    "test_schema",
			TableName: "test_table",
			Filter:    nil,
		}

		assert.Equal(t, "test_schema", query.Schema)
		assert.Equal(t, "test_table", query.TableName)
		assert.Nil(t, query.Filter)
	})
}
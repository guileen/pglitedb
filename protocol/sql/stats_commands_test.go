package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestAnalyzeStatementParsing tests the parsing of ANALYZE statements
func TestAnalyzeStatementParsing(t *testing.T) {
	parser := NewDDLParser()
	
	// Test ANALYZE table_name;
	ddlStmt, err := parser.Parse("ANALYZE users;")
	assert.NoError(t, err)
	assert.Equal(t, AnalyzeStatementType, ddlStmt.Type)
	
	analyzeStmt, ok := ddlStmt.Statement.(*AnalyzeStatement)
	assert.True(t, ok)
	assert.Equal(t, "users", analyzeStmt.TableName)
	assert.False(t, analyzeStmt.AllTables)
	assert.Empty(t, analyzeStmt.Columns)
	
	// Test ANALYZE table_name (column1, column2);
	ddlStmt, err = parser.Parse("ANALYZE users (id, name);")
	assert.NoError(t, err)
	assert.Equal(t, AnalyzeStatementType, ddlStmt.Type)
	
	analyzeStmt, ok = ddlStmt.Statement.(*AnalyzeStatement)
	assert.True(t, ok)
	assert.Equal(t, "users", analyzeStmt.TableName)
	assert.False(t, analyzeStmt.AllTables)
	assert.ElementsMatch(t, []string{"id", "name"}, analyzeStmt.Columns)
	
	// Test ANALYZE; (all tables)
	ddlStmt, err = parser.Parse("ANALYZE;")
	assert.NoError(t, err)
	assert.Equal(t, AnalyzeStatementType, ddlStmt.Type)
	
	analyzeStmt, ok = ddlStmt.Statement.(*AnalyzeStatement)
	assert.True(t, ok)
	assert.Empty(t, analyzeStmt.TableName)
	assert.True(t, analyzeStmt.AllTables)
	assert.Empty(t, analyzeStmt.Columns)
}

// TestAnalyzeStatementStructure tests the structure of AnalyzeStatement
func TestAnalyzeStatementStructure(t *testing.T) {
	analyzeStmt := &AnalyzeStatement{
		Query:      "ANALYZE users (id, name);",
		TableName:  "users",
		Columns:    []string{"id", "name"},
		AllTables:  false,
	}
	
	assert.Equal(t, "ANALYZE users (id, name);", analyzeStmt.Query)
	assert.Equal(t, "users", analyzeStmt.TableName)
	assert.Equal(t, []string{"id", "name"}, analyzeStmt.Columns)
	assert.False(t, analyzeStmt.AllTables)
	
	// Test all tables case
	allTablesStmt := &AnalyzeStatement{
		Query:      "ANALYZE;",
		TableName:  "",
		Columns:    []string{},
		AllTables:  true,
	}
	
	assert.Equal(t, "ANALYZE;", allTablesStmt.Query)
	assert.Empty(t, allTablesStmt.TableName)
	assert.Empty(t, allTablesStmt.Columns)
	assert.True(t, allTablesStmt.AllTables)
}
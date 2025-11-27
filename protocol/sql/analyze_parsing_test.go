package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAnalyzeCommandParsing(t *testing.T) {
	// Test ANALYZE command parsing
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
package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/guileen/pglitedb/protocol/sql/parser"
)

func TestAnalyzeCommandParsing(t *testing.T) {
	// Test ANALYZE command parsing
	ddlParser := NewDDLParser()
	
	// Test ANALYZE table_name;
	ddlStmt, err := ddlParser.Parse("ANALYZE users;")
	assert.NoError(t, err)
	assert.Equal(t, parser.AnalyzeStatementType, ddlStmt.Type)
	
	// Test ANALYZE table_name (column1, column2);
	ddlStmt, err = ddlParser.Parse("ANALYZE users (id, name);")
	assert.NoError(t, err)
	assert.Equal(t, parser.AnalyzeStatementType, ddlStmt.Type)
	
	// Test ANALYZE; (all tables)
	ddlStmt, err = ddlParser.Parse("ANALYZE;")
	assert.NoError(t, err)
	assert.Equal(t, parser.AnalyzeStatementType, ddlStmt.Type)
}
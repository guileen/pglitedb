package pgserver

import (
	"testing"

	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
	"github.com/stretchr/testify/assert"
)

func TestPreparedStatement_Structure(t *testing.T) {
	stmt := &components.PreparedStatement{
		Name:            "test_stmt",
		Query:           "SELECT * FROM test",
		PreprocessedSQL: "SELECT * FROM test",
		ParameterOIDs:   []uint32{23, 701},
		ReturningColumns: []string{"id", "name"},
	}

	assert.Equal(t, "test_stmt", stmt.Name)
	assert.Equal(t, "SELECT * FROM test", stmt.Query)
	assert.Equal(t, "SELECT * FROM test", stmt.PreprocessedSQL)
	assert.Equal(t, []uint32{23, 701}, stmt.ParameterOIDs)
	assert.Equal(t, []string{"id", "name"}, stmt.ReturningColumns)
}

func TestPortal_Structure(t *testing.T) {
	stmt := &components.PreparedStatement{
		Name:            "test_stmt",
		Query:           "SELECT * FROM test WHERE id = $1",
		PreprocessedSQL: "SELECT * FROM test WHERE id = $1",
		ParameterOIDs:   []uint32{23},
		ReturningColumns: []string{"id", "name"},
	}

	portal := &components.Portal{
		Name:         "test_portal",
		Statement:    stmt,
		Params:       []interface{}{42},
		ParamFormats: []int16{0},
	}

	assert.Equal(t, "test_portal", portal.Name)
	assert.Equal(t, stmt, portal.Statement)
	assert.Equal(t, []interface{}{42}, portal.Params)
	assert.Equal(t, []int16{0}, portal.ParamFormats)
}
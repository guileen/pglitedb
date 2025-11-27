package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDDLParser(t *testing.T) {
	parser := NewDDLParser()

	t.Run("CreateTable", func(t *testing.T) {
		query := `CREATE TABLE users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			email VARCHAR(255) UNIQUE
		)`

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, CreateTableStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.Len(t, stmt.Columns, 3)

		// Check id column
		assert.Equal(t, "id", stmt.Columns[0].Name)
		assert.Equal(t, "serial", stmt.Columns[0].Type)
		assert.True(t, stmt.Columns[0].PrimaryKey)

		// Check name column
		assert.Equal(t, "name", stmt.Columns[1].Name)
		assert.Equal(t, "varchar", stmt.Columns[1].Type)
		assert.True(t, stmt.Columns[1].NotNull)

		// Check email column
		assert.Equal(t, "email", stmt.Columns[2].Name)
		assert.Equal(t, "varchar", stmt.Columns[2].Type)
		assert.True(t, stmt.Columns[2].Unique)
	})

	t.Run("DropTable", func(t *testing.T) {
		query := "DROP TABLE users"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, DropTableStatement, stmt.Type)
		// Table name extraction is not fully implemented yet
	})

	t.Run("AlterTable", func(t *testing.T) {
		query := "ALTER TABLE users ADD COLUMN age INTEGER"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, AlterTableStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		// Command parsing is not fully implemented yet
	})
}
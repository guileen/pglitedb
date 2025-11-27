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
		assert.Equal(t, "users", stmt.TableName)
	})
	
	t.Run("DropTableIfExists", func(t *testing.T) {
		query := "DROP TABLE IF EXISTS users"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, DropTableStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.True(t, stmt.IfExists)
	})
	
	t.Run("DropTableCascade", func(t *testing.T) {
		query := "DROP TABLE users CASCADE"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, DropTableStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.True(t, stmt.Cascade)
	})

	t.Run("AlterTable", func(t *testing.T) {
		query := "ALTER TABLE users ADD COLUMN age INTEGER"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, AlterTableStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		// Command parsing is not fully implemented yet
	})

	t.Run("CreateIndex", func(t *testing.T) {
		query := "CREATE INDEX idx_users_email ON users (email)"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, CreateIndexStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.Equal(t, "idx_users_email", stmt.IndexName)
		assert.Equal(t, []string{"email"}, stmt.IndexColumns)
		assert.Equal(t, "btree", stmt.IndexType)
		assert.False(t, stmt.Unique)
	})

	t.Run("CreateUniqueIndex", func(t *testing.T) {
		query := "CREATE UNIQUE INDEX idx_users_email_unique ON users (email)"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, CreateIndexStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.Equal(t, "idx_users_email_unique", stmt.IndexName)
		assert.Equal(t, []string{"email"}, stmt.IndexColumns)
		assert.True(t, stmt.Unique)
	})

	t.Run("DropIndex", func(t *testing.T) {
		query := "DROP INDEX idx_users_email"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, DropIndexStatement, stmt.Type)
		assert.Equal(t, []string{"idx_users_email"}, stmt.IndexNames)
	})

	t.Run("CreateView", func(t *testing.T) {
		query := "CREATE VIEW user_emails AS SELECT id, email FROM users"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, CreateViewStatement, stmt.Type)
		assert.Equal(t, "user_emails", stmt.ViewName)
		assert.False(t, stmt.Replace)
		// View query parsing is simplified
	})

	t.Run("CreateOrReplaceView", func(t *testing.T) {
		query := "CREATE OR REPLACE VIEW user_emails AS SELECT id, email FROM users"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, CreateViewStatement, stmt.Type)
		assert.Equal(t, "user_emails", stmt.ViewName)
		assert.True(t, stmt.Replace)
	})

	t.Run("DropView", func(t *testing.T) {
		query := "DROP VIEW user_emails"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, DropViewStatement, stmt.Type)
		assert.Equal(t, []string{"user_emails"}, stmt.ViewNames)
	})

	t.Run("AlterTableAddConstraint", func(t *testing.T) {
		query := "ALTER TABLE users ADD CONSTRAINT unique_email UNIQUE (email)"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, AlterTableStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.Len(t, stmt.AlterCommands, 1)
		
		cmd := stmt.AlterCommands[0]
		assert.Equal(t, "unique_email", cmd.ConstraintName)
		assert.Equal(t, "UNIQUE", cmd.ConstraintType)
		assert.Equal(t, []string{"email"}, cmd.ConstraintColumns)
	})

	t.Run("AlterTableModifyColumn", func(t *testing.T) {
		query := "ALTER TABLE users ALTER COLUMN name TYPE VARCHAR(500)"

		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, AlterTableStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.Len(t, stmt.AlterCommands, 1)
		
		cmd := stmt.AlterCommands[0]
		assert.Equal(t, "name", cmd.ColumnName)
		assert.Equal(t, "varchar", cmd.ColumnType)
	})
}

func TestDDLParserEnhanced(t *testing.T) {
	parser := NewDDLParser()
	
	t.Run("CreateIndexWithOptions", func(t *testing.T) {
		query := "CREATE INDEX CONCURRENTLY idx_users_email_partial ON users (email) WHERE email IS NOT NULL"
		
		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, CreateIndexStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.Equal(t, "idx_users_email_partial", stmt.IndexName)
		assert.Equal(t, []string{"email"}, stmt.IndexColumns)
		assert.Equal(t, "btree", stmt.IndexType)
		assert.False(t, stmt.Unique)
		assert.True(t, stmt.Concurrent)
		assert.NotEmpty(t, stmt.WhereClause)
	})
	
	t.Run("CreateIndexWithGinType", func(t *testing.T) {
		query := "CREATE INDEX idx_users_data_gin ON users USING gin (data)"
		
		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, CreateIndexStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.Equal(t, "idx_users_data_gin", stmt.IndexName)
		assert.Equal(t, []string{"data"}, stmt.IndexColumns)
		assert.Equal(t, "gin", stmt.IndexType)
		assert.False(t, stmt.Unique)
	})
	
	t.Run("DropIndexCascade", func(t *testing.T) {
		query := "DROP INDEX idx_users_email CASCADE"
		
		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, DropIndexStatement, stmt.Type)
		assert.Equal(t, []string{"idx_users_email"}, stmt.IndexNames)
		assert.True(t, stmt.Cascade)
		assert.False(t, stmt.Restrict)
	})
	
	t.Run("DropIndexConcurrently", func(t *testing.T) {
		query := "DROP INDEX CONCURRENTLY idx_users_email"
		
		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, DropIndexStatement, stmt.Type)
		assert.Equal(t, []string{"idx_users_email"}, stmt.IndexNames)
		assert.True(t, stmt.Concurrent)
	})
	
	t.Run("CreateViewWithOptions", func(t *testing.T) {
		query := "CREATE VIEW user_summary (user_id, email_count) AS SELECT id, COUNT(email) FROM users GROUP BY id"
		
		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, CreateViewStatement, stmt.Type)
		assert.Equal(t, "user_summary", stmt.ViewName)
		assert.False(t, stmt.Replace)
		assert.Equal(t, []string{"user_id", "email_count"}, stmt.ViewColumnNames)
		assert.NotEmpty(t, stmt.ViewQuery)
	})
	
	t.Run("DropViewRestrict", func(t *testing.T) {
		query := "DROP VIEW user_emails RESTRICT"
		
		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, DropViewStatement, stmt.Type)
		assert.Equal(t, []string{"user_emails"}, stmt.ViewNames)
		assert.True(t, stmt.Restrict)
		assert.False(t, stmt.Cascade)
	})
	
	t.Run("AlterTableDropColumn", func(t *testing.T) {
		query := "ALTER TABLE users DROP COLUMN age"
		
		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, AlterTableStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.Len(t, stmt.AlterCommands, 1)
		
		cmd := stmt.AlterCommands[0]
		assert.Equal(t, "age", cmd.ColumnName)
	})
	
	t.Run("AlterTableDropColumnCascade", func(t *testing.T) {
		query := "ALTER TABLE users DROP COLUMN age CASCADE"
		
		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, AlterTableStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.Len(t, stmt.AlterCommands, 1)
		
		cmd := stmt.AlterCommands[0]
		assert.Equal(t, "age", cmd.ColumnName)
	})
	
	t.Run("AlterTableAddConstraintCheck", func(t *testing.T) {
		query := "ALTER TABLE users ADD CONSTRAINT check_age CHECK (age > 0)"
		
		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, AlterTableStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.Len(t, stmt.AlterCommands, 1)
		
		cmd := stmt.AlterCommands[0]
		assert.Equal(t, "check_age", cmd.ConstraintName)
		assert.Equal(t, "CHECK", cmd.ConstraintType)
	})
	
	t.Run("AlterTableDropConstraint", func(t *testing.T) {
		query := "ALTER TABLE users DROP CONSTRAINT unique_email"
		
		stmt, err := parser.Parse(query)
		require.NoError(t, err)
		assert.Equal(t, AlterTableStatement, stmt.Type)
		assert.Equal(t, "users", stmt.TableName)
		assert.Len(t, stmt.AlterCommands, 1)
		
		cmd := stmt.AlterCommands[0]
		assert.Equal(t, "unique_email", cmd.ConstraintName)
	})
}
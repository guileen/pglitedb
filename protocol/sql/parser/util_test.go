package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetStatementType(t *testing.T) {
	tests := []struct {
		query    string
		expected StatementType
	}{
		{"SELECT * FROM users", SelectStatement},
		{"INSERT INTO users (name) VALUES ('John')", InsertStatement},
		{"UPDATE users SET name = 'Jane' WHERE id = 1", UpdateStatement},
		{"DELETE FROM users WHERE id = 1", DeleteStatement},
		{"BEGIN", BeginStatement},
		{"START TRANSACTION", BeginStatement},
		{"COMMIT", CommitStatement},
		{"ROLLBACK", RollbackStatement},
		{"CREATE TABLE users (id INT)", CreateTableStatement},
		{"DROP TABLE users", DropTableStatement},
		{"ALTER TABLE users ADD COLUMN email VARCHAR(255)", AlterTableStatement},
		{"CREATE INDEX idx_users_name ON users (name)", CreateIndexStatement},
		{"DROP INDEX idx_users_name", DropIndexStatement},
		{"CREATE VIEW user_view AS SELECT * FROM users", CreateViewStatement},
		{"CREATE OR REPLACE VIEW user_view AS SELECT * FROM users", CreateViewStatement},
		{"DROP VIEW user_view", DropViewStatement},
		{"ANALYZE users", AnalyzeStatementType},
		{"CREATE DATABASE testdb", CreateDatabaseStatement},
		{"DROP DATABASE testdb", DropDatabaseStatement},
		{"DROP DATABASE IF EXISTS testdb", DropDatabaseStatement},
		{"INVALID SQL STATEMENT", UnknownStatement},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			result := GetStatementType(tt.query)
			assert.Equal(t, tt.expected, result, "Query: %s", tt.query)
		})
	}
}
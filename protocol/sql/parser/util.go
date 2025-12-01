package parser

import (
	"strings"
)

// ParseReturningColumns extracts RETURNING columns from a statement
func ParseReturningColumns(query string) []string {
	lowerQuery := strings.ToLower(query)
	returningIndex := strings.Index(lowerQuery, " returning ")
	if returningIndex == -1 {
		return []string{}
	}
	
	returningPart := query[returningIndex+11:] // Skip " returning "
	
	// Split by comma, but respect function calls and nested expressions
	var columns []string
	current := ""
	parenLevel := 0
	
	for _, char := range returningPart {
		switch char {
		case '(':
			parenLevel++
			current += string(char)
		case ')':
			parenLevel--
			current += string(char)
		case ',':
			if parenLevel == 0 {
				columns = append(columns, strings.TrimSpace(current))
				current = ""
			} else {
				current += string(char)
			}
		default:
			current += string(char)
		}
	}
	
	// Add the last column
	if current != "" {
		columns = append(columns, strings.TrimSpace(current))
	}
	
	return columns
}

// GetStatementType determines the type of SQL statement
func GetStatementType(query string) StatementType {
	trimmedQuery := strings.TrimSpace(query)
	lowerQuery := strings.ToLower(trimmedQuery)
	
	// Handle multi-line queries by taking only the first line for statement type detection
	if newlineIndex := strings.Index(trimmedQuery, "\n"); newlineIndex != -1 {
		firstLine := strings.TrimSpace(trimmedQuery[:newlineIndex])
		lowerQuery = strings.ToLower(firstLine)
	}
	
	switch {
	case strings.HasPrefix(lowerQuery, "select"):
		return SelectStatement
	case strings.HasPrefix(lowerQuery, "insert"):
		return InsertStatement
	case strings.HasPrefix(lowerQuery, "update"):
		return UpdateStatement
	case strings.HasPrefix(lowerQuery, "delete"):
		return DeleteStatement
	case strings.HasPrefix(lowerQuery, "begin") || strings.HasPrefix(lowerQuery, "start transaction"):
		return BeginStatement
	case strings.HasPrefix(lowerQuery, "commit"):
		return CommitStatement
	case strings.HasPrefix(lowerQuery, "rollback"):
		return RollbackStatement
	case strings.HasPrefix(lowerQuery, "create table"):
		return CreateTableStatement
	case strings.HasPrefix(lowerQuery, "drop table"):
		return DropTableStatement
	case strings.HasPrefix(lowerQuery, "alter table"):
		return AlterTableStatement
	case strings.HasPrefix(lowerQuery, "create index"):
		return CreateIndexStatement
	case strings.HasPrefix(lowerQuery, "drop index"):
		return DropIndexStatement
	case strings.HasPrefix(lowerQuery, "create view"):
		return CreateViewStatement
	case strings.HasPrefix(lowerQuery, "drop view"):
		return DropViewStatement
	case strings.HasPrefix(lowerQuery, "create database"):
		return CreateDatabaseStatement
	case strings.HasPrefix(lowerQuery, "drop database"):
		return DropDatabaseStatement
	case strings.HasPrefix(lowerQuery, "alter database"):
		return AlterDatabaseStatement
	case strings.HasPrefix(lowerQuery, "analyze"):
		return AnalyzeStatementType
	case strings.HasPrefix(lowerQuery, "truncate"):
		return TruncateTableStatement
	default:
		return UnknownStatement
	}
}
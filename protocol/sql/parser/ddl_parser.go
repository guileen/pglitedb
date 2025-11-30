package parser

import (
	"strings"
)

// DDLParser handles DDL (CREATE, ALTER, DROP) statement parsing
type DDLParser struct{}

// NewDDLParser creates a new DDLParser
func NewDDLParser() *DDLParser {
	return &DDLParser{}
}

// ExtractCreateTableInfo extracts information from a CREATE TABLE statement
func (dp *DDLParser) ExtractCreateTableInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract table name
	tableIndex := strings.Index(lowerQuery, " table ")
	if tableIndex != -1 {
		// Skip "table" keyword and find the table name
		afterTable := strings.TrimSpace(query[tableIndex+6:])
		
		// Check for IF NOT EXISTS
		if strings.HasPrefix(strings.ToLower(afterTable), "if not exists ") {
			parsed.IfNotExists = true
			afterTable = strings.TrimSpace(afterTable[14:]) // Skip "if not exists "
		}
		
		// Find end of table name (before parentheses or other keywords)
		tableNameEnd := len(afterTable)
		delimiters := []string{"(", "as", "with"}
		for _, delimiter := range delimiters {
			if idx := strings.Index(strings.ToLower(afterTable), delimiter); idx != -1 && idx < tableNameEnd {
				tableNameEnd = idx
			}
		}
		
		parsed.TableName = strings.TrimSpace(afterTable[:tableNameEnd])
		
		// Extract column definitions if present
		if strings.Contains(lowerQuery, "(") {
			columnsStart := strings.Index(query, "(")
			if columnsStart != -1 {
				columnsEnd := dp.findMatchingParen(query, columnsStart)
				if columnsEnd != -1 {
					columnsPart := query[columnsStart+1 : columnsEnd]
					parsed.Columns = dp.parseColumnDefinitions(columnsPart)
				}
			}
		}
	}
}

// ExtractAlterTableInfo extracts information from an ALTER TABLE statement
func (dp *DDLParser) ExtractAlterTableInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract table name
	tableIndex := strings.Index(lowerQuery, " table ")
	if tableIndex != -1 {
		afterTable := strings.TrimSpace(query[tableIndex+6:])
		
		// Find end of table name (before action keywords)
		tableNameEnd := len(afterTable)
		actionKeywords := []string{"add", "drop", "alter", "rename"}
		for _, keyword := range actionKeywords {
			if idx := strings.Index(strings.ToLower(afterTable), " "+keyword+" "); idx != -1 && idx < tableNameEnd {
				tableNameEnd = idx
			}
		}
		
		parsed.TableName = strings.TrimSpace(afterTable[:tableNameEnd])
		
		// Extract alter actions
		parsed.AlterActions = dp.parseAlterActions(afterTable[tableNameEnd:])
	}
}

// ExtractDropTableInfo extracts information from a DROP TABLE statement
func (dp *DDLParser) ExtractDropTableInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract table name
	tableIndex := strings.Index(lowerQuery, " table ")
	if tableIndex != -1 {
		afterTable := strings.TrimSpace(query[tableIndex+6:])
		
		// Check for IF EXISTS
		if strings.HasPrefix(strings.ToLower(afterTable), "if exists ") {
			parsed.IfExists = true
			afterTable = strings.TrimSpace(afterTable[10:]) // Skip "if exists "
		}
		
		// Table name ends at first space or end of string
		tableNameEnd := strings.Index(afterTable, " ")
		if tableNameEnd == -1 {
			parsed.TableName = afterTable
		} else {
			parsed.TableName = afterTable[:tableNameEnd]
		}
	}
}

// ExtractCreateIndexInfo extracts information from a CREATE INDEX statement
func (dp *DDLParser) ExtractCreateIndexInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract index name
	indexIndex := strings.Index(lowerQuery, " index ")
	if indexIndex != -1 {
		afterIndex := strings.TrimSpace(query[indexIndex+6:])
		
		// Check for IF NOT EXISTS
		if strings.HasPrefix(strings.ToLower(afterIndex), "if not exists ") {
			parsed.IfNotExists = true
			afterIndex = strings.TrimSpace(afterIndex[14:]) // Skip "if not exists "
		}
		
		// Find end of index name (before "on")
		onIndex := strings.Index(strings.ToLower(afterIndex), " on ")
		if onIndex != -1 {
			parsed.IndexName = strings.TrimSpace(afterIndex[:onIndex])
			
			// Extract table name and columns
			afterOn := strings.TrimSpace(afterIndex[onIndex+4:])
			tableEnd := strings.Index(afterOn, " ")
			if tableEnd == -1 {
				parsed.Table = afterOn
			} else {
				parsed.Table = strings.TrimSpace(afterOn[:tableEnd])
				
				// Extract columns
				columnsStart := strings.Index(afterOn, "(")
				if columnsStart != -1 {
					columnsEnd := dp.findMatchingParen(afterOn, columnsStart)
					if columnsEnd != -1 {
						columnsPart := afterOn[columnsStart+1 : columnsEnd]
						columns := strings.Split(columnsPart, ",")
						for i, col := range columns {
							columns[i] = strings.TrimSpace(col)
						}
						parsed.IndexColumns = columns
					}
				}
			}
		}
	}
}

// ExtractDropIndexInfo extracts information from a DROP INDEX statement
func (dp *DDLParser) ExtractDropIndexInfo(parsed *ParsedQuery, query, lowerQuery string) {
	// Extract index name
	indexIndex := strings.Index(lowerQuery, " index ")
	if indexIndex != -1 {
		afterIndex := strings.TrimSpace(query[indexIndex+6:])
		
		// Check for IF EXISTS
		if strings.HasPrefix(strings.ToLower(afterIndex), "if exists ") {
			parsed.IfExists = true
			afterIndex = strings.TrimSpace(afterIndex[10:]) // Skip "if exists "
		}
		
		// Index name ends at first space or end of string
		indexNameEnd := strings.Index(afterIndex, " ")
		if indexNameEnd == -1 {
			parsed.IndexName = afterIndex
		} else {
			parsed.IndexName = afterIndex[:indexNameEnd]
		}
	}
}

// parseColumnDefinitions parses column definitions from CREATE TABLE statement
func (dp *DDLParser) parseColumnDefinitions(columnsPart string) []*ColumnDefinition {
	var columns []*ColumnDefinition
	
	// Split by comma, but respect parentheses in constraints
	parts := dp.splitColumnDefinitions(columnsPart)
	
	for _, part := range parts {
		trimmedPart := strings.TrimSpace(part)
		if trimmedPart == "" {
			continue
		}
		
		// Skip constraint definitions for now
		if strings.HasPrefix(strings.ToUpper(trimmedPart), "CONSTRAINT") ||
			strings.HasPrefix(strings.ToUpper(trimmedPart), "PRIMARY KEY") ||
			strings.HasPrefix(strings.ToUpper(trimmedPart), "FOREIGN KEY") ||
			strings.HasPrefix(strings.ToUpper(trimmedPart), "UNIQUE") ||
			strings.HasPrefix(strings.ToUpper(trimmedPart), "CHECK") {
			continue
		}
		
		column := dp.parseColumnDefinition(trimmedPart)
		if column != nil {
			columns = append(columns, column)
		}
	}
	
	return columns
}

// splitColumnDefinitions splits column definitions by comma while respecting parentheses
func (dp *DDLParser) splitColumnDefinitions(s string) []string {
	var result []string
	current := ""
	parenLevel := 0
	
	for _, char := range s {
		switch char {
		case '(':
			parenLevel++
			current += string(char)
		case ')':
			parenLevel--
			current += string(char)
		case ',':
			if parenLevel == 0 {
				result = append(result, current)
				current = ""
			} else {
				current += string(char)
			}
		default:
			current += string(char)
		}
	}
	
	// Add the last part
	if current != "" {
		result = append(result, current)
	}
	
	return result
}

// parseColumnDefinition parses a single column definition
func (dp *DDLParser) parseColumnDefinition(columnPart string) *ColumnDefinition {
	parts := strings.Fields(columnPart)
	if len(parts) < 2 {
		return nil
	}
	
	column := &ColumnDefinition{
		Name: parts[0],
		Type: strings.ToUpper(parts[1]),
	}
	
	// Parse additional properties
	for i := 2; i < len(parts); i++ {
		part := strings.ToUpper(parts[i])
		switch part {
		case "NOT":
			if i+1 < len(parts) && strings.ToUpper(parts[i+1]) == "NULL" {
				column.NotNull = true
				i++ // Skip "NULL"
			}
		case "PRIMARY":
			if i+1 < len(parts) && strings.ToUpper(parts[i+1]) == "KEY" {
				column.PrimaryKey = true
				i++ // Skip "KEY"
			}
		case "UNIQUE":
			column.Unique = true
		case "DEFAULT":
			if i+1 < len(parts) {
				column.Default = parts[i+1]
				i++ // Skip default value
			}
		}
	}
	
	return column
}

// parseAlterActions parses ALTER TABLE actions
func (dp *DDLParser) parseAlterActions(actionsPart string) []AlterAction {
	var actions []AlterAction
	
	// This is a simplified implementation
	// A full implementation would parse various ALTER actions
	
	actionsPart = strings.TrimSpace(actionsPart)
	if strings.HasPrefix(strings.ToUpper(actionsPart), "ADD COLUMN ") {
		columnPart := strings.TrimSpace(actionsPart[11:]) // Skip "ADD COLUMN "
		column := dp.parseColumnDefinition(columnPart)
		if column != nil {
			action := &AddColumnAction{ColumnDef: column}
			actions = append(actions, action)
		}
	} else if strings.HasPrefix(strings.ToUpper(actionsPart), "DROP COLUMN ") {
		columnName := strings.TrimSpace(actionsPart[12:]) // Skip "DROP COLUMN "
		action := &DropColumnAction{ColumnName: columnName}
		actions = append(actions, action)
	}
	
	return actions
}

// findMatchingParen finds the matching closing parenthesis
func (dp *DDLParser) findMatchingParen(s string, openPos int) int {
	if openPos >= len(s) || s[openPos] != '(' {
		return -1
	}
	
	level := 1
	for i := openPos + 1; i < len(s); i++ {
		switch s[i] {
		case '(':
			level++
		case ')':
			level--
			if level == 0 {
				return i
			}
		}
	}
	
	return -1
}
package types

// ColumnType represents the data type of a table column
type ColumnType string

const (
	ColumnTypeString    ColumnType = "string"
	ColumnTypeNumber    ColumnType = "number"
	ColumnTypeBoolean   ColumnType = "boolean"
	ColumnTypeDate      ColumnType = "date"
	ColumnTypeTimestamp ColumnType = "timestamp"
	ColumnTypeJSON      ColumnType = "json"
	ColumnTypeUUID      ColumnType = "uuid"
	ColumnTypeText      ColumnType = "text"
	ColumnTypeBinary    ColumnType = "binary"
)

// IsValidColumnType checks if a column type is valid
func IsValidColumnType(typ ColumnType) bool {
	switch typ {
	case ColumnTypeString, ColumnTypeNumber, ColumnTypeBoolean,
		ColumnTypeDate, ColumnTypeTimestamp, ColumnTypeJSON,
		ColumnTypeUUID, ColumnTypeText, ColumnTypeBinary:
		return true
	default:
		return false
	}
}

// GetColumnTypeFromGoType maps Go types to column types
func GetColumnTypeFromGoType(typ string) ColumnType {
	switch typ {
	case "string":
		return ColumnTypeString
	case "int", "int32", "int64", "float32", "float64":
		return ColumnTypeNumber
	case "bool":
		return ColumnTypeBoolean
	case "time.Time":
		return ColumnTypeTimestamp
	default:
		return ColumnTypeJSON
	}
}
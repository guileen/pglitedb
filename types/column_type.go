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
	
	ColumnTypeSmallInt  ColumnType = "smallint"
	ColumnTypeInteger   ColumnType = "integer"
	ColumnTypeBigInt    ColumnType = "bigint"
	ColumnTypeReal      ColumnType = "real"
	ColumnTypeDouble    ColumnType = "double"
	ColumnTypeNumeric   ColumnType = "numeric"
	ColumnTypeVarchar   ColumnType = "varchar"
	ColumnTypeChar      ColumnType = "char"
	ColumnTypeJSONB     ColumnType = "jsonb"
	
	// PostgreSQL SERIAL types
	ColumnTypeSerial    ColumnType = "serial"
	ColumnTypeBigSerial ColumnType = "bigserial"
	ColumnTypeSmallSerial ColumnType = "smallserial"
)

// IsValidColumnType checks if a column type is valid
func IsValidColumnType(typ ColumnType) bool {
	switch typ {
	case ColumnTypeString, ColumnTypeNumber, ColumnTypeBoolean,
		ColumnTypeDate, ColumnTypeTimestamp, ColumnTypeJSON,
		ColumnTypeUUID, ColumnTypeText, ColumnTypeBinary,
		ColumnTypeSmallInt, ColumnTypeInteger, ColumnTypeBigInt,
		ColumnTypeReal, ColumnTypeDouble, ColumnTypeNumeric,
		ColumnTypeVarchar, ColumnTypeChar, ColumnTypeJSONB,
		ColumnTypeSerial, ColumnTypeBigSerial, ColumnTypeSmallSerial:
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
	case "int16":
		return ColumnTypeSmallInt
	case "int32":
		return ColumnTypeInteger
	case "int", "int64":
		return ColumnTypeBigInt
	case "float32":
		return ColumnTypeReal
	case "float64":
		return ColumnTypeDouble
	case "bool":
		return ColumnTypeBoolean
	case "time.Time":
		return ColumnTypeTimestamp
	default:
		return ColumnTypeJSON
	}
}

// MapSerialType converts SERIAL types to their underlying integer types
func MapSerialType(typ ColumnType) ColumnType {
	switch typ {
	case ColumnTypeSerial:
		return ColumnTypeInteger
	case ColumnTypeBigSerial:
		return ColumnTypeBigInt
	case ColumnTypeSmallSerial:
		return ColumnTypeSmallInt
	default:
		return typ
	}
}

// IsSerialType checks if a column type is a SERIAL type
func IsSerialType(typ ColumnType) bool {
	return typ == ColumnTypeSerial || typ == ColumnTypeBigSerial || typ == ColumnTypeSmallSerial
}
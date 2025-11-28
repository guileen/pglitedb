package oid

import (
	"hash/fnv"
)

// GenerateDeterministicOID generates a deterministic OID based on input string
func GenerateDeterministicOID(input string) int64 {
	h := fnv.New64a()
	h.Write([]byte(input))
	return int64(h.Sum64()%1000000000) + 10000 // Ensure positive OID above 10000
}

// GenerateTableOID generates a deterministic OID for a table
func GenerateTableOID(tableName string) int64 {
	return GenerateDeterministicOID("table:" + tableName)
}

// GenerateTypeOID generates a deterministic OID for a type
func GenerateTypeOID(typeName string) int64 {
	return GenerateDeterministicOID("type:" + typeName)
}

// GenerateNamespaceOID generates a deterministic OID for a namespace
func GenerateNamespaceOID(namespaceName string) int64 {
	return GenerateDeterministicOID("namespace:" + namespaceName)
}
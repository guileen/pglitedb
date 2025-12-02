package oid

import (
	"testing"
)

func TestGenerateDeterministicOID(t *testing.T) {
	// Test that the same input produces the same output
	input := "test_input"
	oid1 := GenerateDeterministicOID(input)
	oid2 := GenerateDeterministicOID(input)
	
	if oid1 != oid2 {
		t.Errorf("Expected same OID for same input, got %d and %d", oid1, oid2)
	}
	
	// Test that different inputs produce different outputs
	input2 := "test_input_2"
	oid3 := GenerateDeterministicOID(input2)
	
	if oid1 == oid3 {
		t.Errorf("Expected different OIDs for different inputs, got %d for both", oid1)
	}
	
	// Test that OID is positive and above 10000
	if oid1 < 10000 {
		t.Errorf("Expected OID to be above 10000, got %d", oid1)
	}
}

func TestGenerateTableOID(t *testing.T) {
	tableName := "users"
	oid := GenerateTableOID(tableName)
	
	// Test that it produces a valid OID
	if oid < 10000 {
		t.Errorf("Expected table OID to be above 10000, got %d", oid)
	}
	
	// Test determinism
	oid1 := GenerateTableOID(tableName)
	oid2 := GenerateTableOID(tableName)
	
	if oid1 != oid2 {
		t.Errorf("Expected same table OID for same table name, got %d and %d", oid1, oid2)
	}
	
	// Test different tables produce different OIDs
	oid3 := GenerateTableOID("orders")
	if oid1 == oid3 {
		t.Errorf("Expected different OIDs for different table names, got %d for both", oid1)
	}
}

func TestGenerateTypeOID(t *testing.T) {
	typeName := "integer"
	oid := GenerateTypeOID(typeName)
	
	// Test that it produces a valid OID
	if oid < 10000 {
		t.Errorf("Expected type OID to be above 10000, got %d", oid)
	}
	
	// Test determinism
	oid1 := GenerateTypeOID(typeName)
	oid2 := GenerateTypeOID(typeName)
	
	if oid1 != oid2 {
		t.Errorf("Expected same type OID for same type name, got %d and %d", oid1, oid2)
	}
	
	// Test different types produce different OIDs
	oid3 := GenerateTypeOID("varchar")
	if oid1 == oid3 {
		t.Errorf("Expected different OIDs for different type names, got %d for both", oid1)
	}
}

func TestGenerateNamespaceOID(t *testing.T) {
	namespaceName := "public"
	oid := GenerateNamespaceOID(namespaceName)
	
	// Test that it produces a valid OID
	if oid < 10000 {
		t.Errorf("Expected namespace OID to be above 10000, got %d", oid)
	}
	
	// Test determinism
	oid1 := GenerateNamespaceOID(namespaceName)
	oid2 := GenerateNamespaceOID(namespaceName)
	
	if oid1 != oid2 {
		t.Errorf("Expected same namespace OID for same namespace name, got %d and %d", oid1, oid2)
	}
	
	// Test different namespaces produce different OIDs
	oid3 := GenerateNamespaceOID("private")
	if oid1 == oid3 {
		t.Errorf("Expected different OIDs for different namespace names, got %d for both", oid1)
	}
}

func TestDifferentOIDTypesProduceDifferentValues(t *testing.T) {
	name := "test"
	
	tableOID := GenerateTableOID(name)
	typeOID := GenerateTypeOID(name)
	namespaceOID := GenerateNamespaceOID(name)
	
	// All three should be different
	if tableOID == typeOID || tableOID == namespaceOID || typeOID == namespaceOID {
		t.Errorf("Expected different OIDs for different types: table=%d, type=%d, namespace=%d", tableOID, typeOID, namespaceOID)
	}
}
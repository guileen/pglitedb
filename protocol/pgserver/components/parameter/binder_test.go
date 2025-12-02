package parameter

import (
	"testing"

	pg_query "github.com/pganalyze/pg_query_go/v6"
	"github.com/stretchr/testify/assert"
)

func TestParameterBinder_NewParameterBinder(t *testing.T) {
	// Create a simple AST for testing
	ast := &pg_query.ParseResult{}
	params := []interface{}{"test", 123, true}

	binder := NewParameterBinder(ast, params)

	assert.NotNil(t, binder)
	assert.Equal(t, ast, binder.ast)
	assert.Equal(t, params, binder.params)
}

func TestParameterBinder_Init(t *testing.T) {
	// Create initial AST and params
	initialAST := &pg_query.ParseResult{}
	initialParams := []interface{}{"initial"}

	binder := NewParameterBinder(initialAST, initialParams)

	// Create new AST and params for initialization
	newAST := &pg_query.ParseResult{
		Stmts: []*pg_query.RawStmt{
			{
				Stmt: &pg_query.Node{
					Node: &pg_query.Node_SelectStmt{
						SelectStmt: &pg_query.SelectStmt{},
					},
				},
			},
		},
	}
	newParams := []interface{}{"new", 456, false}

	// Initialize with new values
	binder.Init(newAST, newParams)

	assert.Equal(t, newAST, binder.ast)
	assert.Equal(t, newParams, binder.params)
}

func TestParameterBinder_Reset(t *testing.T) {
	// Create a binder with initial values
	ast := &pg_query.ParseResult{}
	params := []interface{}{"test", 123}

	binder := NewParameterBinder(ast, params)

	// Reset the binder
	binder.Reset()

	assert.Nil(t, binder.ast)
	assert.Nil(t, binder.params)
}

func TestParameterBinder_BindParameters_EmptyAST(t *testing.T) {
	// Test with empty AST
	ast := &pg_query.ParseResult{}
	params := []interface{}{}

	binder := NewParameterBinder(ast, params)

	result, err := binder.BindParameters()

	assert.NoError(t, err)
	assert.Equal(t, ast, result)
}

func TestParameterBinder_CreateConstantNode_String(t *testing.T) {
	binder := &ParameterBinder{}

	// Test string value
	value := "test string"
	node := binder.createConstantNode(value)

	assert.NotNil(t, node)
	aConst := node.GetAConst()
	assert.NotNil(t, aConst)
	sval := aConst.GetSval()
	assert.NotNil(t, sval)
	assert.Equal(t, value, sval.Sval)
}

func TestParameterBinder_CreateConstantNode_Int(t *testing.T) {
	binder := &ParameterBinder{}

	// Test int value
	value := 42
	node := binder.createConstantNode(value)

	assert.NotNil(t, node)
	aConst := node.GetAConst()
	assert.NotNil(t, aConst)
	ival := aConst.GetIval()
	assert.NotNil(t, ival)
	assert.Equal(t, int32(value), ival.Ival)
}

func TestParameterBinder_CreateConstantNode_Int32(t *testing.T) {
	binder := &ParameterBinder{}

	// Test int32 value
	value := int32(42)
	node := binder.createConstantNode(value)

	assert.NotNil(t, node)
	aConst := node.GetAConst()
	assert.NotNil(t, aConst)
	ival := aConst.GetIval()
	assert.NotNil(t, ival)
	assert.Equal(t, value, ival.Ival)
}

func TestParameterBinder_CreateConstantNode_Int64(t *testing.T) {
	binder := &ParameterBinder{}

	// Test int64 value
	value := int64(42)
	node := binder.createConstantNode(value)

	assert.NotNil(t, node)
	aConst := node.GetAConst()
	assert.NotNil(t, aConst)
	ival := aConst.GetIval()
	assert.NotNil(t, ival)
	assert.Equal(t, int32(value), ival.Ival)
}

func TestParameterBinder_CreateConstantNode_Float32(t *testing.T) {
	binder := &ParameterBinder{}

	// Test float32 value
	value := float32(3.14)
	node := binder.createConstantNode(value)

	assert.NotNil(t, node)
	aConst := node.GetAConst()
	assert.NotNil(t, aConst)
	fval := aConst.GetFval()
	assert.NotNil(t, fval)
	// Float comparison as string since it might be formatted differently
	assert.Contains(t, fval.Fval, "3.14")
}

func TestParameterBinder_CreateConstantNode_Float64(t *testing.T) {
	binder := &ParameterBinder{}

	// Test float64 value
	value := 3.14159
	node := binder.createConstantNode(value)

	assert.NotNil(t, node)
	aConst := node.GetAConst()
	assert.NotNil(t, aConst)
	fval := aConst.GetFval()
	assert.NotNil(t, fval)
	// Float comparison as string since it might be formatted differently
	assert.Contains(t, fval.Fval, "3.14159")
}

func TestParameterBinder_CreateConstantNode_Bool(t *testing.T) {
	binder := &ParameterBinder{}

	// Test boolean true
	value := true
	node := binder.createConstantNode(value)

	assert.NotNil(t, node)
	aConst := node.GetAConst()
	assert.NotNil(t, aConst)
	boolval := aConst.GetBoolval()
	assert.NotNil(t, boolval)
	assert.True(t, boolval.Boolval)

	// Test boolean false
	value = false
	node = binder.createConstantNode(value)

	assert.NotNil(t, node)
	aConst = node.GetAConst()
	assert.NotNil(t, aConst)
	boolval = aConst.GetBoolval()
	assert.NotNil(t, boolval)
	assert.False(t, boolval.Boolval)
}

func TestParameterBinder_CreateConstantNode_Nil(t *testing.T) {
	binder := &ParameterBinder{}

	// Test nil value
	var value interface{} = nil
	node := binder.createConstantNode(value)

	assert.NotNil(t, node)
	aConst := node.GetAConst()
	assert.NotNil(t, aConst)
	assert.True(t, aConst.Isnull)
}

func TestParameterBinder_CreateConstantNode_OtherTypes(t *testing.T) {
	binder := &ParameterBinder{}

	// Test with a custom struct (should be converted to string)
	type CustomStruct struct {
		Field string
	}
	value := CustomStruct{Field: "test"}
	node := binder.createConstantNode(value)

	assert.NotNil(t, node)
	aConst := node.GetAConst()
	assert.NotNil(t, aConst)
	sval := aConst.GetSval()
	assert.NotNil(t, sval)
	assert.Contains(t, sval.Sval, "test")
}
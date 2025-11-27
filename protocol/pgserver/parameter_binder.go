package pgserver

import (
	"fmt"
	"strconv"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// ParameterBinder handles parameter binding for PostgreSQL queries
type ParameterBinder struct {
	ast    *pg_query.ParseResult
	params []interface{}
}

// NewParameterBinder creates a new parameter binder
func NewParameterBinder(ast *pg_query.ParseResult, params []interface{}) *ParameterBinder {
	return &ParameterBinder{
		ast:    ast,
		params: params,
	}
}

// BindParameters binds parameters in the AST and returns a new AST
func (pb *ParameterBinder) BindParameters() (*pg_query.ParseResult, error) {
	if len(pb.ast.Stmts) == 0 {
		return pb.ast, nil
	}

	// Deep copy AST to avoid modifying the original
	newAST := &pg_query.ParseResult{
		Version: pb.ast.Version,
		Stmts:   make([]*pg_query.RawStmt, len(pb.ast.Stmts)),
	}

	// Copy and bind each statement
	for i, stmt := range pb.ast.Stmts {
		newStmt := &pg_query.RawStmt{
			Stmt:         pb.bindNode(stmt.Stmt),
			StmtLocation: stmt.StmtLocation,
			StmtLen:      stmt.StmtLen,
		}
		newAST.Stmts[i] = newStmt
	}

	return newAST, nil
}

// bindNode recursively binds parameters in nodes
func (pb *ParameterBinder) bindNode(node *pg_query.Node) *pg_query.Node {
	if node == nil {
		return nil
	}

	// If this is a parameter reference node, replace with constant
	if paramRef := node.GetParamRef(); paramRef != nil {
		paramIndex := int(paramRef.Number) - 1
		if paramIndex >= 0 && paramIndex < len(pb.params) {
			return pb.createConstantNode(pb.params[paramIndex])
		}
		// If parameter index is out of range, keep original node
		return node
	}

	// Recursively handle node types that may contain parameters
	switch n := node.Node.(type) {
	case *pg_query.Node_SelectStmt:
		selectStmt := n.SelectStmt
		newSelectStmt := &pg_query.SelectStmt{
			DistinctClause: pb.bindNodeList(selectStmt.DistinctClause),
			IntoClause:     selectStmt.IntoClause,
			TargetList:     pb.bindNodeList(selectStmt.TargetList),
			FromClause:     pb.bindNodeList(selectStmt.FromClause),
			WhereClause:    pb.bindNode(selectStmt.WhereClause),
			GroupClause:    pb.bindNodeList(selectStmt.GroupClause),
			HavingClause:   pb.bindNode(selectStmt.HavingClause),
			WindowClause:   pb.bindNodeList(selectStmt.WindowClause),
			ValuesLists:    pb.bindNodeList(selectStmt.ValuesLists),
			SortClause:     pb.bindNodeList(selectStmt.SortClause),
			LimitOffset:    pb.bindNode(selectStmt.LimitOffset),
			LimitCount:     pb.bindNode(selectStmt.LimitCount),
			LimitOption:    selectStmt.LimitOption,
			LockingClause:  pb.bindNodeList(selectStmt.LockingClause),
			WithClause:     selectStmt.WithClause,
			Op:             selectStmt.Op,
			All:            selectStmt.All,
			Larg:           selectStmt.Larg, // These should be SelectStmt, not Node
			Rarg:           selectStmt.Rarg, // These should be SelectStmt, not Node
		}
		return &pg_query.Node{Node: &pg_query.Node_SelectStmt{SelectStmt: newSelectStmt}}

	case *pg_query.Node_InsertStmt:
		insertStmt := n.InsertStmt
		newInsertStmt := &pg_query.InsertStmt{
			Relation:         insertStmt.Relation,
			Cols:             pb.bindNodeList(insertStmt.Cols),
			SelectStmt:       pb.bindNode(insertStmt.SelectStmt),
			OnConflictClause: insertStmt.OnConflictClause,
			ReturningList:    pb.bindNodeList(insertStmt.ReturningList),
			WithClause:       insertStmt.WithClause,
			Override:         insertStmt.Override,
		}
		return &pg_query.Node{Node: &pg_query.Node_InsertStmt{InsertStmt: newInsertStmt}}

	case *pg_query.Node_UpdateStmt:
		updateStmt := n.UpdateStmt
		newUpdateStmt := &pg_query.UpdateStmt{
			Relation:       updateStmt.Relation,
			TargetList:     pb.bindNodeList(updateStmt.TargetList),
			WhereClause:    pb.bindNode(updateStmt.WhereClause),
			FromClause:     pb.bindNodeList(updateStmt.FromClause),
			ReturningList:  pb.bindNodeList(updateStmt.ReturningList),
			WithClause:     updateStmt.WithClause,
		}
		return &pg_query.Node{Node: &pg_query.Node_UpdateStmt{UpdateStmt: newUpdateStmt}}

	case *pg_query.Node_DeleteStmt:
		deleteStmt := n.DeleteStmt
		newDeleteStmt := &pg_query.DeleteStmt{
			Relation:       deleteStmt.Relation,
			UsingClause:    pb.bindNodeList(deleteStmt.UsingClause),
			WhereClause:    pb.bindNode(deleteStmt.WhereClause),
			ReturningList:  pb.bindNodeList(deleteStmt.ReturningList),
			WithClause:     deleteStmt.WithClause,
		}
		return &pg_query.Node{Node: &pg_query.Node_DeleteStmt{DeleteStmt: newDeleteStmt}}

	case *pg_query.Node_AExpr:
		aExpr := n.AExpr
		newAExpr := &pg_query.A_Expr{
			Kind:     aExpr.Kind,
			Name:     pb.bindNodeList(aExpr.Name),
			Lexpr:    pb.bindNode(aExpr.Lexpr),
			Rexpr:    pb.bindNode(aExpr.Rexpr),
			Location: aExpr.Location,
		}
		return &pg_query.Node{Node: &pg_query.Node_AExpr{AExpr: newAExpr}}

	case *pg_query.Node_BoolExpr:
		boolExpr := n.BoolExpr
		newBoolExpr := &pg_query.BoolExpr{
			Boolop:   boolExpr.Boolop,
			Args:     pb.bindNodeList(boolExpr.Args),
			Location: boolExpr.Location,
		}
		return &pg_query.Node{Node: &pg_query.Node_BoolExpr{BoolExpr: newBoolExpr}}

	case *pg_query.Node_ResTarget:
		resTarget := n.ResTarget
		newResTarget := &pg_query.ResTarget{
			Name:        resTarget.Name,
			Indirection: pb.bindNodeList(resTarget.Indirection),
			Val:         pb.bindNode(resTarget.Val),
			Location:    resTarget.Location,
		}
		return &pg_query.Node{Node: &pg_query.Node_ResTarget{ResTarget: newResTarget}}

	// For other node types, return as-is
	default:
		return node
	}
}

// bindNodeList binds parameters in a list of nodes
func (pb *ParameterBinder) bindNodeList(nodes []*pg_query.Node) []*pg_query.Node {
	if nodes == nil {
		return nil
	}

	newNodes := make([]*pg_query.Node, len(nodes))
	for i, node := range nodes {
		newNodes[i] = pb.bindNode(node)
	}
	return newNodes
}

// createConstantNode creates a constant node from a Go value
func (pb *ParameterBinder) createConstantNode(value interface{}) *pg_query.Node {
	switch v := value.(type) {
	case nil:
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Isnull: true,
				},
			},
		}
	case string:
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Sval{
						Sval: &pg_query.String{
							Sval: v,
						},
					},
				},
			},
		}
	case int:
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Ival{
						Ival: &pg_query.Integer{
							Ival: int32(v),
						},
					},
				},
			},
		}
	case int32:
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Ival{
						Ival: &pg_query.Integer{
							Ival: v,
						},
					},
				},
			},
		}
	case int64:
		// Note: PostgreSQL integer types are typically 32-bit
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Ival{
						Ival: &pg_query.Integer{
							Ival: int32(v),
						},
					},
				},
			},
		}
	case float32:
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Fval{
						Fval: &pg_query.Float{
							Fval: strconv.FormatFloat(float64(v), 'f', -1, 32),
						},
					},
				},
			},
		}
	case float64:
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Fval{
						Fval: &pg_query.Float{
							Fval: strconv.FormatFloat(v, 'f', -1, 64),
						},
					},
				},
			},
		}
	case bool:
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Boolval{
						Boolval: &pg_query.Boolean{
							Boolval: v,
						},
					},
				},
			},
		}
	default:
		// For other types, convert to string
		return &pg_query.Node{
			Node: &pg_query.Node_AConst{
				AConst: &pg_query.A_Const{
					Val: &pg_query.A_Const_Sval{
						Sval: &pg_query.String{
							Sval: fmt.Sprintf("%v", v),
						},
					},
				},
			},
		}
	}
}

// BindParametersInQuery is a convenience method to handle query strings directly
func BindParametersInQuery(query string, params []interface{}) (string, error) {
	// Parse query to AST
	parseResult, err := pg_query.Parse(query)
	if err != nil {
		return "", fmt.Errorf("failed to parse query: %w", err)
	}

	// Create binder and bind parameters
	binder := NewParameterBinder(parseResult, params)
	boundAST, err := binder.BindParameters()
	if err != nil {
		return "", fmt.Errorf("failed to bind parameters: %w", err)
	}

	// Convert bound AST back to SQL
	deparseResult, err := pg_query.Deparse(boundAST)
	if err != nil {
		return "", fmt.Errorf("failed to deparse AST: %w", err)
	}

	return deparseResult, nil
}
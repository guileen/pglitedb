package main

import (
	"fmt"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func main() {
	query := "SELECT version();"
	result, err := pg_query.Parse(query)
	if err != nil {
		fmt.Printf("Error parsing query: %v\n", err)
		return
	}
	
	if len(result.Stmts) == 0 {
		fmt.Println("No statements found")
		return
	}
	
	stmt := result.Stmts[0].Stmt
	selectStmt := stmt.GetSelectStmt()
	if selectStmt == nil {
		fmt.Println("Not a SELECT statement")
		return
	}
	
	targetList := selectStmt.GetTargetList()
	if len(targetList) == 0 {
		fmt.Println("No target list")
		return
	}
	
	// Check the first target
	target := targetList[0]
	resTarget := target.GetResTarget()
	if resTarget == nil {
		fmt.Println("Not a ResTarget")
		return
	}
	
	val := resTarget.GetVal()
	if val == nil {
		fmt.Println("No value in ResTarget")
		return
	}
	
	funcCall := val.GetFuncCall()
	if funcCall != nil {
		fmt.Println("Found function call:")
		fmt.Printf("  Function name: %+v\n", funcCall.GetFuncname())
		if len(funcCall.GetFuncname()) > 0 {
			name := funcCall.GetFuncname()[0].GetString_()
			if name != nil {
				fmt.Printf("  Function name string: %s\n", name.GetSval())
			}
		}
	} else {
		fmt.Println("Not a function call")
		fmt.Printf("Value type: %+v\n", val)
	}
}
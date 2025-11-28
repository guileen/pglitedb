package main

import (
	"fmt"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func main() {
	query := "SELECT current_user;"
	result, err := pg_query.Parse(query)
	if err != nil {
		fmt.Printf("Error parsing query: %v
", err)
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
	
	fmt.Printf("Value type: %+v
", val)
}

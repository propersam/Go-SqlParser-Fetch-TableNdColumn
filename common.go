package main

import (
	"fmt"

	"github.com/vitessio/vitess/go/vt/sqlparser"
	// "github.com/xwb1989/sqlparser"
)

func getColumnNames(s interface{}, columns []string, level int) []string {

	switch v := s.(type) {
	case *sqlparser.Select:
		// Checking for unique columns in Select expresion from statement
		selectExprs := v.SelectExprs
		if len(selectExprs) > 0 {
			for _, selectExpr := range selectExprs {
				columns = getColumnNames(selectExpr, columns, level+1)
			}
		}

		// checking for unique columns in the FROM section of statement
		from := v.From
		columns = getColumnNames(from, columns, level+1)

		// Checking for unique columns in OrderBy clause from statement
		orderBy := v.OrderBy
		if len(orderBy) > 0 {
			for _, order := range orderBy {
				columns = getColumnNames(order, columns, level+1)
			}
		}

		// Checking for unique columns in Where clause from statement
		whereClause := v.Where
		if whereClause != nil {
			columns = getColumnNames(whereClause, columns, level+1)
		}

		// Checking for unique columns in Having clause
		having := v.Having
		if having != nil {
			// for some reason, this is also of type *sqlparser.Where
			columns = getColumnNames(having, columns, level+1)
		}

		// checking for unique columns in GroupBy clause
		groupBy := v.GroupBy
		if len(groupBy) > 0 {
			for _, group := range groupBy {
				columns = getColumnNames(group, columns, level+1)
			}
		}

	case *sqlparser.AliasedExpr:
		aliasedExp := v
		colExpr := aliasedExp.Expr
		columns = getColumnNames(colExpr, columns, level+1)

	case *sqlparser.FuncExpr:
		// This is to handle function commands like:
		// SUM, COUNT, AVG, MIN, MAX etc.
		funcExpr := v
		for _, expr := range funcExpr.Exprs {
			columns = getColumnNames(expr, columns, level+1)
		}

	case *sqlparser.Order:
		order := v.Expr
		columns = getColumnNames(order, columns, level+1)

	case *sqlparser.Where:
		where := v.Expr
		columns = getColumnNames(where, columns, level+1)

	case *sqlparser.ColName:
		colIdent := v.Name
		colName := colIdent.String()

		if colName != "" {
			exists := false
			// making sure colNames are nt repeated
			for _, column := range columns {
				if colName == column {
					exists = true
					break
				}
			}
			if !exists {
				columns = append(columns, colName)
			}
		}

	case *sqlparser.StarExpr:
		columns = append(columns, "*")

	case *sqlparser.AndExpr:
		leftExpr := v.Left
		columns = getColumnNames(leftExpr, columns, level+1)

		right := v.Right
		columns = getColumnNames(right, columns, level+1)

	case *sqlparser.OrExpr:
		leftExpr := v.Left
		columns = getColumnNames(leftExpr, columns, level+1)

		right := v.Right
		columns = getColumnNames(right, columns, level+1)

	case *sqlparser.NotExpr:
		expr := v.Expr
		columns = getColumnNames(expr, columns, level+1)

	case *sqlparser.ComparisonExpr:
		leftSide := v.Left
		columns = getColumnNames(leftSide, columns, level+1)

	case sqlparser.TableExprs:
		for _, tableExpr := range v {
			columns = getColumnNames(tableExpr, columns, level+1)
		}

	case *sqlparser.AliasedTableExpr:
		// Do nothing

	case *sqlparser.JoinTableExpr:
		condition := v.Condition
		columns = getColumnNames(condition, columns, level+1)

	case sqlparser.JoinCondition:
		onCondition := v.On
		columns = getColumnNames(onCondition, columns, level+1)

	default:
		panic(fmt.Sprintf("Error in recursive level [%d].\nThis type '(%T)' is unaccounted for\n", level, v))

	}

	return columns
}

func getTableNames(s interface{}, tables []string, level int) []string {

	switch v := s.(type) {

	case *sqlparser.Select:
		tableExprs := v.From
		if len(tableExprs) > 0 {
			for _, tableExpr := range tableExprs {
				tables = getTableNames(tableExpr, tables, level+1)
			}
		}

	case *sqlparser.AliasedTableExpr:
		tableExpr := v.Expr
		tables = getTableNames(tableExpr, tables, level+1)

	case *sqlparser.JoinTableExpr:
		leftExpr := v.LeftExpr
		tables = getTableNames(leftExpr, tables, level+1)

		rightExpr := v.RightExpr
		tables = getTableNames(rightExpr, tables, level+1)

	case sqlparser.TableName:
		tableIdent := v.Name
		tableName := tableIdent.String()

		if tableName != "" {
			exists := false
			// making sure tableNames are nt repeated
			for _, table := range tables {
				if tableName == table {
					exists = true
					break
				}
			}
			if !exists {
				tables = append(tables, tableName)
			}
		}

	default:
		panic(fmt.Sprintf("Error in recursive level [%d].\nThis type '(%T)' is unaccounted for\n", level, v))

	}

	return tables
}

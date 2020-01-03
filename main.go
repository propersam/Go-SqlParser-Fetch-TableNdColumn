package main

import (
	"fmt"

	// "github.com/davecgh/go-spew/spew"
	"github.com/vitessio/vitess/go/vt/sqlparser"
	// "github.com/xwb1989/sqlparser"
)

func main() {
	var (
		columns []string
		tables  []string
		level   int
	)

	// query := "SELECT COUNT(deji, users, stadium) FROM users"
	// query := "SELECT MIN(user_name) FROM users WHERE id = 5 AND last_name = unknown OR ekwetu = tamuno AND virus = youHave"
	// query := "SELECT SUM(number) FROM users WHERE NOT present = false"
	// query := "SELECT Country, Uuid FROM test WHERE  id = 4 ORDER BY Date;"
	// query := "SELECT station, office FROM country WHERE NOT id=8 GROUP BY station, industry HAVING strange = true ORDER BY date;"
	// query := "SELECT * FROM users INNER JOIN products ON users.id = products.id WHERE users.price = 4"

	query := `
	SELECT Employees.LastName, COUNT(Orders.OrderID) AS NumberOfOrders
	FROM Orders
	INNER JOIN Employees ON Orders.EmployeeID = Employees.EmployeeID
	WHERE LastName = 'Davolio' OR LastName = 'Fuller'
	GROUP BY LastName
	HAVING COUNT(Orders.OrderID) > 25;
	`
	parsdStmt, err := parseQuery(query)
	if err != nil {
		panic(err)
	}
	// spew.Dump(parsdStmt)

	columns = getColumnNames(parsdStmt, columns, level)
	tables = getTableNames(parsdStmt, tables, level)

	fmt.Println("Columns:", columns)
	fmt.Println("Tables:", tables)
}

func getColumnNames(s interface{}, columns []string, level int) []string {

	sType := fmt.Sprintf("%T", s)

	switch sType {
	case "*sqlparser.Select":
		// Checking for unique columns in Select expresion from statement
		selectExprs := s.(*sqlparser.Select).SelectExprs
		if len(selectExprs) > 0 {
			for _, selectExpr := range selectExprs {
				columns = getColumnNames(selectExpr, columns, level+1)
			}
		}

		// checking for unique columns in the FROM section of statement
		from := s.(*sqlparser.Select).From
		columns = getColumnNames(from, columns, level+1)

		// Checking for unique columns in OrderBy clause from statement
		orderBy := s.(*sqlparser.Select).OrderBy
		if len(orderBy) > 0 {
			for _, order := range orderBy {
				columns = getColumnNames(order, columns, level+1)
			}
		}

		// Checking for unique columns in Where clause from statement
		whereClause := s.(*sqlparser.Select).Where
		if whereClause != nil {
			columns = getColumnNames(whereClause, columns, level+1)
		}

		// Checking for unique columns in Having clause
		having := s.(*sqlparser.Select).Having
		if having != nil {
			// for some reason, this is also of type *sqlparser.Where
			columns = getColumnNames(having, columns, level+1)
		}

		// checking for unique columns in GroupBy clause
		groupBy := s.(*sqlparser.Select).GroupBy
		if len(groupBy) > 0 {
			for _, group := range groupBy {
				columns = getColumnNames(group, columns, level+1)
			}
		}

	case "*sqlparser.AliasedExpr":
		aliasedExp := s.(*sqlparser.AliasedExpr)
		colExpr := aliasedExp.Expr
		columns = getColumnNames(colExpr, columns, level+1)

	case "*sqlparser.FuncExpr":
		// This is to handle function commands like:
		// SUM, COUNT, AVG, MIN, MAX etc.
		funcExpr := s.(*sqlparser.FuncExpr)
		for _, expr := range funcExpr.Exprs {
			columns = getColumnNames(expr, columns, level+1)
		}

	case "*sqlparser.Order":
		order := s.(*sqlparser.Order).Expr
		columns = getColumnNames(order, columns, level+1)

	case "*sqlparser.Where":
		where := s.(*sqlparser.Where).Expr
		columns = getColumnNames(where, columns, level+1)

	case "*sqlparser.ColName":
		colIdent := s.(*sqlparser.ColName).Name
		colName := colIdent.String()

		if colName != "" {
			exists := false
			// making sure colNames are nt repeated
			for _, column := range columns {
				if colName == column {
					exists = true
				}
			}
			if !exists {
				columns = append(columns, colName)
			}
		}

	case "*sqlparser.StarExpr":
		columns = append(columns, "*")

	case "*sqlparser.AndExpr":
		leftExpr := s.(*sqlparser.AndExpr).Left
		columns = getColumnNames(leftExpr, columns, level+1)

		right := s.(*sqlparser.AndExpr).Right
		columns = getColumnNames(right, columns, level+1)

	case "*sqlparser.OrExpr":
		leftExpr := s.(*sqlparser.OrExpr).Left
		columns = getColumnNames(leftExpr, columns, level+1)

		right := s.(*sqlparser.OrExpr).Right
		columns = getColumnNames(right, columns, level+1)

	case "*sqlparser.NotExpr":
		expr := s.(*sqlparser.NotExpr).Expr
		columns = getColumnNames(expr, columns, level+1)

	case "*sqlparser.ComparisonExpr":
		leftSide := s.(*sqlparser.ComparisonExpr).Left
		columns = getColumnNames(leftSide, columns, level+1)

	case "sqlparser.TableExprs":
		for _, tableExpr := range s.(sqlparser.TableExprs) {
			columns = getColumnNames(tableExpr, columns, level+1)
		}

	case "*sqlparser.AliasedTableExpr":
		// Do nothing

	case "*sqlparser.JoinTableExpr":
		condition := s.(*sqlparser.JoinTableExpr).Condition
		columns = getColumnNames(condition, columns, level+1)

	case "sqlparser.JoinCondition":
		onCondition := s.(sqlparser.JoinCondition).On
		columns = getColumnNames(onCondition, columns, level+1)

	default:
		panic(fmt.Sprintf("Error in recursive level [%d].\nThis type '(%s)' is unaccounted for\n", level, sType))

	}

	return columns
}

func getTableNames(s interface{}, tables []string, level int) []string {

	sType := fmt.Sprintf("%T", s)

	switch sType {

	case "*sqlparser.Select":
		tableExprs := s.(*sqlparser.Select).From
		if len(tableExprs) > 0 {
			for _, tableExpr := range tableExprs {
				tables = getTableNames(tableExpr, tables, level+1)
			}
		}

	case "*sqlparser.AliasedTableExpr":
		tableExpr := s.(*sqlparser.AliasedTableExpr).Expr
		tables = getTableNames(tableExpr, tables, level+1)

	case "*sqlparser.JoinTableExpr":
		leftExpr := s.(*sqlparser.JoinTableExpr).LeftExpr
		tables = getTableNames(leftExpr, tables, level+1)

		rightExpr := s.(*sqlparser.JoinTableExpr).RightExpr
		tables = getTableNames(rightExpr, tables, level+1)

	case "sqlparser.TableName":
		tableIdent := s.(sqlparser.TableName).Name
		tableName := tableIdent.String()

		if tableName != "" {
			exists := false
			// making sure tableNames are nt repeated
			for _, table := range tables {
				if tableName == table {
					exists = true
				}
			}
			if !exists {
				tables = append(tables, tableName)
			}
		}

	default:
		panic(fmt.Sprintf("Error in recursive level [%d].\nThis type '(%s)' is unaccounted for\n", level, sType))

	}

	return tables
}

func parseQuery(query string) (sqlparser.Statement, error) {

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, err
	}

	return stmt, nil
}

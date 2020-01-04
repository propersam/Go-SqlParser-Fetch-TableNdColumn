package main

import (
	"github.com/vitessio/vitess/go/vt/sqlparser"
)

// ParseQuery function receives a query as parameter and
// parses the query then fetch and return tables and columns
func ParseQuery(query string) ([]string, []string, error) {

	var (
		columns []string
		tables  []string
		level   int
	)

	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return nil, nil, err
	}
	// spew.Dump(stmt)

	columns = getColumnNames(stmt, columns, level)
	tables = getTableNames(stmt, tables, level)

	return tables, columns, nil
}

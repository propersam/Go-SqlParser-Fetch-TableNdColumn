package main

import (
	"fmt"
)

func main() {

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

	tables, columns, err := ParseQuery(query)
	if err != nil {
		panic(err)
	}

	fmt.Println("Columns:", columns)
	fmt.Println("Tables:", tables)
}

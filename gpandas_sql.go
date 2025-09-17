package gpandas

import (
	"context"
	"database/sql"
	"fmt"
	"gpandas/dataframe"
	"gpandas/utils/collection"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	_ "github.com/denisenkom/go-mssqldb" // SQL Server driver
)

// struct to store db config.
//
// NOTE: Prefer using env vars instead of hardcoding values
type DbConfig struct {
	Database_server string
	Server          string
	Port            string
	Database        string
	Username        string
	Password        string
}

func connect_to_db(db_config *DbConfig) (*sql.DB, error) {
	var connString string
	if db_config.Database_server == "sqlserver" {
		connString = fmt.Sprintf(
			"server=%s;user id=%s;password=%s;port=%s;database=%s",
			db_config.Server, db_config.Username, db_config.Password, db_config.Port, db_config.Database,
		)
	} else {
		connString = fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
			db_config.Server, db_config.Port, db_config.Username, db_config.Password, db_config.Database,
		)
	}
	DB, err := sql.Open(db_config.Database_server, connString)
	if err != nil {
		fmt.Printf("%s", err)
		return nil, err
	}
	return DB, err
}

// Read_sql executes a SQL query against a database and returns the results as a DataFrame.
//
// Parameters:
//
//	query: The SQL query string to execute.
//	db_config: A DbConfig struct containing database connection parameters:
//	  - database_server: Type of database ("sqlserver" or other)
//	  - server: Database server hostname or IP
//	  - port: Database server port
//	  - database: Database name
//	  - username: Database user
//	  - password: Database password
//
// Returns:
//   - A pointer to a DataFrame containing the query results.
//   - An error if the database connection, query execution, or data processing fails.
//
// The DataFrame's structure will match the query results:
//   - Columns will be named according to the SELECT statement
//   - Data types will be preserved from the database types
//
// Examples:
//
//	gp := gpandas.GoPandas{}
//	config := DbConfig{
//	    database_server: "sqlserver",
//	    server: "localhost",
//	    port: "1433",
//	    database: "mydb",
//	    username: "user",
//	    password: "pass",
//	}
//	query := `SELECT employee_id, name, department
//	          FROM employees
//	          WHERE department = 'Sales'`
//	df, err := gp.Read_sql(query, config)
//	// Result DataFrame:
//	// employee_id | name  | department
//	// 1          | John  | Sales
//	// 2          | Alice | Sales
//	// 3          | Bob   | Sales
func (GoPandas) Read_sql(query string, db_config DbConfig) (*dataframe.DataFrame, error) {
	DB, err := connect_to_db(&db_config)
	if err != nil {
		return nil, fmt.Errorf("database connection error: %w", err)
	}
	defer DB.Close()

	results, err := DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("query execution error: %w", err)
	}
	defer results.Close()

	// Get column names
	columns, err := results.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting columns: %w", err)
	}

	// Prepare per-column buffers
	columnCount := len(columns)
	colBuffers := make([][]any, columnCount)
	for i := range colBuffers {
		colBuffers[i] = make([]any, 0)
	}

	// Create a slice of interfaces to scan into
	values := make([]any, columnCount)
	valuePtrs := make([]any, columnCount)
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for results.Next() {
		err := results.Scan(valuePtrs...)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		// Add values to respective columns
		for i := range values {
			colBuffers[i] = append(colBuffers[i], values[i])
		}
	}

	if err := results.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	// Build Series per column
	cols := make(map[string]*collection.Series, columnCount)
	for i, name := range columns {
		s, err := collection.NewSeriesWithData(nil, colBuffers[i])
		if err != nil {
			return nil, fmt.Errorf("failed creating series for column %s: %w", name, err)
		}
		cols[name] = s
	}

	return &dataframe.DataFrame{Columns: cols, ColumnOrder: append([]string(nil), columns...)}, nil
}

// QueryBigQuery executes a BigQuery SQL query and returns the results as a DataFrame.
//
// Parameters:
//
//	query: The BigQuery SQL query string to execute.
//	projectID: The Google Cloud Project ID where the BigQuery dataset resides.
//
// Returns:
//   - A pointer to a DataFrame containing the query results.
//   - An error if the query execution fails or if there are issues with the BigQuery client.
//
// The DataFrame's structure will match the query results:
//   - Columns will be named according to the SELECT statement
//   - Data types will be converted from BigQuery types to Go types
//
// Examples:
//
//	gp := gpandas.GoPandas{}
//	query := `SELECT name, age, city
//	          FROM dataset.users
//	          WHERE age > 25`
//	df, err := gp.QueryBigQuery(query, "my-project-id")
//	// Result DataFrame:
//	// name    | age | city
//	// Alice   | 30  | New York
//	// Bob     | 35  | Chicago
//	// Charlie | 28  | Boston
//
// Note: Requires appropriate Google Cloud credentials to be configured in the environment.
func (GoPandas) From_gbq(query string, projectID string) (*dataframe.DataFrame, error) {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	q := client.Query(query)
	// q.UseStandardSQL = true  // Enable Standard SQL if needed
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("query.Read: %v", err)
	}

	// Read the first row to determine column names
	var firstRow map[string]bigquery.Value
	err = it.Next(&firstRow)
	if err == iterator.Done {
		return nil, fmt.Errorf("no rows returned")
	}
	if err != nil {
		return nil, fmt.Errorf("iterator.Next: %v", err)
	}

	// Extract column names from the first row's keys
	var columns []string
	for col := range firstRow {
		columns = append(columns, col)
	}

	// Initialize per-column buffers with first row
	colBuffers := make(map[string][]any, len(columns))
	for _, col := range columns {
		colBuffers[col] = []any{firstRow[col]}
	}

	// Process actual data here
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("iterator.Next: %v", err)
		}

		// Append values to column buffers
		for _, col := range columns {
			colBuffers[col] = append(colBuffers[col], row[col])
		}
	}

	// Build Series per column
	cols := make(map[string]*collection.Series, len(columns))
	for _, name := range columns {
		s, err := collection.NewSeriesWithData(nil, colBuffers[name])
		if err != nil {
			return nil, fmt.Errorf("failed creating series for column %s: %w", name, err)
		}
		cols[name] = s
	}

	return &dataframe.DataFrame{Columns: cols, ColumnOrder: append([]string(nil), columns...)}, nil
}

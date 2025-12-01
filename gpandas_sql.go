package gpandas

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"

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
//   - Data types will be inferred from the database types
//   - NULL values are properly tracked using the boolean mask approach
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

	// Get column names and types
	columns, err := results.Columns()
	if err != nil {
		return nil, fmt.Errorf("error getting columns: %w", err)
	}

	columnTypes, err := results.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("error getting column types: %w", err)
	}

	// Prepare per-column buffers with type information
	columnCount := len(columns)
	colBuffers := make([][]any, columnCount)
	colMasks := make([][]bool, columnCount)
	for i := range colBuffers {
		colBuffers[i] = make([]any, 0)
		colMasks[i] = make([]bool, 0)
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

		// Add values to respective columns with null tracking
		for i := range values {
			if values[i] == nil {
				colBuffers[i] = append(colBuffers[i], nil)
				colMasks[i] = append(colMasks[i], true)
			} else {
				colBuffers[i] = append(colBuffers[i], values[i])
				colMasks[i] = append(colMasks[i], false)
			}
		}
	}

	if err := results.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	// Build typed Series per column based on database column types
	cols := make(map[string]collection.Series, columnCount)
	for i, name := range columns {
		var s collection.Series
		var err error

		// Try to create typed series based on column scan type
		scanType := columnTypes[i].ScanType()
		if scanType != nil {
			switch scanType.Kind() {
			case reflect.Float64, reflect.Float32:
				s, err = createFloat64SeriesFromAny(colBuffers[i], colMasks[i])
			case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
				s, err = createInt64SeriesFromAny(colBuffers[i], colMasks[i])
			case reflect.Bool:
				s, err = createBoolSeriesFromAny(colBuffers[i], colMasks[i])
			case reflect.String:
				s, err = createStringSeriesFromAny(colBuffers[i], colMasks[i])
			default:
				// Fallback to AnySeries for complex types
				s, err = collection.NewAnySeriesFromData(colBuffers[i], colMasks[i])
			}
		} else {
			// No scan type available, use AnySeries
			s, err = collection.NewAnySeriesFromData(colBuffers[i], colMasks[i])
		}

		if err != nil {
			return nil, fmt.Errorf("failed creating series for column %s: %w", name, err)
		}
		cols[name] = s
	}

	// Create default index
	rowCount := 0
	if columnCount > 0 && len(colBuffers[0]) > 0 {
		rowCount = len(colBuffers[0])
	}
	index := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &dataframe.DataFrame{Columns: cols, ColumnOrder: append([]string(nil), columns...), Index: index}, nil
}

// Helper functions to create typed series from []any with masks

func createFloat64SeriesFromAny(values []any, mask []bool) (*collection.Float64Series, error) {
	data := make([]float64, len(values))
	for i, v := range values {
		if mask[i] || v == nil {
			continue
		}
		switch val := v.(type) {
		case float64:
			data[i] = val
		case float32:
			data[i] = float64(val)
		case int64:
			data[i] = float64(val)
		case int32:
			data[i] = float64(val)
		case int:
			data[i] = float64(val)
		default:
			// Try to handle other numeric types
			rv := reflect.ValueOf(v)
			if rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Float64 {
				data[i] = rv.Convert(reflect.TypeOf(float64(0))).Float()
			}
		}
	}
	return collection.NewFloat64SeriesFromData(data, mask)
}

func createInt64SeriesFromAny(values []any, mask []bool) (*collection.Int64Series, error) {
	data := make([]int64, len(values))
	for i, v := range values {
		if mask[i] || v == nil {
			continue
		}
		switch val := v.(type) {
		case int64:
			data[i] = val
		case int32:
			data[i] = int64(val)
		case int16:
			data[i] = int64(val)
		case int8:
			data[i] = int64(val)
		case int:
			data[i] = int64(val)
		case float64:
			data[i] = int64(val)
		case float32:
			data[i] = int64(val)
		default:
			// Try to handle other numeric types
			rv := reflect.ValueOf(v)
			if rv.Kind() >= reflect.Int && rv.Kind() <= reflect.Int64 {
				data[i] = rv.Int()
			}
		}
	}
	return collection.NewInt64SeriesFromData(data, mask)
}

func createStringSeriesFromAny(values []any, mask []bool) (*collection.StringSeries, error) {
	data := make([]string, len(values))
	for i, v := range values {
		if mask[i] || v == nil {
			continue
		}
		switch val := v.(type) {
		case string:
			data[i] = val
		case []byte:
			data[i] = string(val)
		default:
			data[i] = fmt.Sprintf("%v", v)
		}
	}
	return collection.NewStringSeriesFromData(data, mask)
}

func createBoolSeriesFromAny(values []any, mask []bool) (*collection.BoolSeries, error) {
	data := make([]bool, len(values))
	for i, v := range values {
		if mask[i] || v == nil {
			continue
		}
		switch val := v.(type) {
		case bool:
			data[i] = val
		case int64:
			data[i] = val != 0
		case int:
			data[i] = val != 0
		}
	}
	return collection.NewBoolSeriesFromData(data, mask)
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
//   - NULL values are properly tracked using the boolean mask approach
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
	colMasks := make(map[string][]bool, len(columns))
	for _, col := range columns {
		val := firstRow[col]
		if val == nil {
			colBuffers[col] = []any{nil}
			colMasks[col] = []bool{true}
		} else {
			colBuffers[col] = []any{val}
			colMasks[col] = []bool{false}
		}
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

		// Append values to column buffers with null tracking
		for _, col := range columns {
			val := row[col]
			if val == nil {
				colBuffers[col] = append(colBuffers[col], nil)
				colMasks[col] = append(colMasks[col], true)
			} else {
				colBuffers[col] = append(colBuffers[col], val)
				colMasks[col] = append(colMasks[col], false)
			}
		}
	}

	// Build typed Series per column (infer type from first non-null value)
	cols := make(map[string]collection.Series, len(columns))
	for _, name := range columns {
		s, err := createTypedSeriesFromBigQuery(colBuffers[name], colMasks[name])
		if err != nil {
			return nil, fmt.Errorf("failed creating series for column %s: %w", name, err)
		}
		cols[name] = s
	}

	// Create default index
	rowCount := 0
	if len(columns) > 0 {
		rowCount = len(colBuffers[columns[0]])
	}
	index := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &dataframe.DataFrame{Columns: cols, ColumnOrder: append([]string(nil), columns...), Index: index}, nil
}

// createTypedSeriesFromBigQuery creates a typed series by inferring type from BigQuery values
func createTypedSeriesFromBigQuery(values []any, mask []bool) (collection.Series, error) {
	// Find first non-null value to infer type
	var inferredType reflect.Type
	for i, v := range values {
		if !mask[i] && v != nil {
			inferredType = reflect.TypeOf(v)
			break
		}
	}

	if inferredType == nil {
		// All values are null
		return collection.NewAnySeriesFromData(values, mask)
	}

	// Create appropriate typed series based on inferred type
	switch inferredType.Kind() {
	case reflect.Float64, reflect.Float32:
		return createFloat64SeriesFromAny(values, mask)
	case reflect.Int64, reflect.Int32, reflect.Int16, reflect.Int8, reflect.Int:
		return createInt64SeriesFromAny(values, mask)
	case reflect.String:
		return createStringSeriesFromAny(values, mask)
	case reflect.Bool:
		return createBoolSeriesFromAny(values, mask)
	default:
		// Fallback to AnySeries for other types
		return collection.NewAnySeriesFromData(values, mask)
	}
}

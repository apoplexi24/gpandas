package gpandas

import (
	"context"
	"database/sql"
	"fmt"
	"gpandas/dataframe"
	"gpandas/utils/nullable"

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
	defer DB.Close()
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

	// Get column types
	columnTypes, err := results.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("error getting column types: %w", err)
	}

	// Create DataFrame
	df := dataframe.NewDataFrame(columns)

	// Create series for each column based on database type
	seriesMap := make(map[string]dataframe.Series, len(columns))

	for i, colName := range columns {
		dbType := columnTypes[i].DatabaseTypeName()

		// Determine series type based on database type
		var seriesType dataframe.SeriesType

		switch dbType {
		case "INT", "BIGINT", "SMALLINT", "TINYINT":
			seriesType = dataframe.IntType
		case "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC":
			seriesType = dataframe.FloatType
		case "BIT", "BOOLEAN":
			seriesType = dataframe.BoolType
		default:
			// Default to string for other types
			seriesType = dataframe.StringType
		}

		// Create empty series (we'll add rows as we scan)
		series := dataframe.CreateSeries(seriesType, colName, 0)
		seriesMap[colName] = series
		df.Series[colName] = series
	}

	// Scan rows
	for results.Next() {
		// Create a slice of interfaces to scan into
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into values
		if err := results.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		// Add a new row to each series
		for i, colName := range columns {
			series := df.Series[colName]

			// Create a properly typed value based on the Series type
			if values[i] == nil {
				// Add a null value
				switch series.(type) {
				case *dataframe.IntSeries:
					newSeries := dataframe.NewIntSeries(colName, series.Len()+1)
					for j := 0; j < series.Len(); j++ {
						newSeries.SetValue(j, series.GetValue(j))
					}
					newSeries.SetValue(series.Len(), nil)
					df.Series[colName] = newSeries
				case *dataframe.FloatSeries:
					newSeries := dataframe.NewFloatSeries(colName, series.Len()+1)
					for j := 0; j < series.Len(); j++ {
						newSeries.SetValue(j, series.GetValue(j))
					}
					newSeries.SetValue(series.Len(), nil)
					df.Series[colName] = newSeries
				case *dataframe.StringSeries:
					newSeries := dataframe.NewStringSeries(colName, series.Len()+1)
					for j := 0; j < series.Len(); j++ {
						newSeries.SetValue(j, series.GetValue(j))
					}
					newSeries.SetValue(series.Len(), nil)
					df.Series[colName] = newSeries
				case *dataframe.BoolSeries:
					newSeries := dataframe.NewBoolSeries(colName, series.Len()+1)
					for j := 0; j < series.Len(); j++ {
						newSeries.SetValue(j, series.GetValue(j))
					}
					newSeries.SetValue(series.Len(), nil)
					df.Series[colName] = newSeries
				}
			} else {
				// Add a non-null value
				switch series.(type) {
				case *dataframe.IntSeries:
					newSeries := dataframe.NewIntSeries(colName, series.Len()+1)
					for j := 0; j < series.Len(); j++ {
						newSeries.SetValue(j, series.GetValue(j))
					}

					// Convert to int64 as needed
					switch v := values[i].(type) {
					case int64:
						newSeries.SetValue(series.Len(), v)
					case int32:
						newSeries.SetValue(series.Len(), int64(v))
					case int:
						newSeries.SetValue(series.Len(), int64(v))
					default:
						// Try to convert to string then int
						newSeries.SetValue(series.Len(), fmt.Sprintf("%v", v))
					}

					df.Series[colName] = newSeries

				case *dataframe.FloatSeries:
					newSeries := dataframe.NewFloatSeries(colName, series.Len()+1)
					for j := 0; j < series.Len(); j++ {
						newSeries.SetValue(j, series.GetValue(j))
					}

					// Convert to float64 as needed
					switch v := values[i].(type) {
					case float64:
						newSeries.SetValue(series.Len(), v)
					case float32:
						newSeries.SetValue(series.Len(), float64(v))
					case int64:
						newSeries.SetValue(series.Len(), float64(v))
					case int32:
						newSeries.SetValue(series.Len(), float64(v))
					case int:
						newSeries.SetValue(series.Len(), float64(v))
					default:
						// Try to convert to string then float
						newSeries.SetValue(series.Len(), fmt.Sprintf("%v", v))
					}

					df.Series[colName] = newSeries

				case *dataframe.StringSeries:
					newSeries := dataframe.NewStringSeries(colName, series.Len()+1)
					for j := 0; j < series.Len(); j++ {
						newSeries.SetValue(j, series.GetValue(j))
					}

					// Convert anything to string
					newSeries.SetValue(series.Len(), fmt.Sprintf("%v", values[i]))
					df.Series[colName] = newSeries

				case *dataframe.BoolSeries:
					newSeries := dataframe.NewBoolSeries(colName, series.Len()+1)
					for j := 0; j < series.Len(); j++ {
						newSeries.SetValue(j, series.GetValue(j))
					}

					// Convert to bool as needed
					switch v := values[i].(type) {
					case bool:
						newSeries.SetValue(series.Len(), v)
					case int64:
						newSeries.SetValue(series.Len(), v != 0)
					case int32:
						newSeries.SetValue(series.Len(), v != 0)
					case int:
						newSeries.SetValue(series.Len(), v != 0)
					default:
						// Try to convert to string then bool
						newSeries.SetValue(series.Len(), fmt.Sprintf("%v", v))
					}

					df.Series[colName] = newSeries
				}
			}
		}
	}

	if err := results.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over rows: %w", err)
	}

	return df, nil
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

	// Get schema information to determine column names and types
	schema := it.Schema
	if len(schema) == 0 {
		return nil, fmt.Errorf("no schema information available")
	}

	columns := make([]string, len(schema))
	for i, field := range schema {
		columns[i] = field.Name
	}

	// Create DataFrame
	df := dataframe.NewDataFrame(columns)

	// Create series for each column based on BigQuery type
	for _, field := range schema {
		var seriesType dataframe.SeriesType

		// Map BigQuery types to Series types
		switch field.Type {
		case bigquery.IntegerFieldType:
			seriesType = dataframe.IntType
		case bigquery.FloatFieldType, bigquery.NumericFieldType:
			seriesType = dataframe.FloatType
		case bigquery.BooleanFieldType:
			seriesType = dataframe.BoolType
		default:
			// Default to string for other types
			seriesType = dataframe.StringType
		}

		series := dataframe.CreateSeries(seriesType, field.Name, 0)
		df.Series[field.Name] = series
	}

	// Process rows
	for {
		var row map[string]bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("iterator.Next: %v", err)
		}

		// Process each column in the row
		for _, colName := range columns {
			value := row[colName]
			series := df.Series[colName]

			// Resize the series to add a new row
			switch s := series.(type) {
			case *dataframe.IntSeries:
				newSeries := dataframe.NewIntSeries(colName, s.Len()+1)
				for i := 0; i < s.Len(); i++ {
					newSeries.SetValue(i, s.GetValue(i))
				}

				if value == nil {
					newSeries.SetValue(s.Len(), nil)
				} else {
					// Convert to int64 as needed
					switch v := value.(type) {
					case int64:
						newSeries.SetValue(s.Len(), v)
					case int32:
						newSeries.SetValue(s.Len(), int64(v))
					case int:
						newSeries.SetValue(s.Len(), int64(v))
					default:
						// Try to convert
						newSeries.SetValue(s.Len(), nullable.FromAny(v))
					}
				}

				df.Series[colName] = newSeries

			case *dataframe.FloatSeries:
				newSeries := dataframe.NewFloatSeries(colName, s.Len()+1)
				for i := 0; i < s.Len(); i++ {
					newSeries.SetValue(i, s.GetValue(i))
				}

				if value == nil {
					newSeries.SetValue(s.Len(), nil)
				} else {
					// Convert to float64 as needed
					switch v := value.(type) {
					case float64:
						newSeries.SetValue(s.Len(), v)
					case float32:
						newSeries.SetValue(s.Len(), float64(v))
					case int64:
						newSeries.SetValue(s.Len(), float64(v))
					default:
						// Try to convert
						newSeries.SetValue(s.Len(), nullable.FromAny(v))
					}
				}

				df.Series[colName] = newSeries

			case *dataframe.StringSeries:
				newSeries := dataframe.NewStringSeries(colName, s.Len()+1)
				for i := 0; i < s.Len(); i++ {
					newSeries.SetValue(i, s.GetValue(i))
				}

				if value == nil {
					newSeries.SetValue(s.Len(), nil)
				} else {
					// Convert to string
					newSeries.SetValue(s.Len(), fmt.Sprintf("%v", value))
				}

				df.Series[colName] = newSeries

			case *dataframe.BoolSeries:
				newSeries := dataframe.NewBoolSeries(colName, s.Len()+1)
				for i := 0; i < s.Len(); i++ {
					newSeries.SetValue(i, s.GetValue(i))
				}

				if value == nil {
					newSeries.SetValue(s.Len(), nil)
				} else {
					// Convert to bool as needed
					switch v := value.(type) {
					case bool:
						newSeries.SetValue(s.Len(), v)
					default:
						// Try to convert
						newSeries.SetValue(s.Len(), nullable.FromAny(v))
					}
				}

				df.Series[colName] = newSeries
			}
		}
	}

	return df, nil
}

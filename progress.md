# gpandas Library Features

This document outlines the user-facing features available in the `gpandas` library.

### DataFrame Creation

*   **`DataFrame(columns []string, data []Column, columns_types map[string]any) (*dataframe.DataFrame, error)`**: Creates a new DataFrame from in-memory data. You provide the column names, the data itself as a slice of columns, and a map defining the data type for each column.

### Data I/O (Reading and Writing)

*   **`Read_csv(filepath string) (*dataframe.DataFrame, error)`**: Reads data from a CSV file into a DataFrame. It uses the first row as headers and is optimized for performance with large files.
*   **`ToCSV(filepath string, separator ...string) (string, error)`**: Exports a DataFrame to a CSV format. You can either write it to a file by providing a `filepath` or get the CSV content as a string if the `filepath` is empty. You can also specify a custom separator.
*   **`Read_sql(query string, db_config DbConfig) (*dataframe.DataFrame, error)`**: Executes a SQL query against a relational database (e.g., SQL Server, PostgreSQL) and returns the result as a DataFrame. It requires a `db_config` struct with connection details.
*   **`From_gbq(query string, projectID string) (*dataframe.DataFrame, error)`**: Executes a query on Google BigQuery and loads the results into a DataFrame. You need to provide the BigQuery query string and your Google Cloud Project ID.

### DataFrame Operations

*   **`Rename(columns map[string]string) error`**: Renames one or more columns in a DataFrame. You provide a map where the keys are the old column names and the values are the new names.
*   **`Merge(other *DataFrame, on string, how MergeHow) (*DataFrame, error)`**: Merges two DataFrames based on a common column. It supports `inner`, `left`, `right`, and `full` joins.
*   **`String() string`**: Returns a formatted, human-readable string representation of the DataFrame, similar to how pandas displays DataFrames in Python.

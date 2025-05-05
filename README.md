<p align="center">
  <img src="https://github.com/user-attachments/assets/2a0d2716-33ec-449d-a5fc-a9f95b8df9d9" />
</p>

# GPandas

GPandas is a high-performance data manipulation and analysis library written in Go, drawing inspiration from Python's popular pandas library. It provides efficient and easy-to-use data structures, primarily the DataFrame, to handle structured data in Go applications.

## Project Structure

The project is organized into the following directories and files:

```
├── .gitignore
├── README.md
├── benchmark
│   ├── read_csv.go
│   ├── read_csv.py
│   ├── read_gbq.go
│   ├── read_gbq.py
│   └── sql_commands.go
├── dataframe
│   ├── DataFrame.go
│   ├── merge.go
│   └── series.go
├── go.mod
├── go.sum
├── gpandas.go
├── gpandas_sql.go
├── tests
│   ├── dataframe
│   │   └── dataframe_test.go
│   ├── gpandas_sql_test.go
│   ├── gpandas_test.go
│   └── utils
│       └── collection
│           └── set_test.go
├── utils
│   ├── collection
│   │   └── set.go
│   └── nullable
│       └── nullable_types.go
```

- **`.gitignore`**: Specifies intentionally untracked files that Git should ignore. Currently ignores CSV files, executables, and environment files (`.env`).
- **`README.md`**: The current file, providing an overview of the GPandas library, its features, project structure, and usage instructions.
- **`benchmark/`**: Contains benchmark scripts for performance evaluation against Python's pandas:
    - **`read_csv.go` & `read_csv.py`**: Benchmark Go GPandas and Python Pandas CSV reading performance.
    - **`read_gbq.go` & `read_gbq.py`**: Benchmark Go GPandas and Python Pandas-GBQ reading from Google BigQuery.
    - **`sql_commands.go`**: Example Go script demonstrating SQL query execution against BigQuery using GPandas.
- **`dataframe/`**:  Houses the core DataFrame implementation:
    - **`DataFrame.go`**: Defines the `DataFrame` struct and fundamental DataFrame operations such as:
        - `Rename()`: For renaming columns.
        - `String()`: For pretty printing DataFrame content as a formatted table in string format.
        - `ToCSV()`: For exporting DataFrame content to CSV format, either as a string or to a file.
        - `MergeWithOptions()`: For merging DataFrames with configurable options.
    - **`merge.go`**: Implements DataFrame merging capabilities, supporting various join types:
        - `Merge()`:  Main function to merge two DataFrames based on a common column and specified merge type (inner, left, right, full outer).
        - `performInnerMerge()`, `performLeftMerge()`, `performRightMerge()`, `performFullMerge()`: Internal functions implementing the different merge algorithms.
    - **`series.go`**: Defines the Series interface and implementations for different data types:
        - `IntSeries`: For nullable int64 values.
        - `FloatSeries`: For nullable float64 values.
        - `StringSeries`: For nullable string values.
        - `BoolSeries`: For nullable boolean values.
- **`go.mod` & `go.sum`**: Go module files that manage project dependencies and their checksums for reproducible builds.
- **`gpandas.go`**: Serves as the primary entry point for the GPandas library. It provides high-level API functions for DataFrame creation and data loading:
    - `DataFrame()`: Constructor to create a new DataFrame from columns, data, and column type definitions.
    - `Read_csv()`: Functionality to read data from a CSV file and create a DataFrame. It uses concurrent processing for efficient CSV parsing.
- **`gpandas_sql.go`**:  Extends GPandas to interact with SQL databases and Google BigQuery:
    - `Read_sql()`: Enables reading data from relational databases (like SQL Server, PostgreSQL) by executing a SQL query and returning the result as a DataFrame.
    - `From_gbq()`: Provides functionality to query Google BigQuery and load the results into a DataFrame.
- **`tests/`**: Contains unit tests to ensure the correctness and robustness of GPandas:
    - **`dataframe/dataframe_test.go`**: Tests for core DataFrame operations defined in `dataframe/DataFrame.go` and `dataframe/merge.go` (e.g., `Rename`, `String`, `Merge`, `ToCSV`).
    - **`gpandas_sql_test.go`**: Tests for SQL related functionalities in `gpandas_sql.go` (`Read_sql`, `From_gbq`).
    - **`gpandas_test.go`**: Tests for general GPandas functionalities in `gpandas.go` (e.g., `Read_csv`).
    - **`utils/collection/set_test.go`**: Unit tests for the generic `Set` data structure implemented in `utils/collection/set.go`.
- **`utils/collection/`**: Contains generic collection utilities:
    - **`set.go`**: Implements a generic `Set` data structure in Go, providing common set operations like `Add`, `Has`, `Union`, `Intersect`, `Difference`, and `Compare`. This `Set` is used internally within GPandas for efficient data handling.
- **`utils/nullable/`**: Implements nullable types for handling missing data:
    - **`nullable_types.go`**: Defines wrapper-based nullable types for different data types (int64, float64, string, bool).

## Recent Updates

### Null Safety Implementation

GPandas now features robust null handling through a wrapper-based approach:

- **Wrapper-based Nullable Types**: 
  - Implemented as structs with `Value` and `Valid` fields (e.g., `NullableInt{Value int64, Valid bool}`)
  - Provides better cache locality and reduces heap fragmentation compared to pointer-based approaches
  - Supports vectorized operations efficiently

### Series Interface Abstraction

A new Series interface has been implemented to abstract data storage details:

- **Strong Type Safety**: Each Series implementation maintains type safety for its values
- **Common Operations**: Standardized methods for accessing, modifying, and querying Series data
- **Multiple Implementations**: Support for different data types through specialized Series:
  - `IntSeries`: For nullable int64 values
  - `FloatSeries`: For nullable float64 values 
  - `StringSeries`: For nullable string values
  - `BoolSeries`: For nullable boolean values

### Improved Error Handling in CSV Parsing

Enhanced error handling for CSV parsing with:

- **Context-based Cancellation**: Properly stops all goroutines when errors occur
- **Detailed Error Messages**: More informative error messages, particularly for inconsistent column counts
- **Consistent Error Propagation**: Better error propagation through the processing pipeline

### Functional Options Pattern

Implemented the Functional Options Pattern for more flexible API design:

- **Backward Compatible**: All existing functions maintain backward compatibility
- **Enhanced Configuration**: Added flexible configuration options for various operations
- **New Options**:
  - `WithCSVSeparator`: Set custom CSV field separator
  - `WithCSVQuote`: Set custom CSV quote character
  - `WithCSVAutoType`: Enable automatic type detection for CSV fields
  - `WithMaxRows`: Limit maximum rows to display or process
  - `WithWorkerCount`: Control concurrency level for operations
  - `WithMergeOn`: Specify columns to merge on with same name
  - `WithMergeColumns`: Specify different columns to merge on
  - `WithMergeSuffix`: Configure suffix handling for duplicate column names

## Code Functionality

GPandas is designed to provide a familiar and efficient way to work with tabular data in Go. Key functionalities include:

### Core DataFrame Operations

- **DataFrame Creation**: Construct DataFrames from in-memory data using `gpandas.DataFrame()`, or load from external sources like CSV files using `gpandas.Read_csv()`.
- **Column Manipulation**:
    - **Renaming**: Easily rename columns using `DataFrame.Rename()`.
- **Data Merging**: Combine DataFrames based on common columns with:
    - **`DataFrame.Merge()`**: Traditional approach with fixed parameters.
    - **`DataFrame.MergeWithOptions()`**: Flexible approach using functional options.
    - Supports various join types:
      - **Inner Join (`InnerMerge`)**: Keep only matching rows from both DataFrames.
      - **Left Join (`LeftMerge`)**: Keep all rows from the left DataFrame, and matching rows from the right.
      - **Right Join (`RightMerge`)**: Keep all rows from the right DataFrame, and matching rows from the left.
      - **Full Outer Join (`FullMerge`)**: Keep all rows from both DataFrames, filling in missing values with `nil`.
- **Null Handling**:
    - **`IsNA()`**: Check if a value is null/NA.
    - **`FillNA()`**: Replace null values with a specified value.
    - **`DropNA()`**: Remove rows containing null values.
- **Data Export**:
    - **CSV Export**:  Export DataFrames to CSV format using `DataFrame.ToCSV()`, with options for:
        - Custom separators.
        - Writing to a file path or returning a CSV string.
- **Data Display**:
    - **Pretty Printing**:  Generate formatted, human-readable table representations of DataFrames using `DataFrame.String()`.

### Data Loading from External Sources

- **CSV Reading**: Efficiently read CSV files into DataFrames with `gpandas.Read_csv()`, leveraging concurrent processing for performance.
- **SQL Database Integration**:
    - **`Read_sql()`**: Query and load data from SQL databases (SQL Server, PostgreSQL, and others supported by Go database/sql package) into DataFrames.
- **Google BigQuery Support**:
    - **`From_gbq()`**: Query and load data from Google BigQuery tables into DataFrames, enabling analysis of large datasets stored in BigQuery.

### Data Types and Series

GPandas provides strong type support through Series implementations:

- **`IntSeries`**: For nullable `int64` values
- **`FloatSeries`**: For nullable `float64` values
- **`StringSeries`**: For nullable `string` values
- **`BoolSeries`**: For nullable `bool` values

The Series interface provides a common API for all data types while ensuring type safety.

### Performance Features

GPandas is built with performance in mind, incorporating several features for efficiency:

- **Concurrent CSV Reading**: Utilizes worker pools and buffered channels for parallel CSV parsing, significantly speeding up CSV loading, especially for large files.
- **Efficient Data Structures**:  Uses Go's native data structures and generics to minimize overhead and maximize performance.
- **Mutex-based Thread Safety**:  Provides thread-safe operations for DataFrame manipulations using mutex locks, ensuring data consistency in concurrent environments.
- **Optimized Memory Management**: Designed for efficient memory usage to handle large datasets effectively.
- **Buffered Channels**: Employs buffered channels for data processing pipelines to improve throughput and reduce blocking.

## Getting Started

### Prerequisites

GPandas requires **Go version 1.18 or above** due to its use of generics.

### Installation

Install GPandas using `go get`:

```bash
go get github.com/apoplexi24/gpandas
```

### Basic Usage

```go
package main

import (
    "fmt"
    "gpandas"
)

func main() {
    // Read CSV with default settings
    pd := gpandas.GoPandas{}
    df, err := pd.Read_csv("data.csv")
    if err != nil {
        panic(err)
    }
    
    // Display the DataFrame
    fmt.Println(df.String())
    
    // Using functional options
    df2, err := pd.Read_csv("data.csv", 
        gpandas.WithCSVSeparator(';'),
        gpandas.WithCSVAutoType(true),
        gpandas.WithMaxRows(100))
    if err != nil {
        panic(err)
    }
    
    // Merge DataFrames
    result, err := df.MergeWithOptions(df2, gpandas.InnerMerge,
        gpandas.WithMergeOn("ID"))
    if err != nil {
        panic(err)
    }
    
    // Export to CSV
    csvString, err := result.ToCSV("")
    if err != nil {
        panic(err)
    }
    fmt.Println(csvString)
}
```

## Acknowledgments

- Inspired by Python's pandas library, aiming to bring similar data manipulation capabilities to the Go ecosystem.
- Built using Go's powerful generic system for type safety and performance.
- Thanks to the Go community for valuable feedback and contributions.

## Status

GPandas is under active development and is suitable for production use. However, it's still evolving, with ongoing efforts to add more features, enhance performance, and improve API ergonomics. Expect continued updates and improvements.
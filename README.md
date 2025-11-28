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
│   └── merge.go
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
│           ├── set_test.go
│           └── series_test.go
└── utils
    └── collection
        ├── set.go
        └── series.go
```

- **`.gitignore`**: Specifies intentionally untracked files that Git should ignore. Currently ignores CSV files, executables, and environment files (`.env`).
- **`README.md`**: The current file, providing an overview of the GPandas library, its features, project structure, and usage instructions.
- **`benchmark/`**: Contains benchmark scripts for performance evaluation against Python's pandas:
    - **`read_csv.go` & `read_csv.py`**: Benchmark Go GPandas and Python Pandas CSV reading performance.
    - **`read_gbq.go` & `read_gbq.py`**: Benchmark Go GPandas and Python Pandas-GBQ reading from Google BigQuery.
    - **`sql_commands.go`**: Example Go script demonstrating SQL query execution against BigQuery using GPandas.
- **`dataframe/`**:  Houses the core DataFrame implementation:
    - **`DataFrame.go`**: Defines the columnar `DataFrame` struct with `Columns map[string]*Series` and `ColumnOrder []string`, along with fundamental DataFrame operations such as:
        - `Rename()`: For renaming columns while preserving order.
        - `String()`: For pretty printing DataFrame content as a formatted table in string format.
        - `ToCSV()`: For exporting DataFrame content to CSV format, either as a string or to a file.
    - **`merge.go`**: Implements DataFrame merging capabilities, supporting various join types:
        - `Merge()`:  Main function to merge two DataFrames based on a common column and specified merge type (inner, left, right, full outer).
        - `performInnerMerge()`, `performLeftMerge()`, `performRightMerge()`, `performFullMerge()`: Internal functions implementing the different merge algorithms.
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
    - **`utils/collection/series_test.go`**: Unit tests for the `Series` data structure, covering basic operations, type enforcement, and concurrency safety.
- **`utils/collection/`**: Contains generic collection utilities:
    - **`set.go`**: Implements a generic `Set` data structure in Go, providing common set operations like `Add`, `Has`, `Union`, `Intersect`, `Difference`, and `Compare`. This `Set` is used internally within GPandas for efficient data handling.
    - **`series.go`**: Implements a concurrency-safe `Series` type that enforces homogeneous data types within columns. Each Series maintains a `dtype` and provides methods like `At()`, `Set()`, `Append()`, and `Len()` for efficient columnar data access.

## Code Functionality

GPandas is designed to provide a familiar and efficient way to work with tabular data in Go. Key functionalities include:

### Core DataFrame Operations

- **DataFrame Creation**: Construct columnar DataFrames from in-memory data using `gpandas.DataFrame()`, or load from external sources like CSV files using `gpandas.Read_csv()`. Each DataFrame uses a `map[string]*Series` structure for efficient columnar access.
- **Column Manipulation**:
    - **Renaming**: Easily rename columns using `DataFrame.Rename()` while preserving column order.
- **Data Merging**: Combine DataFrames based on common columns with `DataFrame.Merge()`, supporting:
    - **Inner Join (`InnerMerge`)**: Keep only matching rows from both DataFrames.
    - **Left Join (`LeftMerge`)**: Keep all rows from the left DataFrame, and matching rows from the right.
    - **Right Join (`RightMerge`)**: Keep all rows from the right DataFrame, and matching rows from the left.
    - **Full Outer Join (`FullMerge`)**: Keep all rows from both DataFrames, filling in missing values with `nil`.
- **Data Export**:
    - **CSV Export**:  Export DataFrames to CSV format using `DataFrame.ToCSV()`, with options for:
        - Custom separators.
        - Writing to a file path or returning a CSV string.
- **Data Display**:
    - **Pretty Printing**:  Generate formatted, human-readable table representations of DataFrames using `DataFrame.String()`.

### Indexing and Selection

GPandas provides pandas-like indexing capabilities for intuitive data access:

- **Column Selection**:
    - **`Select(columns ...string)`**: Select one or more columns, returning a new DataFrame.
    - **`SelectCol(column string)`**: Select a single column as a Series.
- **Label-based Indexing (`Loc`)**:
    - **`Loc().At(rowLabel, colName)`**: Access a single value by row label and column name.
    - **`Loc().Row(rowLabel)`**: Select a single row by label as a DataFrame.
    - **`Loc().Rows(rowLabels)`**: Select multiple rows by labels.
    - **`Loc().Col(colName)`**: Select a single column by name.
    - **`Loc().Cols(colNames)`**: Select multiple columns by names.
- **Position-based Indexing (`iLoc`)**:
    - **`iLoc().At(rowPos, colPos)`**: Access a single value by row and column integer positions.
    - **`iLoc().Row(rowPos)`**: Select a single row by integer position.
    - **`iLoc().Rows(rowPositions)`**: Select multiple rows by integer positions.
    - **`iLoc().Range(start, end)`**: Select a range of rows `[start, end)`.
    - **`iLoc().Col(colPos)`**: Select a single column by integer position.
    - **`iLoc().Cols(colPositions)`**: Select multiple columns by integer positions.
- **Index Management**:
    - **`SetIndex([]string)`**: Set custom row labels.
    - **`ResetIndex()`**: Reset index to default integer sequence.

### Data Loading from External Sources

- **CSV Reading**: Efficiently read CSV files into DataFrames with `gpandas.Read_csv()`, leveraging concurrent processing for performance.
- **SQL Database Integration**:
    - **`Read_sql()`**: Query and load data from SQL databases (SQL Server, PostgreSQL, and others supported by Go database/sql package) into DataFrames.
- **Google BigQuery Support**:
    - **`From_gbq()`**: Query and load data from Google BigQuery tables into DataFrames, enabling analysis of large datasets stored in BigQuery.

### Data Types

GPandas provides strong type support through its columnar architecture:

- **`Series`**: The fundamental column type that enforces homogeneous data types within each column. Each Series maintains a `dtype` and provides type-safe access methods.
- **`FloatCol`**: For `float64` columns (legacy type for DataFrame construction).
- **`StringCol`**: For `string` columns (legacy type for DataFrame construction).
- **`IntCol`**: For `int64` columns (legacy type for DataFrame construction).
- **`BoolCol`**: For `bool` columns (legacy type for DataFrame construction).
- **`Column`**: Generic column type to hold `any` type values when specific type constraints are not needed.
- **`TypeColumn[T comparable]`**: Generic column type for columns of any comparable type `T`.

GPandas ensures type safety through Series-level dtype enforcement, preventing type mismatches and ensuring data integrity across all operations.

### Performance Features

GPandas is built with performance in mind, incorporating several features for efficiency:

- **Columnar Storage**: Uses a columnar DataFrame structure (`map[string]*Series`) for efficient column-wise operations and memory layout, similar to modern analytical databases.
- **Concurrent CSV Reading**: Utilizes worker pools and buffered channels for parallel CSV parsing, significantly speeding up CSV loading, especially for large files.
- **Efficient Data Structures**:  Uses Go's native data structures and generics to minimize overhead and maximize performance.
- **Series-level Thread Safety**:  Provides thread-safe operations at the Series level using RWMutex, ensuring data consistency in concurrent environments while allowing concurrent reads.
- **Optimized Memory Management**: Designed for efficient memory usage with columnar storage to handle large datasets effectively.
- **Buffered Channels**: Employs buffered channels for data processing pipelines to improve throughput and reduce blocking.

## Getting Started

### Prerequisites

GPandas requires **Go version 1.18 or above** due to its use of generics.

### Installation

Install GPandas using `go get`:

```bash
go get github.com/apoplexi24/gpandas
```

## Core Components

### DataFrame

The central data structure in GPandas, the `DataFrame`, is designed for handling two-dimensional, labeled data using a columnar architecture. It consists of a `map[string]*Series` for column storage and a `ColumnOrder []string` for maintaining column sequence. This design provides methods for data manipulation, analysis, and I/O operations, similar to pandas DataFrames in Python but with improved performance characteristics.

### Series

The `utils/collection/series.go` provides a concurrency-safe `Series` type that serves as the fundamental building block for DataFrame columns. Each Series enforces homogeneous data types and provides efficient access methods like `At()`, `Set()`, `Append()`, and `Len()`.

### Set

The `utils/collection/set.go` provides a generic `Set` implementation, useful for various set operations. While not directly exposed as a primary user-facing component, it's an important utility within GPandas for efficient data management and algorithm implementations.

## Performance

GPandas is engineered for performance through:

- **Columnar Architecture**: The `map[string]*Series` structure enables efficient column-wise operations and better memory locality, similar to modern analytical databases.
- **Generics**: Leveraging Go generics to avoid runtime type assertions and interface overhead, leading to faster execution.
- **Efficient Memory Usage**:  Designed to minimize memory allocations and copies with columnar storage for better performance when dealing with large datasets.
- **Concurrency**: Utilizing Go's concurrency features, such as goroutines and channels, to parallelize operations like CSV reading and potentially other data processing tasks in the future.
- **Series-level Optimization**: Each Series maintains its own type information and provides optimized access patterns for columnar data.
- **Zero-copy Operations**:  Aiming for zero-copy operations wherever feasible to reduce overhead and improve speed.

### Development Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/apoplexi24/gpandas.git
   cd gpandas
   ```
2. **Install dependencies**:
   ```bash
   go mod download
   ```

## Acknowledgments

- Inspired by Python's pandas library, aiming to bring similar data manipulation capabilities to the Go ecosystem.
- Built using Go's powerful generic system for type safety and performance.
- Thanks to the Go community for valuable feedback and contributions.

## Status

GPandas is under active development and is suitable for production use. However, it's still evolving, with ongoing efforts to add more features, enhance performance, and improve API ergonomics. Expect continued updates and improvements.
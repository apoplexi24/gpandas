<p align="center">
  <img src="https://github.com/user-attachments/assets/2a0d2716-33ec-449d-a5fc-a9f95b8df9d9" />
</p>

# GPandas

GPandas is a high-performance data manipulation and analysis library written in Go, drawing inspiration from Python's popular pandas library. It provides efficient and easy-to-use data structures, primarily the DataFrame, to handle structured data in Go applications.

## Project Structure

- **`benchmark/`**: Contains benchmark scripts for performance evaluation against Python's pandas:
- **`dataframe/`**:  Houses the core DataFrame implementation:
- **`plot/`**: Provides interactive chart generation capabilities using go-echarts v2:
    - **`bar.go`**: Bar chart rendering functions
    - **`pie.go`**: Pie chart rendering functions
    - **`line.go`**: Line chart rendering functions (single and multi-series)
    - **`options.go`**: Chart configuration options and defaults
    - **`utils.go`**: Type conversion and validation utilities
- **`gpandas.go`**: Serves as the primary entry point for the GPandas library. It provides high-level API functions for DataFrame creation and data loading.
- **`gpandas_sql.go`**:  Extends GPandas to interact with SQL databases and Google BigQuery:
- **`tests/`**: Contains unit tests to ensure the correctness and robustness of GPandas. It follows the exact same dir structure as the project for easy navigation.
- **`examples/`**: Contains example programs demonstrating GPandas features:
    - **`plot/`**: Chart generation examples for bar, pie, line charts, and customization
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

- **Column Selection**
- **Label-based Indexing (`Loc`)**
- **Position-based Indexing (`iLoc`)**
- **Index Management**

### Filtering and Selection by Condition

GPandas supports pandas-like boolean filtering for row subsetting. `Filter` and `Where` return a chainable, error-deferred `FilterChain`; terminate the chain with `.Result()` (or `.MustResult()`):

- **`Filter()`**: Keep rows where a column satisfies a comparison, e.g. `df.Filter("Age", dataframe.GreaterThan, int64(25)).Result()`. Supported operators: `Equals`, `NotEquals`, `GreaterThan`, `GreaterThanOrEqual`, `LessThan`, `LessThanOrEqual`. Numeric comparisons work across `int`/`int64`/`float64`, and null values never match a comparison.
- **Chaining**: Conditions can be combined fluently; the first error is carried through and surfaced by `.Result()`:
  ```go
  result, err := df.
      Filter("Department", dataframe.Equals, "Engineering").
      Filter("Salary", dataframe.GreaterThan, 90000.0).
      Result()
  ```
- **`Where()`**: Keep rows for which a predicate returns true. The predicate receives a `map[string]any` of the row (nulls as `nil`), enabling arbitrary multi-column conditions:
  ```go
  result, err := df.Where(func(row map[string]any) bool {
      age, _ := row["Age"].(int64)
      return age > 25 && row["City"] == "NYC"
  }).Result()
  ```

### Summary Statistics

GPandas provides exploratory data analysis helpers over numeric columns:

- **`Describe()`**: Returns a DataFrame of `count`, `mean`, `std` (sample, ddof=1), `min`, `25%`, `50%`, `75%`, `max` per numeric column. Quantiles use linear interpolation and nulls are ignored.
- **Column aggregations**: `Mean()`, `Sum()`, `Std()`, `Median()`, `Min()`, `Max()` each return a `map[string]float64` keyed by numeric column name.
- **`NullCount()`**: Returns a `map[string]int` of null counts per column.
- **`ValueCounts(column)`**: Returns a DataFrame of unique values and their frequencies (descending), excluding nulls.

### Transforming Columns

GPandas supports element-wise and row-wise transformations:

- **`Apply(column, fn)`**: Transform each value of a column with `fn func(any) any` (nulls passed as `nil`). The result column type is inferred from the returned values; mixed integer and floating-point results are promoted to `float64` (pandas-like).
- **`Map(column, mapping)`**: Replace values in a column according to a `map[any]any`; unmapped values are kept unchanged.
- **`ApplyRow(fn)`**: Transform whole rows with `fn func(map[string]any) map[string]any`, useful for deriving new columns. New keys are appended (sorted) after the existing columns.

See `examples/transform/` for a complete working example.

### Handling Missing Data

GPandas provides null-aware cleaning operations:

- **`FillNA(value)`**: Replace nulls across all compatible columns with a constant. Incompatible columns are left unchanged.
- **`FillNAColumn(column, value)`**: Fill nulls in a single column.
- **`FillNAMethod(method)`**: Forward fill (`"ffill"`) or backward fill (`"bfill"`) nulls by propagation.
- **`DropNA(how, subset)`**: Drop rows containing nulls. `how` is `"any"` (default) or `"all"`; `subset` limits the columns considered.
- **`IsNA()` / `NotNA()`**: Return boolean DataFrames indicating null / non-null cells.

### Adding Columns

GPandas supports adding and inserting columns in place:

- **`Assign(name, series)`**: Add a new column (or replace an existing one) from a Series.
- **`AssignFunc(name, fn)`**: Add a column computed from each row with `fn func(map[string]any) any`; the type is inferred.
- **`Insert(loc, name, series)`**: Insert a column at a specific position.

### Unique Values and Deduplication

- **`Unique(column)`**: Distinct values in first-appearance order (includes a single `nil` if the column has nulls).
- **`NUnique(column)`**: Count of distinct non-null values.
- **`Duplicated(subset, keep)`**: Boolean slice marking duplicate rows. `keep` is `"first"` (default), `"last"`, or `"none"`.
- **`DropDuplicates(subset, keep)`**: Return a new DataFrame with duplicate rows removed.

### Type Casting and Introspection

- **`AsType(column, targetType)`**: Convert a column to `FloatCol{}`, `IntCol{}`, `StringCol{}`, or `BoolCol{}` (string aliases like `"float64"` also accepted). Nulls are preserved.
- **`DTypes()`**: Map of column name to data type name.
- **`Info()`**: Human-readable summary of rows, columns, non-null counts, and dtypes.

See `examples/cleaning/` for a complete working example of missing-data handling, deduplication, column mutation, and type casting.

### Aggregation and Window Functions

GPandas supports flexible aggregation and time-series style window operations:

- **`GroupBy(...).Agg(spec)`**: Apply multiple aggregation functions per column at once, e.g. `gb.Agg(map[string][]dataframe.AggFunc{"Salary": {dataframe.AggSum, dataframe.AggMean}})`. Supported functions: `AggSum`, `AggMean`, `AggCount`, `AggMin`, `AggMax`, `AggStd`, `AggMedian`, `AggFirst`, `AggLast`. Result columns are named `<column>_<func>`.
- **`Rolling(window)`**: Moving-window aggregations — `.Mean()`, `.Sum()`, `.Min()`, `.Max()`, `.Std()`. Positions without a full window of non-null values are null.
- **`Shift(periods)`**: Shift values down (positive) or up (negative), filling vacated cells with null.
- **`CumSum()` / `CumMax()` / `CumMin()` / `CumProd()`**: Cumulative operations over numeric columns; nulls are skipped and preserved.

### Reshaping with Stack, Unstack, and MultiIndex

- **`Stack()`**: Reshape wide → long, producing `index`/`variable`/`value` columns (null cells dropped).
- **`Unstack()`**: Inverse of `Stack`, reshaping the long format back to wide.
- **`SetMultiIndex(columns)`**: Build a composite (flattened) index by joining the given columns' values.

### String Methods

String columns expose a vectorized accessor via `df.Str(column)` (or `series.Str()` on a `*StringSeries`):

- `Lower()`, `Upper()`, `Strip()`, `Title()`, `Replace(old, new)` → `*StringSeries`
- `Contains(substr)`, `StartsWith(prefix)`, `EndsWith(suffix)` → `*BoolSeries`
- `Len()` → `*Int64Series`; `Split(sep)` → `[][]string`

Null values are preserved across all string operations. Results can be added back with `Assign`.

See `examples/advanced/` for a complete working example of aggregation, window functions, reshaping, and string methods.

### Statistics, Sampling, and Chaining

- **`Corr()` / `Cov()`**: Pairwise Pearson correlation and sample covariance matrices over numeric columns (returned as a square DataFrame indexed by column name).
- **`Sample(n, seed...)`**: Randomly select `n` rows without replacement; an optional seed makes the selection deterministic.
- **`Pipe(fn)`**: Apply a custom `func(*DataFrame) (*DataFrame, error)` for fluent method chaining.

### DateTime and Categorical Types

- **`ToDatetime(column, layout)`**: Parse a string column into a datetime column (auto-detects common layouts when `layout` is empty). Then `df.Dt(column)` exposes `Year()`, `Month()`, `Day()`, `Hour()`, `Weekday()`, `Date()`, and more.
- **`AsCategorical(column)`**: Convert a column to a memory-efficient categorical type backed by integer codes; `Categories(column)` lists the distinct categories.

### Multi-key Merge

- **`MergeOn(other, on, how)`**: Join two DataFrames on multiple key columns (inner, left, right, full), generalizing `Merge`.

### Additional Visualizations

- **`PlotScatter(xCol, yCol, opts)`**: Scatter chart from two numeric columns.
- **`PlotHistogram(column, bins, opts)`**: Histogram of a numeric column.
- **`PlotHeatmap(opts)`**: Heatmap of numeric columns (pairs well with `Corr()`).

See `examples/analytics/` for a complete working example of correlation, sampling, datetime, categorical, multi-key merge, Parquet, and the new charts.

### Data Loading from External Sources

- **CSV Reading**: Efficiently read CSV files into DataFrames with `gpandas.Read_csv()`, leveraging concurrent processing for performance.
- **JSON I/O**: Read records-oriented JSON with `gpandas.Read_json()` and export with `DataFrame.ToJSON()`.
- **Excel I/O**: Read `.xlsx` files with `gpandas.Read_excel()` and export with `DataFrame.ToExcel()` (powered by [excelize](https://github.com/xuri/excelize)).
- **Parquet I/O**: Read `.parquet` files with `gpandas.Read_parquet()` and export with `DataFrame.ToParquet()` (powered by [parquet-go](https://github.com/parquet-go/parquet-go)). Note: columns are written as non-nullable, so nulls are stored as zero values.
- **SQL Database Integration**:
    - **`Read_sql()`**: Query and load data from SQL databases (SQL Server, PostgreSQL, and others supported by Go database/sql package) into DataFrames.
- **Google BigQuery Support**:
    - **`From_gbq()`**: Query and load data from Google BigQuery tables into DataFrames, enabling analysis of large datasets stored in BigQuery.

### Data Visualization

GPandas integrates with [go-echarts v2](https://github.com/go-echarts/go-echarts) to provide interactive HTML chart generation directly from DataFrames:

- **Bar Charts**: Create bar charts with `DataFrame.PlotBar()` for categorical data visualization
- **Pie Charts**: Generate pie charts with `DataFrame.PlotPie()` for proportional data representation
- **Line Charts**: Plot line charts with `DataFrame.PlotLine()` for time series and trend analysis
  - Supports single and multi-series line charts for comparing multiple data series

**Key Features**:
- Interactive HTML output viewable in any web browser
- Customizable chart options (title, width, height, theme)
- Automatic null value handling
- Thread-safe concurrent plotting
- Type-safe data conversion

**Example**:
```go
import (
    "github.com/apoplexi24/gpandas/dataframe"
    "github.com/apoplexi24/gpandas/plot"
    "github.com/apoplexi24/gpandas/utils/collection"
)

// Create DataFrame
categories, _ := collection.NewStringSeriesFromData([]string{"A", "B", "C"}, nil)
values, _ := collection.NewFloat64SeriesFromData([]float64{10.0, 20.0, 30.0}, nil)

df := &dataframe.DataFrame{
    Columns: map[string]collection.Series{
        "category": categories,
        "value":    values,
    },
    ColumnOrder: []string{"category", "value"},
    Index:       []string{"0", "1", "2"},
}

// Generate bar chart
opts := &plot.ChartOptions{
    Title:      "Sample Bar Chart",
    Width:      900,
    Height:     500,
    OutputPath: "output/chart.html",
}
df.PlotBar("category", "value", opts)
```

See `examples/plot/` for complete working examples of all chart types.

**Dependencies**: Requires `github.com/go-echarts/go-echarts/v2` - automatically installed via `go get`.

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

## Troubleshooting

### Plotting Issues

**Problem**: `output path is required in ChartOptions`
- **Solution**: Always provide an `OutputPath` in `ChartOptions`. This field is required for all plotting methods.
  ```go
  opts := &plot.ChartOptions{
      OutputPath: "output/chart.html",  // Required
  }
  ```

**Problem**: `column 'X' not found in DataFrame`
- **Solution**: Verify that the column name exists in your DataFrame using `df.ColumnOrder` or by printing the DataFrame with `df.String()`.

**Problem**: `column 'X' has type string, expected numeric type`
- **Solution**: Ensure y-axis columns for bar/line charts and value columns for pie charts contain numeric data (int64 or float64). Use string columns only for labels and x-axis categories.

**Problem**: Charts display incorrectly or show no data
- **Solution**: Check for null values in your data. While GPandas handles nulls by skipping them, too many nulls may result in sparse charts. Use Series methods to inspect null counts.

**Problem**: File write errors when generating charts
- **Solution**: Ensure the output directory exists and you have write permissions. Create the directory first if needed:
  ```go
  os.MkdirAll("output", 0755)
  ```

### General Issues

**Problem**: Type mismatch errors when creating Series
- **Solution**: Use the appropriate Series constructor for your data type:
  - `NewStringSeriesFromData()` for strings
  - `NewFloat64SeriesFromData()` for float64
  - `NewInt64SeriesFromData()` for int64
  - `NewBoolSeriesFromData()` for booleans

**Problem**: DataFrame operations fail with nil pointer errors
- **Solution**: Always check if DataFrame is nil before performing operations, especially after operations that may return nil on error.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by Python's pandas library, aiming to bring similar data manipulation capabilities to the Go ecosystem.
- Built using Go's powerful generic system for type safety and performance.
- Thanks to the Go community for valuable feedback and contributions.

## Status

GPandas is under active development and is suitable for production use. However, it's still evolving, with ongoing efforts to add more features, enhance performance, and improve API ergonomics. Expect continued updates and improvements.

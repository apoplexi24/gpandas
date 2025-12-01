package gpandas

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"sync"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

type GoPandas struct{}

// FloatColumn represents a slice of float64 values.
type FloatCol []float64

// StringColumn represents a slice of string values.
type StringCol []string

// IntColumn represents a slice of int64 values.
type IntCol []int64

// BoolColumn represents a slice of bool values.
type BoolCol []bool

// Column represents a slice of any type.
type Column []any

// TypeColumn represents a slice of a comparable type T.
type TypeColumn[T comparable] []T

func FloatColumn(col []any) ([]float64, error) {
	floatCol := make(FloatCol, len(col))
	for i, v := range col {
		if val, ok := v.(float64); ok {
			floatCol[i] = val
		} else {
			return nil, fmt.Errorf("invalid type for column %d: expected float64, got %T", i, v)
		}
	}
	return floatCol, nil
}

// DataFrame creates a new DataFrame from the provided columns, data, and column types.
//
// It validates the input parameters to ensure data consistency and proper type definitions.
//
// The function performs several validation checks:
// - Ensures column_types map is provided
// - Verifies at least one column name is present
// - Checks that data is not empty
// - Confirms the number of columns matches the data columns
// - Validates all columns have the same length
// - Ensures type definitions exist for all columns
//
// The data is then converted to the internal DataFrame format, creating typed Series
// based on the specified column types (FloatCol, IntCol, StringCol, BoolCol).
// Null values (nil) are properly tracked using the boolean mask approach.
//
// Parameters:
//
//	columns: A slice of strings representing column names
//	data: A slice of Columns containing the actual data
//	columns_types: A map defining the expected type for each column
//
// Returns:
//
//	A pointer to a DataFrame containing the processed data, or an error if validation fails
func (GoPandas) DataFrame(columns []string, data []Column, columns_types map[string]any) (*dataframe.DataFrame, error) {
	// Validate inputs
	if columns_types == nil {
		return nil, errors.New("columns_types map is required to assert column types")
	}

	if len(columns) == 0 {
		return nil, errors.New("at least one column name is required")
	}

	if len(data) == 0 {
		return nil, errors.New("data cannot be empty")
	}

	if len(columns) != len(data) {
		return nil, errors.New("number of columns must match number of data columns")
	}

	// Validate all columns have same length
	rowCount := len(data[0])
	for i, col := range data {
		if len(col) != rowCount {
			return nil, fmt.Errorf("inconsistent row count in column %s: expected %d, got %d", columns[i], rowCount, len(col))
		}
	}

	// Validate column types
	for _, colName := range columns {
		if _, exists := columns_types[colName]; !exists {
			return nil, fmt.Errorf("missing type definition for column: %s", colName)
		}
	}

	// Create columnar DataFrame with typed Series
	cols := make(map[string]collection.Series, len(columns))
	for i, colName := range columns {
		var series collection.Series
		var err error

		switch columns_types[colName].(type) {
		case FloatCol:
			// Create Float64Series
			floatData := make([]float64, rowCount)
			mask := make([]bool, rowCount)
			for j := 0; j < rowCount; j++ {
				if data[i][j] == nil {
					mask[j] = true
				} else if v, ok := data[i][j].(float64); ok {
					floatData[j] = v
				} else {
					return nil, fmt.Errorf("type mismatch in column %s at row %d: expected float64, got %T", colName, j, data[i][j])
				}
			}
			series, err = collection.NewFloat64SeriesFromData(floatData, mask)

		case IntCol:
			// Create Int64Series
			intData := make([]int64, rowCount)
			mask := make([]bool, rowCount)
			for j := 0; j < rowCount; j++ {
				if data[i][j] == nil {
					mask[j] = true
				} else if v, ok := data[i][j].(int64); ok {
					intData[j] = v
				} else {
					return nil, fmt.Errorf("type mismatch in column %s at row %d: expected int64, got %T", colName, j, data[i][j])
				}
			}
			series, err = collection.NewInt64SeriesFromData(intData, mask)

		case StringCol:
			// Create StringSeries
			strData := make([]string, rowCount)
			mask := make([]bool, rowCount)
			for j := 0; j < rowCount; j++ {
				if data[i][j] == nil {
					mask[j] = true
				} else if v, ok := data[i][j].(string); ok {
					strData[j] = v
				} else {
					return nil, fmt.Errorf("type mismatch in column %s at row %d: expected string, got %T", colName, j, data[i][j])
				}
			}
			series, err = collection.NewStringSeriesFromData(strData, mask)

		case BoolCol:
			// Create BoolSeries
			boolData := make([]bool, rowCount)
			mask := make([]bool, rowCount)
			for j := 0; j < rowCount; j++ {
				if data[i][j] == nil {
					mask[j] = true
				} else if v, ok := data[i][j].(bool); ok {
					boolData[j] = v
				} else {
					return nil, fmt.Errorf("type mismatch in column %s at row %d: expected bool, got %T", colName, j, data[i][j])
				}
			}
			series, err = collection.NewBoolSeriesFromData(boolData, mask)

		default:
			// Fallback to AnySeries for unknown types
			values := make([]any, rowCount)
			for j := 0; j < rowCount; j++ {
				values[j] = data[i][j]
			}
			series, err = collection.NewAnySeriesFromData(values, nil)
		}

		if err != nil {
			return nil, fmt.Errorf("failed creating series for column %s: %w", colName, err)
		}
		cols[colName] = series
	}

	// Create default index
	index := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &dataframe.DataFrame{Columns: cols, ColumnOrder: append([]string(nil), columns...), Index: index}, nil
}

// Read_csv reads a CSV file from the specified filepath and converts it into a DataFrame.
//
// It opens the CSV file, reads the header to determine the column names, and then reads all the records.
//
// The function checks for errors during file operations and ensures that the CSV file is not empty.
//
// It initializes data columns based on the number of headers and populates them with the corresponding values from the records.
//
// If the number of columns in any row is inconsistent with the header, that row is skipped.
//
// All values are stored as strings in StringSeries with proper null handling.
//
// Parameters:
//
//	filepath: A string representing the path to the CSV file to be read.
//
// Returns:
//
//	A pointer to a DataFrame containing the data from the CSV file, or an error if the operation fails.
func (GoPandas) Read_csv(filepath string) (*dataframe.DataFrame, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	// Read header
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading headers: %w", err)
	}

	columnCount := len(headers)
	if columnCount == 0 {
		return nil, errors.New("no headers found in CSV")
	}

	// Use a worker pool for dynamic workload distribution
	type RowData struct {
		Index int
		Row   []string
	}
	rowChan := make(chan RowData, 100)                 // Buffered channel to hold rows
	resultChan := make(chan [][]string, runtime.NumCPU()) // Channel to hold columnar string data
	var wg sync.WaitGroup

	// Start workers for processing rows
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Local column buffers
			localData := make([][]string, columnCount)
			for i := range localData {
				localData[i] = make([]string, 0, 100) // Preallocate some space
			}

			for row := range rowChan {
				if len(row.Row) != columnCount {
					// Handle inconsistent row lengths
					continue
				}
				for j, val := range row.Row {
					localData[j] = append(localData[j], val)
				}
			}
			resultChan <- localData
		}()
	}

	// Feed rows to workers
	go func() {
		index := 0
		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				close(rowChan)
				return
			}
			rowChan <- RowData{Index: index, Row: record}
			index++
		}
		close(rowChan)
	}()

	// Wait for workers to finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Combine results into columnar format
	combinedData := make([][]string, columnCount)
	for i := range combinedData {
		combinedData[i] = make([]string, 0)
	}

	for localData := range resultChan {
		for i := range localData {
			combinedData[i] = append(combinedData[i], localData[i]...)
		}
	}

	// Build StringSeries per column
	cols := make(map[string]collection.Series, columnCount)
	for i, header := range headers {
		// Create StringSeries from string data (no nulls from CSV - empty strings are valid)
		series, err := collection.NewStringSeriesFromData(combinedData[i], nil)
		if err != nil {
			return nil, fmt.Errorf("failed creating series for column %s: %w", header, err)
		}
		cols[header] = series
	}

	// Create default index based on row count
	rowCount := 0
	if len(headers) > 0 && cols[headers[0]] != nil {
		rowCount = cols[headers[0]].Len()
	}
	index := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &dataframe.DataFrame{Columns: cols, ColumnOrder: append([]string(nil), headers...), Index: index}, nil
}

// Read_csv_typed reads a CSV file and creates typed Series based on the provided column types.
//
// This is similar to Read_csv but allows specifying column types for automatic type conversion.
// Empty strings in the CSV are treated as null values for non-string types.
//
// Parameters:
//
//	filepath: A string representing the path to the CSV file to be read.
//	columnTypes: A map defining the expected type for each column (FloatCol, IntCol, StringCol, BoolCol)
//
// Returns:
//
//	A pointer to a DataFrame containing the typed data from the CSV file, or an error if the operation fails.
func (gp GoPandas) Read_csv_typed(filepath string, columnTypes map[string]any) (*dataframe.DataFrame, error) {
	// First read as strings
	df, err := gp.Read_csv(filepath)
	if err != nil {
		return nil, err
	}

	// Convert columns to specified types
	for colName, colType := range columnTypes {
		series, ok := df.Columns[colName]
		if !ok {
			continue // Skip columns not in DataFrame
		}

		strSeries, ok := series.(*collection.StringSeries)
		if !ok {
			continue // Skip if not a StringSeries
		}

		rowCount := strSeries.Len()

		switch colType.(type) {
		case FloatCol:
			floatData := make([]float64, rowCount)
			mask := make([]bool, rowCount)
			for i := 0; i < rowCount; i++ {
				if strSeries.IsNull(i) {
					mask[i] = true
					continue
				}
				strVal, _ := strSeries.StringValue(i)
				if strVal == "" {
					mask[i] = true
					continue
				}
				var f float64
				_, err := fmt.Sscanf(strVal, "%f", &f)
				if err != nil {
					mask[i] = true
				} else {
					floatData[i] = f
				}
			}
			newSeries, err := collection.NewFloat64SeriesFromData(floatData, mask)
			if err != nil {
				return nil, fmt.Errorf("failed converting column %s to float64: %w", colName, err)
			}
			df.Columns[colName] = newSeries

		case IntCol:
			intData := make([]int64, rowCount)
			mask := make([]bool, rowCount)
			for i := 0; i < rowCount; i++ {
				if strSeries.IsNull(i) {
					mask[i] = true
					continue
				}
				strVal, _ := strSeries.StringValue(i)
				if strVal == "" {
					mask[i] = true
					continue
				}
				var n int64
				_, err := fmt.Sscanf(strVal, "%d", &n)
				if err != nil {
					mask[i] = true
				} else {
					intData[i] = n
				}
			}
			newSeries, err := collection.NewInt64SeriesFromData(intData, mask)
			if err != nil {
				return nil, fmt.Errorf("failed converting column %s to int64: %w", colName, err)
			}
			df.Columns[colName] = newSeries

		case BoolCol:
			boolData := make([]bool, rowCount)
			mask := make([]bool, rowCount)
			for i := 0; i < rowCount; i++ {
				if strSeries.IsNull(i) {
					mask[i] = true
					continue
				}
				strVal, _ := strSeries.StringValue(i)
				if strVal == "" {
					mask[i] = true
					continue
				}
				switch strVal {
				case "true", "True", "TRUE", "1":
					boolData[i] = true
				case "false", "False", "FALSE", "0":
					boolData[i] = false
				default:
					mask[i] = true
				}
			}
			newSeries, err := collection.NewBoolSeriesFromData(boolData, mask)
			if err != nil {
				return nil, fmt.Errorf("failed converting column %s to bool: %w", colName, err)
			}
			df.Columns[colName] = newSeries

		case StringCol:
			// Already a StringSeries, no conversion needed
		}
	}

	return df, nil
}

// NewDataFrameFromSeries creates a DataFrame from a map of Series.
//
// Parameters:
//
//	columns: A map of column names to Series
//	columnOrder: Optional slice specifying column order (uses map order if nil)
//
// Returns:
//
//	A pointer to a DataFrame, or an error if validation fails
func NewDataFrameFromSeries(columns map[string]collection.Series, columnOrder []string) (*dataframe.DataFrame, error) {
	if len(columns) == 0 {
		return nil, errors.New("at least one column is required")
	}

	// Determine column order
	if columnOrder == nil {
		columnOrder = make([]string, 0, len(columns))
		for name := range columns {
			columnOrder = append(columnOrder, name)
		}
	}

	// Validate all column names exist
	for _, name := range columnOrder {
		if _, ok := columns[name]; !ok {
			return nil, fmt.Errorf("column '%s' not found in columns map", name)
		}
	}

	// Validate all columns have same length
	var rowCount int
	for i, name := range columnOrder {
		if i == 0 {
			rowCount = columns[name].Len()
		} else if columns[name].Len() != rowCount {
			return nil, fmt.Errorf("inconsistent row count: column '%s' has %d rows, expected %d", name, columns[name].Len(), rowCount)
		}
	}

	// Create default index
	index := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &dataframe.DataFrame{
		Columns:     columns,
		ColumnOrder: columnOrder,
		Index:       index,
	}, nil
}

// NewEmptyDataFrame creates an empty DataFrame with specified column names and types.
//
// Parameters:
//
//	columns: A slice of column names
//	columnTypes: A map of column names to their types (uses reflect.Type)
//
// Returns:
//
//	A pointer to an empty DataFrame with the specified structure
func NewEmptyDataFrame(columns []string, columnTypes map[string]reflect.Type) *dataframe.DataFrame {
	cols := make(map[string]collection.Series, len(columns))
	for _, name := range columns {
		dtype, ok := columnTypes[name]
		if !ok {
			cols[name] = collection.NewAnySeries(0)
		} else {
			cols[name] = collection.NewSeriesOfType(dtype, 0)
		}
	}

	return &dataframe.DataFrame{
		Columns:     cols,
		ColumnOrder: append([]string(nil), columns...),
		Index:       []string{},
	}
}

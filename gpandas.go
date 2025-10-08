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
// The data is then converted to the internal DataFrame format, performing type assertions
// based on the specified column types (FloatCol, IntCol, StringCol, BoolCol).
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

	// Create columnar DataFrame
	cols := make(map[string]*collection.Series, len(columns))
	for i, colName := range columns {
		// Build series with dtype enforcement based on columns_types
		var series *collection.Series
		// Prepare values slice
		values := make([]any, rowCount)
		for j := 0; j < rowCount; j++ {
			values[j] = data[i][j]
		}
		var err error
		switch columns_types[colName].(type) {
		case FloatCol:
			series, err = collection.NewSeriesWithData(reflect.TypeOf(float64(0)), values)
		case IntCol:
			series, err = collection.NewSeriesWithData(reflect.TypeOf(int64(0)), values)
		case StringCol:
			series, err = collection.NewSeriesWithData(reflect.TypeOf(""), values)
		case BoolCol:
			series, err = collection.NewSeriesWithData(reflect.TypeOf(true), values)
		default:
			series, err = collection.NewSeriesWithData(nil, values)
		}
		if err != nil {
			return nil, fmt.Errorf("failed creating series for column %s: %w", colName, err)
		}
		cols[colName] = series
	}

	return &dataframe.DataFrame{Columns: cols, ColumnOrder: append([]string(nil), columns...)}, nil
}

// Read_csv reads a CSV file from the specified filepath and converts it into a DataFrame.
//
// It opens the CSV file, reads the header to determine the column names, and then reads all the records.
//
// The function checks for errors during file operations and ensures that the CSV file is not empty.
//
// It initializes data columns based on the number of headers and populates them with the corresponding values from the records.
//
// If the number of columns in any row is inconsistent with the header, an error is returned.
//
// The function also creates a map of column types, defaulting to StringCol for all columns.
//
// Finally, it calls the DataFrame constructor to create and return a DataFrame containing the data from the CSV file.
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
	resultChan := make(chan [][]any, runtime.NumCPU()) // Channel to hold columnar data
	var wg sync.WaitGroup

	// Start workers for processing rows
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Local column buffers
			localData := make([][]any, columnCount)
			for i := range localData {
				localData[i] = make([]any, 0, 100) // Preallocate some space
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
	combinedData := make([][]any, columnCount)
	for i := range combinedData {
		combinedData[i] = make([]any, 0)
	}

	for localData := range resultChan {
		for i := range localData {
			combinedData[i] = append(combinedData[i], localData[i]...)
		}
	}

	// Infer column types (default to string for now)
	columnTypes := make(map[string]any, columnCount)
	for _, header := range headers {
		columnTypes[header] = StringCol{}
	}

	// Build Series per column
	cols := make(map[string]*collection.Series, columnCount)
	for i, header := range headers {
		series, err := collection.NewSeriesWithData(nil, combinedData[i])
		if err != nil {
			return nil, fmt.Errorf("failed creating series for column %s: %w", header, err)
		}
		cols[header] = series
	}

	return &dataframe.DataFrame{Columns: cols, ColumnOrder: append([]string(nil), headers...)}, nil
}

package dataframe

import (
	"fmt"
	"sort"
	"strings"

	"github.com/apoplexi24/gpandas/utils/collection"
)

// AggFunc represents the aggregation function to use in pivot table operations.
type AggFunc string

const (
	// AggSum computes the sum of values.
	AggSum AggFunc = "sum"
	// AggMean computes the mean of values.
	AggMean AggFunc = "mean"
	// AggCount counts non-null values.
	AggCount AggFunc = "count"
	// AggMin computes the minimum value.
	AggMin AggFunc = "min"
	// AggMax computes the maximum value.
	AggMax AggFunc = "max"
)

// PivotTableOptions configures the behavior of the PivotTable function.
type PivotTableOptions struct {
	// Index specifies the column(s) to use as row labels for the pivot table.
	// These columns will be used to group rows.
	Index []string

	// Columns specifies the column whose unique values will become new column headers.
	Columns string

	// Values specifies the column(s) to aggregate.
	Values []string

	// AggFunc specifies the aggregation function to apply.
	// Supported values: "sum", "mean", "count", "min", "max".
	// Default: "mean"
	AggFunc AggFunc

	// FillValue is the value to use for missing combinations.
	// If nil, missing values will remain null.
	FillValue any
}

// PivotTable creates a spreadsheet-style pivot table as a DataFrame.
//
// The pivot table aggregates data based on the specified index and columns,
// applying the aggregation function to the values.
//
// Parameters:
//   - opts: PivotTableOptions configuring the pivot operation
//
// Returns:
//   - *DataFrame: the pivot table result
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	df := &DataFrame{...}  // DataFrame with columns: "A", "B", "C", "D"
//	pivot, err := df.PivotTable(dataframe.PivotTableOptions{
//	    Index:   []string{"A"},
//	    Columns: "B",
//	    Values:  []string{"C"},
//	    AggFunc: dataframe.AggSum,
//	})
func (df *DataFrame) PivotTable(opts PivotTableOptions) (*DataFrame, error) {
	if df == nil {
		return nil, fmt.Errorf("DataFrame is nil")
	}

	// Validate options
	if len(opts.Index) == 0 {
		return nil, fmt.Errorf("Index column(s) must be specified")
	}
	if opts.Columns == "" {
		return nil, fmt.Errorf("Columns parameter must be specified")
	}
	if len(opts.Values) == 0 {
		return nil, fmt.Errorf("Values column(s) must be specified")
	}

	// Default aggregation function
	if opts.AggFunc == "" {
		opts.AggFunc = AggMean
	}

	df.RLock()
	defer df.RUnlock()

	// Validate that all required columns exist
	for _, col := range opts.Index {
		if _, ok := df.Columns[col]; !ok {
			return nil, fmt.Errorf("index column '%s' not found", col)
		}
	}
	if _, ok := df.Columns[opts.Columns]; !ok {
		return nil, fmt.Errorf("columns column '%s' not found", opts.Columns)
	}
	for _, col := range opts.Values {
		if _, ok := df.Columns[col]; !ok {
			return nil, fmt.Errorf("values column '%s' not found", col)
		}
	}

	numRows := df.Len()

	// Collect unique values for columns (to create new column headers)
	columnValues := make(map[string]bool)
	for i := 0; i < numRows; i++ {
		if !df.Columns[opts.Columns].IsNull(i) {
			val, _ := df.Columns[opts.Columns].At(i)
			columnValues[fmt.Sprintf("%v", val)] = true
		}
	}

	// Sort column values for deterministic output
	sortedColumnValues := make([]string, 0, len(columnValues))
	for v := range columnValues {
		sortedColumnValues = append(sortedColumnValues, v)
	}
	sort.Strings(sortedColumnValues)

	// Collect unique index combinations
	indexKeys := make(map[string][]string) // key -> original index values
	for i := 0; i < numRows; i++ {
		keyParts := make([]string, len(opts.Index))
		for j, col := range opts.Index {
			val, _ := df.Columns[col].At(i)
			keyParts[j] = fmt.Sprintf("%v", val)
		}
		key := strings.Join(keyParts, "\x00")
		if _, exists := indexKeys[key]; !exists {
			indexKeys[key] = keyParts
		}
	}

	// Sort index keys for deterministic output
	sortedIndexKeys := make([]string, 0, len(indexKeys))
	for k := range indexKeys {
		sortedIndexKeys = append(sortedIndexKeys, k)
	}
	sort.Strings(sortedIndexKeys)

	// Build aggregation data structure
	// Map: indexKey -> columnValue -> valueColumn -> []values
	aggData := make(map[string]map[string]map[string][]float64)
	for i := 0; i < numRows; i++ {
		// Build index key
		keyParts := make([]string, len(opts.Index))
		for j, col := range opts.Index {
			val, _ := df.Columns[col].At(i)
			keyParts[j] = fmt.Sprintf("%v", val)
		}
		indexKey := strings.Join(keyParts, "\x00")

		// Get column value
		if df.Columns[opts.Columns].IsNull(i) {
			continue
		}
		colVal, _ := df.Columns[opts.Columns].At(i)
		colValStr := fmt.Sprintf("%v", colVal)

		// Initialize maps if needed
		if aggData[indexKey] == nil {
			aggData[indexKey] = make(map[string]map[string][]float64)
		}
		if aggData[indexKey][colValStr] == nil {
			aggData[indexKey][colValStr] = make(map[string][]float64)
		}

		// Collect values for each value column
		for _, valCol := range opts.Values {
			if !df.Columns[valCol].IsNull(i) {
				val, _ := df.Columns[valCol].At(i)
				floatVal, ok := toFloat64(val)
				if ok {
					aggData[indexKey][colValStr][valCol] = append(aggData[indexKey][colValStr][valCol], floatVal)
				}
			}
		}
	}

	// Build result DataFrame
	numResultRows := len(sortedIndexKeys)

	// Create index columns
	resultCols := make(map[string]collection.Series)
	for _, col := range opts.Index {
		resultCols[col], _ = collection.NewStringSeriesFromData(make([]string, numResultRows), nil)
	}

	// Create value columns (for each combination of value column and column value)
	resultOrder := make([]string, 0, len(opts.Index))
	resultOrder = append(resultOrder, opts.Index...)

	for _, valCol := range opts.Values {
		for _, colVal := range sortedColumnValues {
			var colName string
			if len(opts.Values) == 1 {
				colName = colVal
			} else {
				colName = fmt.Sprintf("%s_%s", valCol, colVal)
			}
			resultCols[colName], _ = collection.NewFloat64SeriesFromData(make([]float64, numResultRows), nil)
			resultOrder = append(resultOrder, colName)
		}
	}

	// Fill in the data
	for rowIdx, indexKey := range sortedIndexKeys {
		// Set index column values
		indexVals := indexKeys[indexKey]
		for colIdx, col := range opts.Index {
			resultCols[col].Set(rowIdx, indexVals[colIdx])
		}

		// Set aggregated values
		for _, valCol := range opts.Values {
			for _, colVal := range sortedColumnValues {
				var colName string
				if len(opts.Values) == 1 {
					colName = colVal
				} else {
					colName = fmt.Sprintf("%s_%s", valCol, colVal)
				}

				values := aggData[indexKey][colVal][valCol]
				if len(values) == 0 {
					if opts.FillValue != nil {
						resultCols[colName].Set(rowIdx, opts.FillValue)
					} else {
						resultCols[colName].SetNull(rowIdx)
					}
				} else {
					aggResult := aggregate(values, opts.AggFunc)
					resultCols[colName].Set(rowIdx, aggResult)
				}
			}
		}
	}

	// Create index
	resultIndex := make([]string, numResultRows)
	for i := 0; i < numResultRows; i++ {
		resultIndex[i] = fmt.Sprintf("%d", i)
	}

	return &DataFrame{
		Columns:     resultCols,
		ColumnOrder: resultOrder,
		Index:       resultIndex,
	}, nil
}

// aggregate applies the aggregation function to a slice of values.
func aggregate(values []float64, aggFunc AggFunc) float64 {
	if len(values) == 0 {
		return 0
	}

	switch aggFunc {
	case AggSum:
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum

	case AggMean:
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))

	case AggCount:
		return float64(len(values))

	case AggMin:
		min := values[0]
		for _, v := range values[1:] {
			if v < min {
				min = v
			}
		}
		return min

	case AggMax:
		max := values[0]
		for _, v := range values[1:] {
			if v > max {
				max = v
			}
		}
		return max

	default:
		// Default to mean
		sum := 0.0
		for _, v := range values {
			sum += v
		}
		return sum / float64(len(values))
	}
}

// toFloat64 attempts to convert a value to float64.
func toFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	case int16:
		return float64(v), true
	case int8:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint64:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint8:
		return float64(v), true
	default:
		return 0, false
	}
}

// MeltOptions configures the behavior of the Melt function.
type MeltOptions struct {
	// IdVars specifies the column(s) to use as identifier variables.
	// These columns will be kept as-is in the output.
	IdVars []string

	// ValueVars specifies the column(s) to unpivot.
	// If empty, all columns not in IdVars will be used.
	ValueVars []string

	// VarName is the name for the variable column.
	// Default: "variable"
	VarName string

	// ValueName is the name for the value column.
	// Default: "value"
	ValueName string
}

// Melt unpivots a DataFrame from wide to long format.
//
// This operation transforms columns into rows, keeping identifier variables
// fixed while "melting" the specified value columns.
//
// Parameters:
//   - opts: MeltOptions configuring the melt operation
//
// Returns:
//   - *DataFrame: the melted DataFrame in long format
//   - error: nil if successful, otherwise an error
//
// Example:
//
//	// Wide format DataFrame:
//	//   Name  | Math | Science
//	//   Alice | 90   | 85
//	//   Bob   | 80   | 75
//	//
//	melted, err := df.Melt(dataframe.MeltOptions{
//	    IdVars:    []string{"Name"},
//	    ValueVars: []string{"Math", "Science"},
//	    VarName:   "Subject",
//	    ValueName: "Score",
//	})
//	// Result (long format):
//	//   Name  | Subject | Score
//	//   Alice | Math    | 90
//	//   Alice | Science | 85
//	//   Bob   | Math    | 80
//	//   Bob   | Science | 75
func (df *DataFrame) Melt(opts MeltOptions) (*DataFrame, error) {
	if df == nil {
		return nil, fmt.Errorf("DataFrame is nil")
	}

	// Set defaults
	if opts.VarName == "" {
		opts.VarName = "variable"
	}
	if opts.ValueName == "" {
		opts.ValueName = "value"
	}

	df.RLock()
	defer df.RUnlock()

	// Validate IdVars
	idVarsSet := make(map[string]bool)
	for _, col := range opts.IdVars {
		if _, ok := df.Columns[col]; !ok {
			return nil, fmt.Errorf("id_vars column '%s' not found", col)
		}
		idVarsSet[col] = true
	}

	// Determine ValueVars
	var valueVars []string
	if len(opts.ValueVars) == 0 {
		// Use all columns not in IdVars
		for _, col := range df.ColumnOrder {
			if !idVarsSet[col] {
				valueVars = append(valueVars, col)
			}
		}
	} else {
		// Validate provided ValueVars
		for _, col := range opts.ValueVars {
			if _, ok := df.Columns[col]; !ok {
				return nil, fmt.Errorf("value_vars column '%s' not found", col)
			}
			valueVars = append(valueVars, col)
		}
	}

	if len(valueVars) == 0 {
		return nil, fmt.Errorf("no columns to melt")
	}

	numRows := df.Len()
	numValueVars := len(valueVars)
	resultRows := numRows * numValueVars

	// Create result series
	resultCols := make(map[string]collection.Series)
	resultOrder := make([]string, 0, len(opts.IdVars)+2)

	// ID columns (will be repeated for each value var)
	for _, col := range opts.IdVars {
		// Determine type from source column
		srcSeries := df.Columns[col]
		resultCols[col] = collection.NewSeriesOfTypeWithSize(srcSeries.DType(), resultRows)
		resultOrder = append(resultOrder, col)
	}

	// Variable column (string type)
	resultCols[opts.VarName], _ = collection.NewStringSeriesFromData(make([]string, resultRows), nil)
	resultOrder = append(resultOrder, opts.VarName)

	// Value column (use AnySeries to handle mixed types)
	resultCols[opts.ValueName] = collection.NewAnySeries(resultRows)
	resultOrder = append(resultOrder, opts.ValueName)

	// Fill in the data
	resultIdx := 0
	for i := 0; i < numRows; i++ {
		for _, valCol := range valueVars {
			// Copy ID values
			for _, idCol := range opts.IdVars {
				srcSeries := df.Columns[idCol]
				if srcSeries.IsNull(i) {
					resultCols[idCol].SetNull(resultIdx)
				} else {
					val, _ := srcSeries.At(i)
					resultCols[idCol].Set(resultIdx, val)
				}
			}

			// Set variable name
			resultCols[opts.VarName].Set(resultIdx, valCol)

			// Set value
			srcSeries := df.Columns[valCol]
			if srcSeries.IsNull(i) {
				resultCols[opts.ValueName].AppendNull()
			} else {
				val, _ := srcSeries.At(i)
				resultCols[opts.ValueName].Append(val)
			}

			resultIdx++
		}
	}

	// Create index
	resultIndex := make([]string, resultRows)
	for i := 0; i < resultRows; i++ {
		resultIndex[i] = fmt.Sprintf("%d", i)
	}

	return &DataFrame{
		Columns:     resultCols,
		ColumnOrder: resultOrder,
		Index:       resultIndex,
	}, nil
}

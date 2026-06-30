package gpandas

import (
	"fmt"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
	"github.com/xuri/excelize/v2"
)

// Read_excel reads an Excel (.xlsx) file into a DataFrame. The first row is
// treated as the header, and all remaining cells are loaded as strings (use
// DataFrame.AsType to convert columns afterwards, as with Read_csv).
//
// Parameters:
//
//	filepath: path to the .xlsx file.
//	sheet: optional sheet name. If omitted, the first sheet is used.
//
// Returns:
//
//	A pointer to a DataFrame, or an error if the file cannot be read.
//
// Example:
//
//	df, err := gp.Read_excel("data.xlsx")
//	df, err := gp.Read_excel("data.xlsx", "Sheet2")
func (GoPandas) Read_excel(filepath string, sheet ...string) (*dataframe.DataFrame, error) {
	f, err := excelize.OpenFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening Excel file: %w", err)
	}
	defer f.Close()

	sheetName := ""
	if len(sheet) > 0 && sheet[0] != "" {
		sheetName = sheet[0]
	} else {
		sheetName = f.GetSheetName(0)
		if sheetName == "" {
			return nil, fmt.Errorf("no sheets found in Excel file")
		}
	}

	rows, err := f.GetRows(sheetName)
	if err != nil {
		return nil, fmt.Errorf("error reading sheet '%s': %w", sheetName, err)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("sheet '%s' is empty", sheetName)
	}

	headers := rows[0]
	columnCount := len(headers)
	if columnCount == 0 {
		return nil, fmt.Errorf("no headers found in sheet '%s'", sheetName)
	}

	dataRows := rows[1:]
	rowCount := len(dataRows)

	// Build per-column string slices, padding short rows with empty strings.
	cols := make(map[string]collection.Series, columnCount)
	for c, header := range headers {
		colData := make([]string, rowCount)
		for r := 0; r < rowCount; r++ {
			if c < len(dataRows[r]) {
				colData[r] = dataRows[r][c]
			} else {
				colData[r] = ""
			}
		}
		series, err := collection.NewStringSeriesFromData(colData, nil)
		if err != nil {
			return nil, fmt.Errorf("failed creating series for column '%s': %w", header, err)
		}
		cols[header] = series
	}

	index := make([]string, rowCount)
	for i := 0; i < rowCount; i++ {
		index[i] = fmt.Sprintf("%d", i)
	}

	return &dataframe.DataFrame{
		Columns:     cols,
		ColumnOrder: append([]string(nil), headers...),
		Index:       index,
	}, nil
}

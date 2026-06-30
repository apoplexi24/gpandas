package dataframe

import (
	"errors"
	"fmt"

	"github.com/xuri/excelize/v2"
)

// ToExcel writes the DataFrame to an Excel (.xlsx) file. The first row contains
// the column headers, followed by one row per record. Null values are written as
// empty cells.
//
// Parameters:
//
//	filepath: destination .xlsx path.
//	sheet: optional sheet name (defaults to "Sheet1").
//
// This is analogous to df.to_excel(path) in pandas.
//
// Example:
//
//	err := df.ToExcel("out.xlsx")
func (df *DataFrame) ToExcel(filepath string, sheet ...string) error {
	if df == nil {
		return errors.New("ToExcel: DataFrame is nil")
	}

	sheetName := "Sheet1"
	if len(sheet) > 0 && sheet[0] != "" {
		sheetName = sheet[0]
	}

	df.RLock()
	defer df.RUnlock()

	f := excelize.NewFile()
	defer f.Close()

	// Rename the default sheet to the requested name.
	defaultSheet := f.GetSheetName(0)
	if defaultSheet != sheetName {
		if err := f.SetSheetName(defaultSheet, sheetName); err != nil {
			return fmt.Errorf("ToExcel: %w", err)
		}
	}

	// Write headers.
	for c, colName := range df.ColumnOrder {
		cell, err := excelize.CoordinatesToCellName(c+1, 1)
		if err != nil {
			return fmt.Errorf("ToExcel: %w", err)
		}
		if err := f.SetCellValue(sheetName, cell, colName); err != nil {
			return fmt.Errorf("ToExcel: %w", err)
		}
	}

	rowCount := 0
	if len(df.ColumnOrder) > 0 {
		rowCount = df.Columns[df.ColumnOrder[0]].Len()
	}

	// Write data rows (offset by 1 for the header, and 1-based indexing).
	for r := 0; r < rowCount; r++ {
		for c, colName := range df.ColumnOrder {
			series := df.Columns[colName]
			if series.IsNull(r) {
				continue // leave cell empty for nulls
			}
			cell, err := excelize.CoordinatesToCellName(c+1, r+2)
			if err != nil {
				return fmt.Errorf("ToExcel: %w", err)
			}
			v, _ := series.At(r)
			if err := f.SetCellValue(sheetName, cell, v); err != nil {
				return fmt.Errorf("ToExcel: %w", err)
			}
		}
	}

	if err := f.SaveAs(filepath); err != nil {
		return fmt.Errorf("ToExcel: failed to save file: %w", err)
	}
	return nil
}

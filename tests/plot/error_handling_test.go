package gpandas_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/plot"
	"github.com/apoplexi24/gpandas/utils/collection"
)

// TestErrorHandling_EmptyDataFrame tests error handling for empty DataFrame
func TestErrorHandling_EmptyDataFrame(t *testing.T) {
	tests := []struct {
		name      string
		plotFunc  func(*dataframe.DataFrame, *plot.ChartOptions) error
		operation string
	}{
		{
			name: "PlotBar with empty DataFrame",
			plotFunc: func(df *dataframe.DataFrame, opts *plot.ChartOptions) error {
				return df.PlotBar("x", "y", opts)
			},
			operation: "PlotBar",
		},
		{
			name: "PlotPie with empty DataFrame",
			plotFunc: func(df *dataframe.DataFrame, opts *plot.ChartOptions) error {
				return df.PlotPie("label", "value", opts)
			},
			operation: "PlotPie",
		},
		{
			name: "PlotLine with empty DataFrame",
			plotFunc: func(df *dataframe.DataFrame, opts *plot.ChartOptions) error {
				return df.PlotLine("x", []string{"y"}, opts)
			},
			operation: "PlotLine",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create empty DataFrame
			df := &dataframe.DataFrame{
				Columns:     map[string]collection.Series{},
				ColumnOrder: []string{},
				Index:       []string{},
			}

			tmpDir := t.TempDir()
			opts := &plot.ChartOptions{
				OutputPath: filepath.Join(tmpDir, "test.html"),
			}

			err := tt.plotFunc(df, opts)

			// Verify error is returned
			if err == nil {
				t.Fatal("Expected error for empty DataFrame, got nil")
			}

			// Verify error message contains "empty"
			if !strings.Contains(err.Error(), "empty") {
				t.Errorf("Expected error message to contain 'empty', got: %v", err)
			}

			// Verify error message contains operation context
			if !strings.Contains(err.Error(), tt.operation) {
				t.Errorf("Expected error message to contain '%s' context, got: %v", tt.operation, err)
			}
		})
	}
}

// TestErrorHandling_MissingColumn tests error handling for non-existent columns
func TestErrorHandling_MissingColumn(t *testing.T) {
	// Create test DataFrame
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

	tmpDir := t.TempDir()
	opts := &plot.ChartOptions{
		OutputPath: filepath.Join(tmpDir, "test.html"),
	}

	tests := []struct {
		name         string
		plotFunc     func() error
		operation    string
		missingCol   string
	}{
		{
			name: "PlotBar with missing x column",
			plotFunc: func() error {
				return df.PlotBar("nonexistent_x", "value", opts)
			},
			operation:  "PlotBar",
			missingCol: "nonexistent_x",
		},
		{
			name: "PlotBar with missing y column",
			plotFunc: func() error {
				return df.PlotBar("category", "nonexistent_y", opts)
			},
			operation:  "PlotBar",
			missingCol: "nonexistent_y",
		},
		{
			name: "PlotPie with missing label column",
			plotFunc: func() error {
				return df.PlotPie("nonexistent_label", "value", opts)
			},
			operation:  "PlotPie",
			missingCol: "nonexistent_label",
		},
		{
			name: "PlotPie with missing value column",
			plotFunc: func() error {
				return df.PlotPie("category", "nonexistent_value", opts)
			},
			operation:  "PlotPie",
			missingCol: "nonexistent_value",
		},
		{
			name: "PlotLine with missing x column",
			plotFunc: func() error {
				return df.PlotLine("nonexistent_x", []string{"value"}, opts)
			},
			operation:  "PlotLine",
			missingCol: "nonexistent_x",
		},
		{
			name: "PlotLine with missing y column",
			plotFunc: func() error {
				return df.PlotLine("category", []string{"nonexistent_y"}, opts)
			},
			operation:  "PlotLine",
			missingCol: "nonexistent_y",
		},
		{
			name: "PlotLine with one valid and one missing y column",
			plotFunc: func() error {
				return df.PlotLine("category", []string{"value", "nonexistent_y2"}, opts)
			},
			operation:  "PlotLine",
			missingCol: "nonexistent_y2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plotFunc()

			// Verify error is returned
			if err == nil {
				t.Fatal("Expected error for missing column, got nil")
			}

			// Verify error message contains "not found"
			if !strings.Contains(err.Error(), "not found") {
				t.Errorf("Expected error message to contain 'not found', got: %v", err)
			}

			// Verify error message contains the missing column name
			if !strings.Contains(err.Error(), tt.missingCol) {
				t.Errorf("Expected error message to contain column name '%s', got: %v", tt.missingCol, err)
			}

			// Verify error message contains operation context
			if !strings.Contains(err.Error(), tt.operation) {
				t.Errorf("Expected error message to contain '%s' context, got: %v", tt.operation, err)
			}
		})
	}
}

// TestErrorHandling_TypeMismatch tests error handling for incompatible column types 
func TestErrorHandling_TypeMismatch(t *testing.T) {
	// Create DataFrame with string columns (incompatible for numeric operations)
	col1, _ := collection.NewStringSeriesFromData([]string{"A", "B", "C"}, nil)
	col2, _ := collection.NewStringSeriesFromData([]string{"X", "Y", "Z"}, nil)

	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"col1": col1,
			"col2": col2,
		},
		ColumnOrder: []string{"col1", "col2"},
		Index:       []string{"0", "1", "2"},
	}

	tmpDir := t.TempDir()
	opts := &plot.ChartOptions{
		OutputPath: filepath.Join(tmpDir, "test.html"),
	}

	tests := []struct {
		name      string
		plotFunc  func() error
		operation string
	}{
		{
			name: "PlotBar with non-numeric y column",
			plotFunc: func() error {
				return df.PlotBar("col1", "col2", opts)
			},
			operation: "PlotBar",
		},
		{
			name: "PlotPie with non-numeric value column",
			plotFunc: func() error {
				return df.PlotPie("col1", "col2", opts)
			},
			operation: "PlotPie",
		},
		{
			name: "PlotLine with non-numeric y column",
			plotFunc: func() error {
				return df.PlotLine("col1", []string{"col2"}, opts)
			},
			operation: "PlotLine",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plotFunc()

			// Verify error is returned
			if err == nil {
				t.Fatal("Expected error for type mismatch, got nil")
			}

			// Verify error message indicates type issue
			// The error should mention "numeric" or "type" or similar
			errorMsg := strings.ToLower(err.Error())
			hasTypeError := strings.Contains(errorMsg, "numeric") ||
				strings.Contains(errorMsg, "type") ||
				strings.Contains(errorMsg, "int64") ||
				strings.Contains(errorMsg, "float64")

			if !hasTypeError {
				t.Errorf("Expected error message to indicate type mismatch, got: %v", err)
			}

			// Verify error message contains operation context
			if !strings.Contains(err.Error(), tt.operation) {
				t.Errorf("Expected error message to contain '%s' context, got: %v", tt.operation, err)
			}
		})
	}
}

// TestErrorHandling_FileWriteError tests error handling for file write failures
func TestErrorHandling_FileWriteError(t *testing.T) {
	// Create valid DataFrame
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

	tests := []struct {
		name         string
		outputPath   string
		operation    string
		plotFunc     func(string) error
		skipOnDarwin bool // Some file permission tests behave differently on macOS
	}{
		{
			name:       "PlotBar with empty output path",
			outputPath: "",
			operation:  "PlotBar",
			plotFunc: func(path string) error {
				opts := &plot.ChartOptions{OutputPath: path}
				return df.PlotBar("category", "value", opts)
			},
		},
		{
			name:       "PlotPie with empty output path",
			outputPath: "",
			operation:  "PlotPie",
			plotFunc: func(path string) error {
				opts := &plot.ChartOptions{OutputPath: path}
				return df.PlotPie("category", "value", opts)
			},
		},
		{
			name:       "PlotLine with empty output path",
			outputPath: "",
			operation:  "PlotLine",
			plotFunc: func(path string) error {
				opts := &plot.ChartOptions{OutputPath: path}
				return df.PlotLine("category", []string{"value"}, opts)
			},
		},
		{
			name:       "PlotBar with invalid directory path",
			outputPath: "/nonexistent/directory/that/does/not/exist/chart.html",
			operation:  "PlotBar",
			plotFunc: func(path string) error {
				opts := &plot.ChartOptions{OutputPath: path}
				return df.PlotBar("category", "value", opts)
			},
		},
		{
			name:       "PlotPie with invalid directory path",
			outputPath: "/nonexistent/directory/that/does/not/exist/chart.html",
			operation:  "PlotPie",
			plotFunc: func(path string) error {
				opts := &plot.ChartOptions{OutputPath: path}
				return df.PlotPie("category", "value", opts)
			},
		},
		{
			name:       "PlotLine with invalid directory path",
			outputPath: "/nonexistent/directory/that/does/not/exist/chart.html",
			operation:  "PlotLine",
			plotFunc: func(path string) error {
				opts := &plot.ChartOptions{OutputPath: path}
				return df.PlotLine("category", []string{"value"}, opts)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipOnDarwin && os.Getenv("GOOS") == "darwin" {
				t.Skip("Skipping on macOS due to different file permission behavior")
			}

			err := tt.plotFunc(tt.outputPath)

			// Verify error is returned
			if err == nil {
				t.Fatal("Expected error for file write failure, got nil")
			}

			// Verify error message contains operation context
			if !strings.Contains(err.Error(), tt.operation) {
				t.Errorf("Expected error message to contain '%s' context, got: %v", tt.operation, err)
			}

			// For empty path, verify specific error message
			if tt.outputPath == "" {
				if !strings.Contains(err.Error(), "output") && !strings.Contains(err.Error(), "path") {
					t.Errorf("Expected error message to mention output path issue, got: %v", err)
				}
			}
		})
	}
}

// TestErrorHandling_ErrorContextWrapping tests that all errors include proper context
func TestErrorHandling_ErrorContextWrapping(t *testing.T) {
	// Create test DataFrame
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

	tests := []struct {
		name              string
		plotFunc          func() error
		expectedOperation string
		expectedContext   []string // Additional context keywords to check
	}{
		{
			name: "PlotBar error includes operation context",
			plotFunc: func() error {
				opts := &plot.ChartOptions{OutputPath: ""}
				return df.PlotBar("category", "value", opts)
			},
			expectedOperation: "PlotBar",
			expectedContext:   []string{"output", "path"},
		},
		{
			name: "PlotBar missing column includes context",
			plotFunc: func() error {
				tmpDir := t.TempDir()
				opts := &plot.ChartOptions{OutputPath: filepath.Join(tmpDir, "test.html")}
				return df.PlotBar("missing", "value", opts)
			},
			expectedOperation: "PlotBar",
			expectedContext:   []string{"not found", "missing"},
		},
		{
			name: "PlotPie error includes operation context",
			plotFunc: func() error {
				opts := &plot.ChartOptions{OutputPath: ""}
				return df.PlotPie("category", "value", opts)
			},
			expectedOperation: "PlotPie",
			expectedContext:   []string{"output", "path"},
		},
		{
			name: "PlotPie missing column includes context",
			plotFunc: func() error {
				tmpDir := t.TempDir()
				opts := &plot.ChartOptions{OutputPath: filepath.Join(tmpDir, "test.html")}
				return df.PlotPie("missing", "value", opts)
			},
			expectedOperation: "PlotPie",
			expectedContext:   []string{"not found", "missing"},
		},
		{
			name: "PlotLine error includes operation context",
			plotFunc: func() error {
				opts := &plot.ChartOptions{OutputPath: ""}
				return df.PlotLine("category", []string{"value"}, opts)
			},
			expectedOperation: "PlotLine",
			expectedContext:   []string{"output", "path"},
		},
		{
			name: "PlotLine missing column includes context",
			plotFunc: func() error {
				tmpDir := t.TempDir()
				opts := &plot.ChartOptions{OutputPath: filepath.Join(tmpDir, "test.html")}
				return df.PlotLine("missing", []string{"value"}, opts)
			},
			expectedOperation: "PlotLine",
			expectedContext:   []string{"not found", "missing"},
		},
		{
			name: "PlotLine empty yCols includes context",
			plotFunc: func() error {
				tmpDir := t.TempDir()
				opts := &plot.ChartOptions{OutputPath: filepath.Join(tmpDir, "test.html")}
				return df.PlotLine("category", []string{}, opts)
			},
			expectedOperation: "PlotLine",
			expectedContext:   []string{"empty"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plotFunc()

			// Verify error is returned
			if err == nil {
				t.Fatal("Expected error, got nil")
			}

			errorMsg := err.Error()

			// Verify operation context is present
			if !strings.Contains(errorMsg, tt.expectedOperation) {
				t.Errorf("Expected error to contain operation '%s', got: %v", tt.expectedOperation, err)
			}

			// Verify additional context keywords are present
			for _, keyword := range tt.expectedContext {
				if !strings.Contains(strings.ToLower(errorMsg), strings.ToLower(keyword)) {
					t.Errorf("Expected error to contain context keyword '%s', got: %v", keyword, err)
				}
			}

			// Verify error message is descriptive (not just a generic error)
			if len(errorMsg) < 10 {
				t.Errorf("Error message too short, expected descriptive error, got: %v", err)
			}
		})
	}
}

// TestErrorHandling_NilDataFrame tests error handling for nil DataFrame
func TestErrorHandling_NilDataFrame(t *testing.T) {
	var df *dataframe.DataFrame

	tmpDir := t.TempDir()
	opts := &plot.ChartOptions{
		OutputPath: filepath.Join(tmpDir, "test.html"),
	}

	tests := []struct {
		name      string
		plotFunc  func() error
		operation string
	}{
		{
			name: "PlotBar with nil DataFrame",
			plotFunc: func() error {
				return df.PlotBar("x", "y", opts)
			},
			operation: "PlotBar",
		},
		{
			name: "PlotPie with nil DataFrame",
			plotFunc: func() error {
				return df.PlotPie("label", "value", opts)
			},
			operation: "PlotPie",
		},
		{
			name: "PlotLine with nil DataFrame",
			plotFunc: func() error {
				return df.PlotLine("x", []string{"y"}, opts)
			},
			operation: "PlotLine",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.plotFunc()

			// Verify error is returned
			if err == nil {
				t.Fatal("Expected error for nil DataFrame, got nil")
			}

			// Verify error message contains "nil"
			if !strings.Contains(err.Error(), "nil") {
				t.Errorf("Expected error message to contain 'nil', got: %v", err)
			}

			// Verify error message contains operation context
			if !strings.Contains(err.Error(), tt.operation) {
				t.Errorf("Expected error message to contain '%s' context, got: %v", tt.operation, err)
			}
		})
	}
}

// TestErrorHandling_ValidationOrder tests that validation occurs in the correct order
// to provide the most relevant error message 
func TestErrorHandling_ValidationOrder(t *testing.T) {
	t.Run("Empty DataFrame error takes precedence over missing column", func(t *testing.T) {
		df := &dataframe.DataFrame{
			Columns:     map[string]collection.Series{},
			ColumnOrder: []string{},
			Index:       []string{},
		}

		tmpDir := t.TempDir()
		opts := &plot.ChartOptions{
			OutputPath: filepath.Join(tmpDir, "test.html"),
		}

		err := df.PlotBar("nonexistent", "also_nonexistent", opts)

		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		// Should get empty DataFrame error, not missing column error
		if !strings.Contains(err.Error(), "empty") {
			t.Errorf("Expected 'empty' error to take precedence, got: %v", err)
		}
	})

	t.Run("Missing column error before type mismatch", func(t *testing.T) {
		// Create DataFrame with only one column
		values, _ := collection.NewFloat64SeriesFromData([]float64{10.0, 20.0, 30.0}, nil)

		df := &dataframe.DataFrame{
			Columns: map[string]collection.Series{
				"value": values,
			},
			ColumnOrder: []string{"value"},
			Index:       []string{"0", "1", "2"},
		}

		tmpDir := t.TempDir()
		opts := &plot.ChartOptions{
			OutputPath: filepath.Join(tmpDir, "test.html"),
		}

		err := df.PlotBar("nonexistent", "value", opts)

		if err == nil {
			t.Fatal("Expected error, got nil")
		}

		// Should get missing column error first
		if !strings.Contains(err.Error(), "not found") {
			t.Errorf("Expected 'not found' error, got: %v", err)
		}
	})
}

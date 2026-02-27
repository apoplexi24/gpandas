package plot

// ChartOptions configures chart properties for plotting operations.
// It provides control over chart appearance and output location.
type ChartOptions struct {
	// Title is the chart title displayed at the top
	Title string

	// Width is the chart width in pixels (default: 900)
	Width int

	// Height is the chart height in pixels (default: 500)
	Height int

	// OutputPath is the file path for HTML output (required)
	OutputPath string

	// Theme is the chart theme for styling (optional, e.g., "light", "dark")
	Theme string
}

// DefaultChartOptions returns a ChartOptions with default values applied.
// Default width: 900 pixels
// Default height: 500 pixels
// Default title: empty string
// Default theme: empty string (go-echarts default)
func DefaultChartOptions() *ChartOptions {
	return &ChartOptions{
		Width:  900,
		Height: 500,
		Title:  "",
		Theme:  "",
	}
}

// applyDefaultOptions fills in default values for unspecified ChartOptions fields.
// If opts is nil, returns a new ChartOptions with all defaults.
// Otherwise, fills in zero values with defaults while preserving user-specified values.
func applyDefaultOptions(opts *ChartOptions) *ChartOptions {
	if opts == nil {
		return DefaultChartOptions()
	}

	// Create a copy to avoid modifying the original
	result := &ChartOptions{
		Title:      opts.Title,
		Width:      opts.Width,
		Height:     opts.Height,
		OutputPath: opts.OutputPath,
		Theme:      opts.Theme,
	}

	// Apply defaults for zero values
	if result.Width == 0 {
		result.Width = 900
	}
	if result.Height == 0 {
		result.Height = 500
	}

	return result
}

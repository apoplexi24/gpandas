package dataframe_test

import (
	"strings"
	"testing"

	"github.com/apoplexi24/gpandas/dataframe"
	"github.com/apoplexi24/gpandas/utils/collection"
)

func ioDF() *dataframe.DataFrame {
	return &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"Name":   mustSeries("Alice", "Bob"),
			"Age":    mustSeries(30, 25),
			"Active": mustSeries(true, false),
		},
		ColumnOrder: []string{"Name", "Age", "Active"},
		Index:       []string{"0", "1"},
	}
}

func TestToJSON(t *testing.T) {
	s, err := ioDF().ToJSON("")
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}
	// Records orientation, column order preserved
	if !strings.HasPrefix(s, "[{") {
		t.Errorf("expected records array, got %q", s)
	}
	if !strings.Contains(s, `"Name":"Alice"`) {
		t.Errorf("missing Alice record: %s", s)
	}
	if !strings.Contains(s, `"Age":30`) {
		t.Errorf("missing Age value: %s", s)
	}
	if !strings.Contains(s, `"Active":true`) {
		t.Errorf("missing Active value: %s", s)
	}
}

func TestToJSONNulls(t *testing.T) {
	df := &dataframe.DataFrame{
		Columns: map[string]collection.Series{
			"A": mustSeries(1.0, nil),
		},
		ColumnOrder: []string{"A"},
		Index:       []string{"0", "1"},
	}
	s, err := df.ToJSON("")
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}
	if !strings.Contains(s, `"A":null`) {
		t.Errorf("expected null in output, got %s", s)
	}
}

package gpandas_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apoplexi24/gpandas"
)

func TestParquetRoundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gpandas_parquet")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	gp := gpandas.GoPandas{}
	df, _ := gp.DataFrame(
		[]string{"name", "age", "score", "active"},
		[]gpandas.Column{
			{"Alice", "Bob"},
			{int64(30), int64(25)},
			{9.5, 8.0},
			{true, false},
		},
		map[string]any{
			"name":   gpandas.StringCol{},
			"age":    gpandas.IntCol{},
			"score":  gpandas.FloatCol{},
			"active": gpandas.BoolCol{},
		},
	)

	path := filepath.Join(tmpDir, "out.parquet")
	if err := df.ToParquet(path); err != nil {
		t.Fatalf("ToParquet failed: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected parquet file: %v", err)
	}

	loaded, err := gp.Read_parquet(path)
	if err != nil {
		t.Fatalf("Read_parquet failed: %v", err)
	}

	if loaded.Len() != 2 {
		t.Fatalf("expected 2 rows, got %d", loaded.Len())
	}

	// Types inferred from the parquet schema.
	if loaded.Columns["age"].DType().String() != "int64" {
		t.Errorf("age expected int64, got %v", loaded.Columns["age"].DType())
	}
	if loaded.Columns["score"].DType().String() != "float64" {
		t.Errorf("score expected float64, got %v", loaded.Columns["score"].DType())
	}
	if loaded.Columns["active"].DType().String() != "bool" {
		t.Errorf("active expected bool, got %v", loaded.Columns["active"].DType())
	}

	// Values round-trip (including the false in row 2).
	name0, _ := loaded.Columns["name"].At(0)
	active1, _ := loaded.Columns["active"].At(1)
	if name0 != "Alice" {
		t.Errorf("expected Alice, got %v", name0)
	}
	if active1 != false {
		t.Errorf("expected false preserved, got %v", active1)
	}
}

package gpandas_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/apoplexi24/gpandas"
)

func TestReadJSONRoundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gpandas_json")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	path := filepath.Join(tmpDir, "data.json")
	content := `[{"name":"Alice","age":30},{"name":"Bob","age":25}]`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	gp := gpandas.GoPandas{}
	df, err := gp.Read_json(path)
	if err != nil {
		t.Fatalf("Read_json failed: %v", err)
	}

	if df.Len() != 2 {
		t.Fatalf("expected 2 rows, got %d", df.Len())
	}
	// Keys sorted alphabetically: age, name
	if df.ColumnOrder[0] != "age" || df.ColumnOrder[1] != "name" {
		t.Errorf("expected columns [age name], got %v", df.ColumnOrder)
	}
	// JSON numbers decode to float64
	v, _ := df.Columns["age"].At(0)
	if f, ok := v.(float64); !ok || f != 30 {
		t.Errorf("expected age 30 (float64), got %v (%T)", v, v)
	}
}

func TestReadJSONMissingKeys(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "gpandas_json2")
	defer os.RemoveAll(tmpDir)

	path := filepath.Join(tmpDir, "data.json")
	// second record missing "age"
	content := `[{"name":"Alice","age":30},{"name":"Bob"}]`
	_ = os.WriteFile(path, []byte(content), 0644)

	gp := gpandas.GoPandas{}
	df, err := gp.Read_json(path)
	if err != nil {
		t.Fatalf("Read_json failed: %v", err)
	}
	if !df.Columns["age"].IsNull(1) {
		t.Error("expected missing key to be null")
	}
}

func TestExcelRoundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gpandas_xlsx")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	gp := gpandas.GoPandas{}
	df, _ := gp.DataFrame(
		[]string{"Name", "Age"},
		[]gpandas.Column{
			{"Alice", "Bob"},
			{int64(30), int64(25)},
		},
		map[string]any{"Name": gpandas.StringCol{}, "Age": gpandas.IntCol{}},
	)

	path := filepath.Join(tmpDir, "out.xlsx")
	if err := df.ToExcel(path); err != nil {
		t.Fatalf("ToExcel failed: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file to exist: %v", err)
	}

	loaded, err := gp.Read_excel(path)
	if err != nil {
		t.Fatalf("Read_excel failed: %v", err)
	}
	if loaded.Len() != 2 {
		t.Fatalf("expected 2 rows, got %d", loaded.Len())
	}
	// Excel cells load as strings
	name, _ := loaded.Columns["Name"].At(0)
	if name != "Alice" {
		t.Errorf("expected Alice, got %v", name)
	}
	age, _ := loaded.Columns["Age"].At(1)
	if age != "25" {
		t.Errorf("expected '25' (string), got %v", age)
	}
}

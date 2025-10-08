package gpandas_test

import (
	"database/sql"
	"os"
	"testing"

	"github.com/apoplexi24/gpandas"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/lib/pq" // PostgreSQL driver
)

func TestRead_sql(t *testing.T) {
	if os.Getenv("RUN_DB_TESTS") == "" {
		t.Skip("Skipping DB tests; set RUN_DB_TESTS=1 to run")
	}
	// Create mock db
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("error creating mock database: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name        string
		query       string
		dbConfig    gpandas.DbConfig
		mockSetup   func(sqlmock.Sqlmock)
		expectError bool
	}{
		{
			name:  "successful query - SQL Server",
			query: "SELECT id, name, age FROM users",
			dbConfig: gpandas.DbConfig{
				Database_server: "sqlserver",
				Server:          "172.16.64.2",
				Port:            "1433",
				Database:        "testdb",
				Username:        "postgres",
				Password:        "pass",
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				columns := []string{"id", "name", "age"}
				mock.ExpectQuery("SELECT id, name, age FROM users").WillReturnRows(
					sqlmock.NewRows(columns).
						AddRow(1, "Alice", 30).
						AddRow(2, "Bob", 25),
				)
			},
			expectError: false,
		},
		{
			name:  "successful query - PostgreSQL",
			query: "SELECT id, name, age FROM users",
			dbConfig: gpandas.DbConfig{
				Database_server: "postgres",
				Server:          "localhost",
				Port:            "5432",
				Database:        "testdb",
				Username:        "user",
				Password:        "pass",
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				columns := []string{"id", "name", "age"}
				mock.ExpectQuery("SELECT id, name, age FROM users").WillReturnRows(
					sqlmock.NewRows(columns).
						AddRow(1, "Alice", 30).
						AddRow(2, "Bob", 25),
				)
			},
			expectError: false,
		},
		{
			name:  "query execution error",
			query: "SELECT * FROM nonexistent_table",
			dbConfig: gpandas.DbConfig{
				Database_server: "sqlserver",
				Server:          "localhost",
				Port:            "1433",
				Database:        "testdb",
				Username:        "user",
				Password:        "pass",
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SELECT * FROM nonexistent_table").WillReturnError(sql.ErrNoRows)
			},
			expectError: true,
		},
		{
			name:  "empty result set",
			query: "SELECT id, name FROM users WHERE age > 100",
			dbConfig: gpandas.DbConfig{
				Database_server: "sqlserver",
				Server:          "localhost",
				Port:            "1433",
				Database:        "testdb",
				Username:        "user",
				Password:        "pass",
			},
			mockSetup: func(mock sqlmock.Sqlmock) {
				columns := []string{"id", "name"}
				mock.ExpectQuery("SELECT id, name FROM users WHERE age > 100").
					WillReturnRows(sqlmock.NewRows(columns))
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup mock expectations
			tt.mockSetup(mock)

			// Execute test
			gp := gpandas.GoPandas{}
			df, err := gp.Read_sql(tt.query, tt.dbConfig)

			// Check error expectations
			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			// Additional checks for successful cases
			if !tt.expectError && err == nil {
				if df == nil {
					t.Error("expected non-nil DataFrame")
					return
				}

				// Verify that all expectations were met
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("unfulfilled expectations: %v", err)
				}

				if len(df.ColumnOrder) == 0 {
					t.Error("expected non-empty ColumnOrder")
				}
				if len(df.Columns) != len(df.ColumnOrder) {
					t.Errorf("columns map size and ColumnOrder mismatch: %d vs %d", len(df.Columns), len(df.ColumnOrder))
				}

				// If rows exist, ensure per-column lengths match and values are non-nil
				rows := 0
				if len(df.ColumnOrder) > 0 {
					rows = df.Columns[df.ColumnOrder[0]].Len()
					for _, c := range df.ColumnOrder[1:] {
						if l := df.Columns[c].Len(); l != rows {
							t.Errorf("column %s length mismatch: expected %d, got %d", c, rows, l)
						}
					}
				}

				// Optional: verify basic types are consistent (can't assert exact DB types here)
				if rows > 0 {
					for _, c := range df.ColumnOrder {
						dt := df.Columns[c].DType()
						if dt == nil {
							t.Errorf("unexpected nil dtype for column %s", c)
						}
					}
				}
			}
		})
	}
}

func TestFrom_gbq(t *testing.T) {
	// Note: Testing BigQuery functionality typically requires integration tests
	// with actual BigQuery service or a more sophisticated mock.
	// Here we'll just test basic error cases that don't require BigQuery connection

	tests := []struct {
		name        string
		query       string
		projectID   string
		expectError bool
	}{
		{
			name:        "empty project ID",
			query:       "SELECT * FROM dataset.table",
			projectID:   "",
			expectError: true,
		},
		{
			name:        "empty query",
			query:       "",
			projectID:   "test-project",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gp := gpandas.GoPandas{}
			df, err := gp.From_gbq(tt.query, tt.projectID)

			if tt.expectError && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !tt.expectError && df == nil {
				t.Error("expected non-nil DataFrame")
			}
		})
	}
}

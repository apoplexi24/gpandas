package gpandas_test

import (
	"database/sql"
	"gpandas"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/lib/pq" // PostgreSQL driver
)

func TestRead_sql(t *testing.T) {
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

				// Check DataFrame structure
				if len(df.Columns) == 0 {
					t.Error("expected non-empty columns")
				}

				// For non-empty result sets, check data consistency
				if df.Rows() > 0 {
					// Check if all columns have the same length
					numRows := df.Rows()
					for _, col := range df.Columns {
						series, ok := df.Series[col]
						if !ok {
							t.Errorf("column %s not found in Series map", col)
							continue
						}

						if series.Len() != numRows {
							t.Errorf("column %s has inconsistent length: expected %d, got %d",
								col, numRows, series.Len())
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

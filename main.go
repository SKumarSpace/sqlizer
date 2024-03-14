package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
)

type Parameter struct {
	Name     string `db:"name"`
	TypeName string `db:"suggested_system_type_name"`
}

type ResultColumn struct {
	Name     string `db:"name"`
	TypeName string `db:"system_type_name"`
}

type File struct {
	Name          string
	Content       string
	Parameters    []Parameter
	ResultColumns []ResultColumn

	ParameterString      string
	ParameterNamedString string
	ResultTypesString    string
	ResultScanString     string
}

func main() {
	// Define a flag for the directory path
	dirPath := flag.String("dir", "", "directory path")
	flag.Parse()

	// Check if the directory path is provided
	if *dirPath == "" {
		log.Fatal("Directory path is required")
	}

	// Get the absolute path of the directory
	absDirPath, err := filepath.Abs(*dirPath)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Directory path:", absDirPath)

	// Read the directory
	files, err := os.ReadDir(absDirPath)
	if err != nil {
		log.Fatal(err)
	}

	err = godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	pool, err := sqlx.Open("sqlserver", os.Getenv("CONNECTION_STRING"))
	if err != nil {
		panic(fmt.Sprintf("failed to open database connection, error: %v", err))
	}

	pool = pool.Unsafe()

	ctx := context.Background()
	conn, err := pool.Connx(ctx)
	if err != nil {
		panic(fmt.Sprintf("failed to open database connection, error: %v", err))
	}

	defer conn.Close()

	var list []File

	// Loop through the files in the directory
	for _, file := range files {
		// Get file content
		content, err := os.ReadFile(filepath.Join(absDirPath, file.Name()))
		if err != nil {
			log.Fatal(err)
		}

		// Get parameters
		parameters, err := getParameters(ctx, conn, string(content))
		die(err)

		var parameterString []string
		var parameterNamedString []string
		for _, parameter := range parameters {
			parameterString = append(parameterString, fmt.Sprintf("%s %s", strings.ReplaceAll(parameter.Name, "@", ""), parameter.TypeName))
			parameterNamedString = append(parameterNamedString, fmt.Sprintf(`sql.Named("%s", %s)`, strings.ReplaceAll(parameter.Name, "@", ""), strings.ReplaceAll(parameter.Name, "@", "")))
		}

		// Get result columns
		resultColumns, err := getResultColumns(ctx, conn, string(content))
		if err != nil {
			log.Fatal(err)
		}

		var resultScans []string
		var resultTypes []string
		for _, resultColumn := range resultColumns {
			resultTypes = append(resultTypes, resultColumn.TypeName)
			resultScans = append(resultScans, fmt.Sprintf("&%s", resultColumn.Name))
		}

		// Append the file to the list
		list = append(list, File{
			Name:                 strings.ReplaceAll(file.Name(), ".sql", ""),
			Content:              string(content),
			Parameters:           parameters,
			ResultColumns:        resultColumns,
			ParameterString:      strings.Join(parameterString, ", "),
			ParameterNamedString: strings.Join(parameterNamedString, ", "),
			ResultTypesString:    strings.Join(resultTypes, ", "),
			ResultScanString:     strings.Join(resultScans, ", "),
		})
	}

	// Print the list
	f, err := os.Create("output.go")
	die(err)
	defer f.Close()

	err = packageTemplate.Execute(f, struct {
		Timestamp time.Time
		Files     []File
	}{
		Timestamp: time.Now(),
		Files:     list,
	})
	die(err)
}

func die(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func getParameters(ctx context.Context, conn *sqlx.Conn, query string) ([]Parameter, error) {
	var parameters []Parameter
	rows, err := conn.QueryxContext(ctx, `EXEC sp_describe_undeclared_parameters @sql`, sql.Named("sql", query))
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var parameter Parameter
		err := rows.StructScan(&parameter)
		if err != nil {
			return nil, err
		}

		parameter.Name = strings.ReplaceAll(parameter.Name, "@", "")

		if strings.Contains(parameter.TypeName, "varchar") {
			parameter.TypeName = "string"
		} else if strings.Contains(parameter.TypeName, "int") {
			parameter.TypeName = "int"
		} else {
			parameter.TypeName = "any"
		}

		parameters = append(parameters, parameter)
	}

	return parameters, nil
}

func getResultColumns(ctx context.Context, conn *sqlx.Conn, query string) ([]ResultColumn, error) {
	var resultColumns []ResultColumn
	rows, err := conn.QueryxContext(ctx, `SELECT name, system_type_name FROM sys.dm_exec_describe_first_result_set(@sql, null, 0)`, sql.Named("sql", query))
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		var resultColumn ResultColumn
		err := rows.StructScan(&resultColumn)
		if err != nil {
			return nil, err
		}

		if resultColumn.TypeName == "varchar" {
			resultColumn.TypeName = "string"
		} else if resultColumn.TypeName == "int" {
			resultColumn.TypeName = "int"
		} else if resultColumn.TypeName == "bit" {
			resultColumn.TypeName = "bool"
		} else {
			resultColumn.TypeName = "any"
		}

		resultColumns = append(resultColumns, resultColumn)
	}

	return resultColumns, nil
}

var packageTemplate = template.Must(template.New("").Parse(`// Code generated by go generate; DO NOT EDIT.
// This file was generated by robots at
// {{ .Timestamp }}
package database

import (
	"context"
	"database/sql"
	"fmt"

	_ "embed"

	// "github.com/jmoiron/sqlx"
)

{{- range .Files }}

//go:embed sql/{{ .Name }}.sql
var {{ .Name }}QueryAuto string

func {{ .Name }}({{ .ParameterString }}) ({{ .ResultTypesString }}, error) {
	ctx := context.Background()
	conn, err := pool.Connx(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to open connection, error: %v", err)
	}

	defer conn.Close()

	{{range .ResultColumns }}
	var {{ .Name }} {{ .TypeName }}
	{{- end }}

	err = conn.QueryRowxContext(ctx, {{.Name}}QueryAuto, {{ .ParameterNamedString }}).Scan({{ .ResultScanString }})
	if err != nil {
		return false, fmt.Errorf("failed to scan row, error: %v", err)
	}

	return true, nil
}
{{- end }}
`))

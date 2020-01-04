package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

func main() {
	// Read arguments
	arguments := argumentsToMap(os.Args[1:])
	if mapContains(arguments, "-h") || mapContains(arguments, "-help") {
		fmt.Println("json2sql upload JSON files to sql server")
		fmt.Println("Flags:")
		fmt.Println("\t-c sql server connection string")
		fmt.Println("\t-f path to JSON file")
		fmt.Println("\t-t table to use (defaults to JSON file name)")
		os.Exit(0)
	}

	if mapContains(arguments, "-c") == false {
		log.Fatal("Connection string required with -c flag")
	} else if mapContains(arguments, "-f") == false {
		log.Fatal("JSON file path required with -f flag")
	}

	// Connect to database
	pool, err := sql.Open("mssql", arguments["-c"])
	if err != nil {
		log.Fatal("Cannot connect to database")
	}
	fmt.Println("Connected to database")

	filename := arguments["-f"]
	var tableName string
	if val, ok := arguments["-t"]; ok {
		tableName = val
	} else {
		tableName = filename[:strings.IndexByte(filename, '.')]
	}

	// Read file
	jsonFile, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
	}
	defer jsonFile.Close()

	byteValue, _ := ioutil.ReadAll(jsonFile)

	var results []map[string]interface{}
	err = json.Unmarshal([]byte(byteValue), &results)
	if err != nil {
		fmt.Println(err)
	}

	columns := make([]string, 0, len(results[0]))
	for key := range results[0] {
		columns = append(columns, key)
	}
	columnsString := strings.Join(columns, ",")

	for _, object := range results {
		values := make([]string, 0, len(columns))
		for _, key := range columns {
			valAsString, err := valueToSQLField(object[key])
			if err != nil {
				log.Fatal(err)
			}
			values = append(values, valAsString)
		}

		query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s);", tableName, columnsString, strings.Join(values, ","))
		_, err = pool.Query(query)
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("Successfully added %d entries to table '%s'\n", len(results), tableName)
}

func argumentsToMap(arguments []string) map[string]string {
	args := map[string]string{}

	for i := 0; i < len(arguments); i += 2 {
		if len(arguments) <= i+1 {
			args[arguments[i]] = ""
		} else {
			args[arguments[i]] = arguments[i+1]
		}
	}

	return args
}

func mapContains(m map[string]string, key string) bool {
	_, ok := m[key]
	return ok
}

func valueToSQLField(val interface{}) (string, error) {
	switch val.(type) {
	case bool:
		if val == true {
			return "1", nil
		}
		return "0", nil
	case string:
		return fmt.Sprintf("'%s'", val), nil
	case int:
		return fmt.Sprint(val), nil
	case float64:
		return fmt.Sprint(val), nil
	default:
		return "", errors.New("unknown type")
	}
}

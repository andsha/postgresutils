package postgresutils

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/andsha/securestorage"
	"github.com/andsha/vconfig"
)

//var pgProcesses []PostgresProcess

type PostgresProcess struct {
	pgHost   string
	pgPort   string
	pgDBName string
	pgUser   string
	pgDB     *sql.DB // safe for concurrent use by multiple goroutines
}

func NewDB(host, port, dbname, user, password, sslmode string, pwdSection *vconfig.Section) (*PostgresProcess, error) {
	keyStorage, err := securestorage.NewSecureStorage("", "", pwdSection)
	if err != nil {
		return nil, err
	}

	pgProcess := new(PostgresProcess)
	connInfo := ""
	if host != "" {
		pgProcess.pgHost = host
		connInfo += fmt.Sprintf("host=%s ", host)
	}
	if port != "" {
		pgProcess.pgPort = port
		connInfo += fmt.Sprintf(" port=%s ", port)
	}
	if dbname != "" {
		pgProcess.pgDBName = dbname
		connInfo += fmt.Sprintf(" dbname=%s ", dbname)
	}
	if user != "" {
		pgProcess.pgUser = user
		connInfo += fmt.Sprintf(" user=%s ", user)
	}
	if password != "" {
		if strings.HasSuffix(password, ".key") {
			password, err = keyStorage.GetPasswordFromFile(password)
		} else {
			password, err = keyStorage.GetPasswordFromString(password)
		}
		if err != nil {
			return nil, err
		}
		connInfo += fmt.Sprintf(" password=%s ", password)
	}
	if sslmode != "" {
		connInfo += fmt.Sprintf(" sslmode=%s ", sslmode)
	}

	database, err := sql.Open("postgres", connInfo)
	if err != nil {
		return nil, err
	}
	if err := database.Ping(); err != nil {
		return nil, err
	}
	pgProcess.pgDB = database
	return pgProcess, nil

}

func (process *PostgresProcess) CloseDB() error {
	if err := process.pgDB.Close(); err != nil {
		return err
	}
	return nil
}

func (process *PostgresProcess) Run(sql string) ([][]interface{}, error) {
	rows, err := process.pgDB.Query(sql)

	if err != nil {
		return nil, err
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	tmpres := make([]interface{}, len(cols))
	dest := make([]interface{}, len(cols))
	var result [][]interface{}

	for i, _ := range tmpres {
		dest[i] = &tmpres[i]
	}

	for rows.Next() {
		rows.Scan(dest...)
		result = append(result, tmpres)
	}

	return result, nil

}

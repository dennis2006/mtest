package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"log"
	"net"
	"os"
	"strconv"
	"sync/atomic"
)

// MockBuilder struct for building and managing the mock MySQL server
type MockBuilder struct {
	dbName  string
	port    int
	server  *server.Server
	sqlDB   *sql.DB
	sqlxDB  *sqlx.DB
	err     error
	started atomic.Bool

	sqlStmts []string
	sqlFiles []string
}

// Builder initializes a new MockBuilder instance with db name,
// if db name is not provided, gmm would generate a random db name.
func Builder(db ...string) *MockBuilder {
	b := &MockBuilder{
		sqlStmts: make([]string, 0),
		sqlFiles: make([]string, 0),
		started:  atomic.Bool{},
	}
	dbName := "test-db-" + uuid.NewString()[:6]
	if len(db) > 0 {
		dbName = db[0]
	}
	b.dbName = dbName
	return b
}

// Port sets the port for the MySQL server,
// if not set, gmm would generate a port start from 19527
func (b *MockBuilder) Port(port int) *MockBuilder {
	b.port = port
	return b
}

// GetPort returns the port of the MySQL server,
// if not set, gmm would return the port of the server.
func (b *MockBuilder) GetPort() int {
	if b.port == 0 {
		return b.server.Listener.Addr().(*net.TCPAddr).Port
	}
	return b.port
}

// Build initializes and starts the MySQL server, returns handles to SQL and Gorm DB
func (b *MockBuilder) Build() (*sqlx.DB, *sql.DB, func(), error) {
	if b.err != nil {
		return nil, nil, nil, b.err
	}

	if !b.started.CompareAndSwap(false, true) {
		return nil, nil, nil, errors.New("mysql server already started")
	}

	// If not specify port, get an unused one form local machine.
	//
	// NOTE: The `getFreePort` method indeed has the limitation
	// that it cannot guarantee the port will remain available.
	// While it can find a currently unused port,
	// there is a possibility that the port might be occupied
	// by another process between the time the port number is
	// retrieved and the moment it is actually used.
	if b.port == 0 {
		var listener net.Listener
		listener, b.port, b.err = getFreePort()
		if b.err != nil {
			return nil, nil, nil, b.err
		}
		_ = listener.Close()
	}

	// Init mysql server
	b.initServer()
	if b.err != nil {
		return nil, nil, nil, b.err
	}

	// Start mysql server
	log.Print("start go mysql mocker server, listening at 127.0.0.1:" + strconv.Itoa(b.port))
	go func() {
		if err := b.server.Start(); err != nil {
			panic(err)
		}
	}()

	shutdown := func() {
		_ = b.server.Close()
	}

	// Create client and connect to server
	var err error
	b.sqlxDB, b.sqlDB, err = createMySQLClient(b.port, b.dbName)
	if err != nil {
		b.err = fmt.Errorf("failed to create sql client: %w", err)
		return nil, nil, nil, b.err
	}

	b.initWithStmts()
	b.initWithFiles()
	if b.err != nil {
		return nil, nil, nil, b.err
	}

	return b.sqlxDB, b.sqlDB, shutdown, nil
}

// initServer initializes the mock MySQL server
func (b *MockBuilder) initServer() *MockBuilder {
	if b.err != nil {
		return b
	}
	b.server, b.err = createMySQLServer(b.dbName, b.port)
	return b
}

// SQLStmts adds SQL statements to be executed upon initialization
func (b *MockBuilder) SQLStmts(stmts ...string) *MockBuilder {
	b.sqlStmts = append(b.sqlStmts, stmts...)
	return b
}

// SQLFiles adds SQL files whose contents are to be executed upon initialization
func (b *MockBuilder) SQLFiles(files ...string) *MockBuilder {
	for _, file := range files {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			b.err = fmt.Errorf("sql file %s not exist", file)
			return b
		}
	}

	b.sqlFiles = append(b.sqlFiles, files...)
	return b
}

func (b *MockBuilder) initWithStmts() {
	if b.err != nil || len(b.sqlStmts) == 0 {
		return
	}
	log.Print("start to init data with sql stmts, count = " + strconv.Itoa(len(b.sqlStmts)))
	for _, stmt := range b.sqlStmts {
		stmts, err := splitSQLStatements(stmt)
		if err != nil {
			b.err = err
			return
		}
		if err = b.executeSQLStatements(stmts); err != nil {
			b.err = err
			return
		}
	}
	log.Print("init data with sql stmts successfully, count = " + strconv.Itoa(len(b.sqlStmts)))
}

func (b *MockBuilder) initWithFiles() {
	if b.err != nil || len(b.sqlFiles) == 0 {
		return
	}
	log.Print("start to init data with sql files, count = " + strconv.Itoa(len(b.sqlFiles)))
	for _, file := range b.sqlFiles {
		stmts, err := splitSQLFile(file)
		if err != nil {
			b.err = fmt.Errorf("failed to split sql file '%s': %w", file, err)
			return
		}
		if err = b.executeSQLStatements(stmts); err != nil {
			b.err = err
			return
		}
	}
	log.Print("init data with sql files successfully, count = " + strconv.Itoa(len(b.sqlFiles)))
}

func (b *MockBuilder) executeSQLStatements(stmts []string) error {
	for _, stmt := range stmts {
		_, err := b.sqlDB.Exec(stmt)
		if err != nil {
			return fmt.Errorf("failed to exec sql stmt '%s': %w", stmt, err)
		}
	}
	return nil
}

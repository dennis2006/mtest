package mysql

import (
	"context"
	"fmt"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
)

func createMySQLServer(dbName string, port int) (*server.Server, error) {
	// create a new database
	db := memory.NewDatabase(dbName)
	db.BaseDatabase.EnablePrimaryKeyIndexes()

	pro := memory.NewDBProvider(db)
	session := memory.NewSession(sql.NewBaseSession(), pro)
	ctx := sql.NewContext(context.Background(), sql.WithSession(session))
	ctx.SetCurrentDatabase(dbName)

	// create a new server engine
	engine := sqle.NewDefault(pro)
	config := server.Config{
		Protocol: "tcp",
		Address:  fmt.Sprintf("127.0.0.1:%d", port),
	}

	// create a new server
	s, err := server.NewServer(config, engine, sql.NewContext, memory.NewSessionBuilder(pro), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create server: %w", err)
	}
	return s, nil
}

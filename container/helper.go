package container

import (
	"context"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/qiniu/qmgo"
	qnOpts "github.com/qiniu/qmgo/options"
	r "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/redis"
	"go.mongodb.org/mongo-driver/mongo/options"
	"mutils/mtest/container/doris"
	"os"
	"path/filepath"
)

type RedisContainer struct {
	*redis.RedisContainer
	RedisCli *r.Client
}

type MySQLContainer struct {
	*mysql.MySQLContainer
	Db *sqlx.DB
}

type MongoDBContainer struct {
	*mongodb.MongoDBContainer
	MongoCli *qmgo.Client
}

type DorisContainer struct {
	*doris.Container
	Db *sqlx.DB
}

func CreateRedisContainer(ctx context.Context) (*RedisContainer, error) {
	c, err := redis.Run(ctx, "redis:6.2.6")
	if err != nil {
		return nil, err
	}

	connStr, err := c.ConnectionString(ctx)
	if err != nil {
		return nil, err
	}

	options, err := r.ParseURL(connStr)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to connect to Redis: %v\n", err)
		return nil, err
	}

	cli := r.NewClient(options)

	return &RedisContainer{
		RedisContainer: c,
		RedisCli:       cli,
	}, nil
}

func CreateMySQLContainer(ctx context.Context) (*MySQLContainer, error) {
	c, err := mysql.Run(ctx,
		"mysql:8.4.5",
		mysql.WithConfigFile(filepath.Join("..", "mounts", "mysql", "my_8.cnf")),
		mysql.WithDatabase("foo"),
		mysql.WithUsername("root"),
		mysql.WithPassword("password"),
	)
	if err != nil {
		return nil, err
	}

	connStr, err := c.ConnectionString(ctx)
	if err != nil {
		return nil, err
	}

	db, err := sqlx.Connect("mysql", connStr)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to connect to mysql: %v\n", err)
		return nil, err
	}

	if err = db.Ping(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to ping MySQL: %v\n", err)
		return nil, err
	}

	return &MySQLContainer{
		MySQLContainer: c,
		Db:             db,
	}, nil
}

func CreateMongoDBContainer(ctx context.Context) (*MongoDBContainer, error) {
	c, err := mongodb.Run(ctx,
		"mongo:6.0.19",
	)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to start container: %v\n", err)
		return nil, err
	}

	connStr, err := c.ConnectionString(ctx)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to get connection string: %v\n", err)
		return nil, err
	}

	var (
		timeout     int64  = 2000
		maxPoolSize uint64 = 100
		minPoolSize uint64 = 0
	)
	opts := qnOpts.ClientOptions{
		ClientOptions: options.Client().ApplyURI(connStr),
	}
	cfg := qmgo.Config{
		Uri:              connStr,
		ConnectTimeoutMS: &timeout,
		MaxPoolSize:      &maxPoolSize,
		MinPoolSize:      &minPoolSize,
	}

	mongoCli, err := qmgo.NewClient(ctx, &cfg, opts)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to connect to mongodb: %v\n", err)
		return nil, err
	}

	err = mongoCli.Ping(5)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "MongoClient ping failed:  %v\n", err)
		return nil, err
	}

	return &MongoDBContainer{
		MongoDBContainer: c,
		MongoCli:         mongoCli,
	}, nil
}

func CreateDorisContainer(ctx context.Context) (*DorisContainer, error) {
	c, err := doris.Run(ctx, "starrocks/allin1-ubuntu:3.4.3")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to start container: %v\n", err)
		return nil, err
	}

	connStr, err := c.ConnectionString(ctx, "charset=utf8mb4", "parseTime=True")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to get connection string: %v\n", err)
		return nil, err
	}

	db, err := sqlx.Connect("mysql", connStr)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Unable to connect to mysql: %v\n", err)
		return nil, err
	}

	if err = db.Ping(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "failed to ping MySQL: %v\n", err)
		return nil, err
	}

	return &DorisContainer{
		Container: c,
		Db:        db,
	}, nil
}

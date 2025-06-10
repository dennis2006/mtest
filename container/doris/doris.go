package doris

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

//go:embed mounts/init.sql.tpl
var embedDorisConfigTpl string

const (
	defaultDorisInitContainerPath = "/tmp/doris.init"
	defaultPassword               = "test"
	defaultDatabaseName           = "test"
)

// Container represents the StarRocks container type used in the module
type Container struct {
	testcontainers.Container
	password string
	database string
}

// Deprecated: use Run instead
// RunContainer creates an instance of the StarRocks container type
func RunContainer(ctx context.Context, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	return Run(ctx, "starrocks/allin1-ubuntu:3.4.3", opts...)
}

// Run creates an instance of the StarRocks container type
func Run(ctx context.Context, img string, opts ...testcontainers.ContainerCustomizer) (*Container, error) {
	req := testcontainers.ContainerRequest{
		Image:        img,
		Env:          make(map[string]string),
		ExposedPorts: []string{"9030/tcp"},
		WaitingFor:   wait.ForLog("Enjoy the journey to StarRocks blazing-fast lake-house engine!"),
	}

	genericContainerReq := testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	}

	// 处理默认参数 && 合并自定义参数
	defaultOpts := defaultOptions(ctx)
	opts = append(defaultOpts, opts...)
	for _, opt := range opts {
		if err := opt.Customize(&genericContainerReq); err != nil {
			return nil, err
		}
	}
	database := genericContainerReq.Env["DORIS_DATABASE"]
	password := genericContainerReq.Env["DORIS_PASSWORD"]

	// 根据参数及模板生成初始化脚本文件
	initScriptBytes, err := renderEmbedDorisConfig(database, password)
	if err != nil {
		return nil, fmt.Errorf("render config: %w", err)
	}
	tmpConfigFile := filepath.Join(os.TempDir(), "init.sql.tmp")
	err = os.WriteFile(tmpConfigFile, initScriptBytes, 0o600)
	if err != nil {
		return nil, err
	}

	var postOpts []testcontainers.ContainerCustomizer

	// 挂载初始化脚本 && 执行初始化脚本
	dorisInitScript := testcontainers.WithFiles(testcontainers.ContainerFile{
		HostFilePath:      tmpConfigFile,
		ContainerFilePath: defaultDorisInitContainerPath,
		FileMode:          0o644,
	})
	initCmd := testcontainers.NewRawCommand(
		[]string{"/bin/sh", "-c", fmt.Sprintf("mysql -P9030 -h127.0.0.1 -uroot -e 'source %s'", defaultDorisInitContainerPath)})
	postOpts = append(postOpts, dorisInitScript, testcontainers.WithAfterReadyCommand(initCmd))

	// 挂载其它文件 && 执行其它脚本
	var execs []testcontainers.Executable
	for _, opt := range genericContainerReq.Files {
		if opt.ContainerFilePath == defaultDorisInitContainerPath {
			// skip
		} else {
			execs = append(execs, testcontainers.NewRawCommand([]string{
				"/bin/sh",
				"-c",
				fmt.Sprintf("mysql -P9030 -h127.0.0.1 -uroot -p%s %s -e 'source %s'", password, database, opt.ContainerFilePath),
			}))
		}
	}
	postOpts = append(postOpts, dorisInitScript, testcontainers.WithAfterReadyCommand(execs...))

	for _, opt := range postOpts {
		if err = opt.Customize(&genericContainerReq); err != nil {
			return nil, err
		}
	}

	container, err := testcontainers.GenericContainer(ctx, genericContainerReq)
	var c *Container
	if container != nil {
		c = &Container{
			Container: container,
			database:  database,
			password:  password,
		}
	}

	if err != nil {
		return c, fmt.Errorf("generic container: %w", err)
	}

	return c, nil
}

// MustConnectionString panics if the address cannot be determined.
func (c *Container) MustConnectionString(ctx context.Context, args ...string) string {
	addr, err := c.ConnectionString(ctx, args...)
	if err != nil {
		panic(err)
	}
	return addr
}

func (c *Container) ConnectionString(ctx context.Context, args ...string) (string, error) {
	containerPort, err := c.MappedPort(ctx, "9030/tcp")
	if err != nil {
		return "", err
	}

	host, err := c.Host(ctx)
	if err != nil {
		return "", err
	}

	extraArgs := ""
	if len(args) > 0 {
		extraArgs = strings.Join(args, "&")
	}
	if extraArgs != "" {
		extraArgs = "?" + extraArgs
	}

	connectionString := fmt.Sprintf("root:%s@tcp(%s:%s)/%s%s", c.password, host, containerPort.Port(), c.database, extraArgs)
	return connectionString, nil
}

func defaultOptions(ctx context.Context) []testcontainers.ContainerCustomizer {
	return []testcontainers.ContainerCustomizer{
		WithDatabase(defaultDatabaseName),
		WithPassword(defaultPassword),
	}
}

func WithPassword(password string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["DORIS_PASSWORD"] = password

		return nil
	}
}

func WithDatabase(database string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		req.Env["DORIS_DATABASE"] = database

		return nil
	}
}

func WithSQLScripts(scripts ...string) testcontainers.CustomizeRequestOption {
	return func(req *testcontainers.GenericContainerRequest) error {
		var initScripts []testcontainers.ContainerFile
		for _, script := range scripts {
			if !strings.EqualFold(".sql", filepath.Ext(script)) {
				return fmt.Errorf("file %s is not a sql file", script)
			}

			cf := testcontainers.ContainerFile{
				HostFilePath:      script,
				ContainerFilePath: "/tmp/" + filepath.Base(script),
				FileMode:          0o644,
			}
			initScripts = append(initScripts, cf)
		}
		req.Files = append(req.Files, initScripts...)

		return nil
	}
}

type embedDorisConfigTplParams struct {
	Database string
	Password string
}

// renderEmbedDorisConfig renders the embed etcd config template with the given database/password
// and returns it as []byte.
func renderEmbedDorisConfig(database, password string) ([]byte, error) {
	tplParams := embedDorisConfigTplParams{
		Database: database,
		Password: password,
	}

	dorisCfgTpl, err := template.New("init.sql").Parse(embedDorisConfigTpl)
	if err != nil {
		return nil, fmt.Errorf("failed to parse embed StarRocks config file template: %w", err)
	}

	var embedDorisSql bytes.Buffer
	if err = dorisCfgTpl.Execute(&embedDorisSql, tplParams); err != nil {
		return nil, fmt.Errorf("failed to render embed StarRocks config template: %w", err)
	}

	return embedDorisSql.Bytes(), nil
}

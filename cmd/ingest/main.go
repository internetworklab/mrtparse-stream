package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
	pkgtask "github.com/internetworklab/mrtparse-stream/pkg/task"
	pkgutils "github.com/internetworklab/mrtparse-stream/pkg/utils"

	"github.com/alecthomas/kong"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type Sink string

const (
	SinkPostgres   Sink = "postgres"
	SinkJSONStdout Sink = "json-stdout"
	SinkNull       Sink = "null"
)

type CLI struct {
	Source   string `arg:"" help:"URL or file path of the MRT data source (e.g. https://data.ris.ripe.net/rrc00/2026.05/bview.20260502.1600.gz or bview.20260502.1600.gz)."`
	Provider string `name:"provider" default:"ripe-ris" help:"Data source provider identifier."`
	Sink     Sink   `name:"sink" default:"postgres" enum:"postgres,json-stdout,null" help:"Sink destination for ingested data."`
	Limit    int    `name:"limit" default:"0" help:"Maximum number of entries to process. 0 means no limit."`

	PgUserEnv             string `name:"pg-user-env" default:"TEST_PG_USER" help:"Environment variable name for PostgreSQL user."`
	PgPassEnv             string `name:"pg-pass-env" default:"TEST_PG_PASSWORD" help:"Environment variable name for PostgreSQL password."`
	PgHostPortEnv         string `name:"pg-hostport-env" default:"TEST_PG_HOSTPORT" help:"Environment variable name for PostgreSQL host:port."`
	PgDBNameEnv           string `name:"pg-dbname-env" default:"TEST_PG_DBNAME" help:"Environment variable name for PostgreSQL database name."`
	MRTEntriesTablePrefix string `name:"mrt-entries-table-prefix" default:"mrt_entries" help:"Table name prefix for per-generation MRT entries tables."`
}

func (c *CLI) getConnStr() string {
	user := os.Getenv(c.PgUserEnv)
	password := os.Getenv(c.PgPassEnv)
	hostport := os.Getenv(c.PgHostPortEnv)
	dbname := os.Getenv(c.PgDBNameEnv)
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, hostport, dbname)
}

func (c *CLI) Run() error {
	ctx := context.Background()

	logger := log.New(os.Stderr, "", log.LstdFlags)
	ctx = context.WithValue(ctx, pkgutils.CtxKeyLogger, logger)

	if err := godotenv.Load(); err != nil {
		logger.Printf("failed to load .env file: %v", err)
	}

	rc, err := pkgutils.GetDecompressedSourceReadCloser(ctx, c.Source)
	if err != nil {
		return fmt.Errorf("failed to open source: %w", err)
	}
	defer rc.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived signal, shutting down...")
		cancel()
	}()

	switch c.Sink {
	case SinkPostgres:
		return c.runPGSqlIngestTask(ctx, rc)
	case SinkJSONStdout:
		return c.runJSONStdoutIngestTask(ctx, rc)
	case SinkNull:
		return c.runNullIngestTask(ctx, rc)
	default:
		return fmt.Errorf("unsupported sink: %s", c.Sink)
	}
}

func (c *CLI) runPGSqlIngestTask(ctx context.Context, source io.Reader) error {
	connStr := c.getConnStr()
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	tableBuilder, err := pkgdb.NewMRTEntriesTableBuilder(pool, c.MRTEntriesTablePrefix)
	if err != nil {
		return fmt.Errorf("failed to create table builder: %w", err)
	}

	writer, err := pkgdb.NewPG_SQL_MRTEntries_Write_Channel(
		ctx,
		pool,
		c.Provider,
		tableBuilder,
		pkgdb.WithStreamMaxReadyGenerationsAllowed(1),
	)
	if err != nil {
		return fmt.Errorf("failed to create streaming writer: %w", err)
	}

	ingestTask := pkgtask.NewIngestTask(
		source,
		writer,
		pkgtask.WithShowProgress(true),
		pkgtask.WithShowRate(true),
		pkgtask.WithPGIngestLimit(c.Limit),
	)

	return ingestTask.Run(ctx)
}

func (c *CLI) runJSONStdoutIngestTask(ctx context.Context, source io.Reader) error {
	return pkgtask.NewJSONIngestTask(source, pkgtask.WithJSONLineIngestLimit(c.Limit)).Run(ctx)
}

func (c *CLI) runNullIngestTask(ctx context.Context, source io.Reader) error {
	return pkgtask.NewNullIngestTask(
		source,
		pkgtask.WithNullShowProgress(true),
		pkgtask.WithNullShowRate(true),
		pkgtask.WithNullIngestLimit(c.Limit),
	).Run(ctx)
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli)
	ctx.FatalIfErrorf(cli.Run())
}

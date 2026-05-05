package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
	pkgtask "github.com/internetworklab/mrtparse-stream/pkg/task"

	"log"

	"github.com/alecthomas/kong"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type Sink string

const (
	SinkPostgres   Sink = "postgres"
	SinkJSONStdout Sink = "json-stdout"
)

type CLI struct {
	Source   string `arg:"" help:"URL or file path of the MRT data source (e.g. https://data.ris.ripe.net/rrc00/2026.05/bview.20260502.1600.gz or bview.20260502.1600.gz)."`
	Provider string `name:"provider" default:"ripe-ris" help:"Data source provider identifier."`
	Sink     Sink   `name:"sink" default:"postgres" enum:"postgres,json-stdout" help:"Sink destination for ingested data."`

	PgUserEnv     string `name:"pg-user-env" default:"TEST_PG_USER" help:"Environment variable name for PostgreSQL user."`
	PgPassEnv     string `name:"pg-pass-env" default:"TEST_PG_PASSWORD" help:"Environment variable name for PostgreSQL password."`
	PgHostPortEnv string `name:"pg-hostport-env" default:"TEST_PG_HOSTPORT" help:"Environment variable name for PostgreSQL host:port."`
	PgDBNameEnv   string `name:"pg-dbname-env" default:"TEST_PG_DBNAME" help:"Environment variable name for PostgreSQL database name."`
}

func (c *CLI) getConnStr() string {
	user := os.Getenv(c.PgUserEnv)
	password := os.Getenv(c.PgPassEnv)
	hostport := os.Getenv(c.PgHostPortEnv)
	dbname := os.Getenv(c.PgDBNameEnv)
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, hostport, dbname)
}

func isGzippedPath(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".gz")
}

func getSourceReadCloser(_ context.Context, urlOrPath string) (io.ReadCloser, error) {
	var raw io.ReadCloser
	var isGzip bool

	if strings.HasPrefix(urlOrPath, "http://") || strings.HasPrefix(urlOrPath, "https://") {
		resp, err := http.Get(urlOrPath)
		if err != nil {
			return nil, fmt.Errorf("HTTP GET failed: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		raw = resp.Body
		isGzip = isGzippedPath(urlOrPath)
	} else {
		f, err := os.Open(urlOrPath)
		if err != nil {
			return nil, fmt.Errorf("open file failed: %w", err)
		}

		raw = f
		isGzip = isGzippedPath(urlOrPath)
	}

	if isGzip {
		gr, err := gzip.NewReader(raw)
		if err != nil {
			raw.Close()
			return nil, fmt.Errorf("gzip.NewReader failed: %w", err)
		}
		return gr, nil
	}

	return raw, nil
}

func (c *CLI) Run() error {
	if err := godotenv.Load(); err != nil {
		log.Printf("failed to load .env file: %v", err)
	}

	ctx := context.Background()

	fmt.Printf("Opening %s ...\n", c.Source)
	rc, err := getSourceReadCloser(ctx, c.Source)
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

	writer, err := pkgdb.NewPG_SQL_MRTEntries_Write_Channel(
		ctx,
		pool,
		c.Provider,
		pkgdb.WithStreamMaxReadyGenerationsAllowed(1),
	)
	if err != nil {
		return fmt.Errorf("failed to create streaming writer: %w", err)
	}

	return pkgtask.NewIngestTask(source, writer, pkgtask.WithShowProgress(true)).Run(ctx)
}

func (c *CLI) runJSONStdoutIngestTask(ctx context.Context, source io.Reader) error {
	return pkgtask.NewJSONIngestTask(source).Run(ctx)
}

func main() {
	var cli CLI
	ctx := kong.Parse(&cli)
	ctx.FatalIfErrorf(cli.Run())
}

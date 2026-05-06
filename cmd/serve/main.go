package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/alecthomas/kong"
	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
	"github.com/internetworklab/mrtparse-stream/pkg/handler"
	"github.com/internetworklab/mrtparse-stream/pkg/lister"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ServeCLI struct {
	ListenAddress string `name:"listen-address" default:":8190" help:"Address to listen on (host:port)."`

	PgUserEnv     string `name:"pg-user-env" default:"TEST_PG_USER" help:"Environment variable name for PostgreSQL user."`
	PgPassEnv     string `name:"pg-pass-env" default:"TEST_PG_PASSWORD" help:"Environment variable name for PostgreSQL password."`
	PgHostPortEnv string `name:"pg-hostport-env" default:"TEST_PG_HOSTPORT" help:"Environment variable name for PostgreSQL host:port."`
	PgDBNameEnv   string `name:"pg-dbname-env" default:"TEST_PG_DBNAME" help:"Environment variable name for PostgreSQL database name."`
}

func (cli *ServeCLI) getConnStr() string {
	user := os.Getenv(cli.PgUserEnv)
	password := os.Getenv(cli.PgPassEnv)
	hostport := os.Getenv(cli.PgHostPortEnv)
	dbname := os.Getenv(cli.PgDBNameEnv)
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, hostport, dbname)
}

func main() {
	var cli ServeCLI
	kong.Parse(&cli)

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, cli.getConnStr())
	if err != nil {
		log.Fatalf("failed to connect to database: %v", err)
	}
	defer pool.Close()

	providersReader := pkgdb.NewPGSqlProvidersReader(pool, pkgdb.WithReadyOnly(true))
	dbProvidersLister := lister.NewDBProvidersLister(providersReader)

	ln, err := net.Listen("tcp", cli.ListenAddress)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", cli.ListenAddress, err)
	}
	log.Printf("listening on %s", ln.Addr())

	mux := http.NewServeMux()

	providersHandler := &handler.ProvidersQueryHandler{
		ProvidersLister: dbProvidersLister,
	}
	mux.Handle("/providers", providersHandler)

	srv := &http.Server{
		Handler: mux,
	}

	if err := srv.Serve(ln); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

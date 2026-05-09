package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/alecthomas/kong"
	cloudpingauth "github.com/internetworklab/cloudping/pkg/auth"
	cloudpingcli "github.com/internetworklab/cloudping/pkg/cli"
	cloudpingandler "github.com/internetworklab/cloudping/pkg/handler"
	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
	"github.com/internetworklab/mrtparse-stream/pkg/handler"
	"github.com/internetworklab/mrtparse-stream/pkg/lister"
	pkgutils "github.com/internetworklab/mrtparse-stream/pkg/utils"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type ServeCLI struct {
	ListenAddress  string                            `name:"listen-address" default:":8190" help:"Address to listen on (host:port)."`
	TablePrefix    string                            `name:"table-prefix" default:"mrt_entries" help:"Table name prefix for per-generation MRT entries tables."`
	PgUserEnv      string                            `name:"pg-user-env" default:"TEST_PG_USER" help:"Environment variable name for PostgreSQL user."`
	PgPassEnv      string                            `name:"pg-pass-env" default:"TEST_PG_PASSWORD" help:"Environment variable name for PostgreSQL password."`
	PgHostPortEnv  string                            `name:"pg-hostport-env" default:"TEST_PG_HOSTPORT" help:"Environment variable name for PostgreSQL host:port."`
	PgDBNameEnv    string                            `name:"pg-dbname-env" default:"TEST_PG_DBNAME" help:"Environment variable name for PostgreSQL database name."`
	Authentication cloudpingcli.AuthenticationMethod `name:"authentication" help:"Specify the authentication method to use, currently supported auth methods are: 'none', 'jwt'. For 'jwt' authentication, attach the jwt token in the Authorization header as 'Authorization: bearer <jwt>'" default:"none"`

	// For authenticate client's request
	JWTAuthSecretFromEnv  string `name:"jwt-auth-secret-from-env" help:"Name of the environment variable that contains the JWT secret"`
	JWTAuthSecretFromFile string `name:"jwt-auth-secret-from-file" help:"Path to the file that contains the JWT secret"`
}

func getJWTSecFromSomewhere(envVar string, filePath string) ([]byte, error) {
	if envVar != "" {
		secret := os.Getenv(envVar)
		if secret == "" {
			return nil, fmt.Errorf("JWT secret is not set in environment variable %s", envVar)
		}
		return []byte(secret), nil
	}

	if filePath != "" {
		secret, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read JWT secret file %s: %v", filePath, err)
		}
		if len(secret) == 0 {
			return nil, fmt.Errorf("JWT secret file %s is empty", filePath)
		}
		return secret, nil
	}

	return nil, fmt.Errorf("no JWT secret is set")
}

func (cli *ServeCLI) getSecret() ([]byte, error) {
	return getJWTSecFromSomewhere(cli.JWTAuthSecretFromEnv, cli.JWTAuthSecretFromFile)
}

func (cli *ServeCLI) getConnStr() string {
	user := os.Getenv(cli.PgUserEnv)
	password := os.Getenv(cli.PgPassEnv)
	hostport := os.Getenv(cli.PgHostPortEnv)
	dbname := os.Getenv(cli.PgDBNameEnv)
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, hostport, dbname)
}

func (cli *ServeCLI) Run() error {
	ctx := context.Background()

	logger := log.New(os.Stderr, "", log.LstdFlags)
	ctx = context.WithValue(ctx, pkgutils.CtxKeyLogger, logger)

	if err := godotenv.Load(); err != nil {
		logger.Printf("failed to load .env file: %v", err)
	}

	pool, err := pgxpool.New(ctx, cli.getConnStr())
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	providersReader := pkgdb.NewPGSqlProvidersReader(pool, pkgdb.WithReadyOnly(true))
	dbProvidersLister := lister.NewDBProvidersLister(providersReader)

	ln, err := net.Listen("tcp", cli.ListenAddress)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", cli.ListenAddress, err)
	}
	logger.Printf("listening on %s", ln.Addr())

	mux := http.NewServeMux()

	providersHandler := &handler.ProvidersQueryHandler{
		ProvidersLister: dbProvidersLister,
	}
	mux.Handle("/providers", providersHandler)

	tableBuilder, err := pkgdb.NewMRTEntriesTableBuilder(pool, cli.TablePrefix)
	if err != nil {
		return fmt.Errorf("failed to create table builder: %w", err)
	}

	mrtEntriesRW, err := pkgdb.NewPgSqlMRTEntriesReadWriter(pool, tableBuilder)
	if err != nil {
		return fmt.Errorf("failed to create MRT entries reader: %w", err)
	}

	mrtEntriesHandler := &handler.MRTEntriesQueryHandler{
		Querier: mrtEntriesRW,
	}
	mux.Handle("/mrt_entries/query/{provider}", mrtEntriesHandler)

	counterHandler := handler.NewCounterHandler()
	mux.Handle("/counter", counterHandler)

	var muxHandler http.Handler = mux

	if cli.Authentication == cloudpingcli.AuthenticationMethodJWT {
		secret, err := cli.getSecret()
		if err != nil {
			log.Fatalf("failed to load secret while authentication is not 'none': %v", err)
			return err
		}
		validator := cloudpingauth.NewStaticKeyJWTValidator(
			cloudpingauth.NewStaticSecretProvider(secret),
			cloudpingauth.NewNullBlackListProvider(),
		)
		muxHandler = cloudpingandler.WithJWTAuth(muxHandler, validator, nil)
	}

	srv := &http.Server{
		Handler: muxHandler,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived signal, shutting down...")
		srv.Close()
		cancel()
	}()

	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("server error: %w", err)
	}
	return nil
}

func main() {
	var cli ServeCLI
	ctx := kong.Parse(&cli)
	ctx.FatalIfErrorf(cli.Run())
}

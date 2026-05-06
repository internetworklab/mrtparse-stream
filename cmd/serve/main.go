package main

import (
	"log"
	"net"
	"net/http"

	"github.com/alecthomas/kong"
	"github.com/internetworklab/mrtparse-stream/pkg/handler"
	"github.com/internetworklab/mrtparse-stream/pkg/lister"
)

type ServeCLI struct {
	ListenAddress string `name:"listen-address" default:":8190" help:"Address to listen on (host:port)."`

	PgUserEnv     string `name:"pg-user-env" default:"TEST_PG_USER" help:"Environment variable name for PostgreSQL user."`
	PgPassEnv     string `name:"pg-pass-env" default:"TEST_PG_PASSWORD" help:"Environment variable name for PostgreSQL password."`
	PgHostPortEnv string `name:"pg-hostport-env" default:"TEST_PG_HOSTPORT" help:"Environment variable name for PostgreSQL host:port."`
	PgDBNameEnv   string `name:"pg-dbname-env" default:"TEST_PG_DBNAME" help:"Environment variable name for PostgreSQL database name."`
}

func main() {
	var cli ServeCLI
	kong.Parse(&cli)

	ln, err := net.Listen("tcp", cli.ListenAddress)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", cli.ListenAddress, err)
	}
	log.Printf("listening on %s", ln.Addr())

	mux := http.NewServeMux()

	providersHandler := &handler.ProvidersQueryHandler{
		ProvidersLister: &lister.MockProvidersLister{},
	}
	mux.Handle("/providers", providersHandler)

	srv := &http.Server{
		Handler: mux,
	}

	if err := srv.Serve(ln); err != nil {
		log.Fatalf("server error: %v", err)
	}
}

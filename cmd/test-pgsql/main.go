package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

const (
	defaultProvider              = "ripe-ris"
	defaultMRTEntriesTablePrefix = "mrt_entries"
)

func testWrite(ctx context.Context, readWriter pkgdb.MRTEntriesReadWriter, provider string) {
	// 构造示例 MRTEntry
	entries := pkgmodel.GetExampleMRTEntries()

	if err := readWriter.WriteMRTEntries(ctx, entries, provider); err != nil {
		log.Fatalf("failed to write records to db: %v", err)
	}

	// 拉取该 generation 全量数据并做 round-trip 校验
	if pgSqlReadWriter, ok := readWriter.(*pkgdb.PG_SQL_MRTEntriesReadWriter); ok {
		var fetched []*pkgmodel.MRTEntry
		genCh := pgSqlReadWriter.GetAllMRTEntries(ctx, provider)
		for evt := range genCh {
			if evt.Err != nil {
				log.Fatalf("fetch by generation failed: %v", evt.Err)
			}
			fetched = append(fetched, evt.Data)
		}
		if !pkgmodel.IsDeepEqual(entries, fetched) {
			log.Fatalf("round-trip check failed: original and fetched entries are not equal")
		}
		fmt.Printf("Round-trip check passed: %d entries verified\n", len(fetched))
	}
}

func testRead(ctx context.Context, reader pkgdb.MRTEntriesReader, provider string) {

	targetIP := net.ParseIP("192.168.1.100")
	fmt.Printf("\n--- Lookup IP %s in prefixes ---\n", targetIP.String())
	ipCh := reader.GetMRTEntriesByIP(ctx, targetIP, provider)
	for evt := range ipCh {
		if evt.Err != nil {
			log.Fatalf("prefix lookup failed: %v", evt.Err)
		}
		fmt.Printf("Matched: prefix=%s peer=%s peer_as=%d as_path=%v\n", evt.Data.Prefix.String(), evt.Data.Peer, evt.Data.PeerAS, evt.Data.ASPath)
	}

	_, targetCIDR, _ := net.ParseCIDR("192.168.1.128/28")
	fmt.Printf("\n--- Lookup CIDR %s in prefixes ---\n", targetCIDR.String())
	cidrCh := reader.GetMRTEntriesByCIDR(ctx, *targetCIDR, provider)
	for evt := range cidrCh {
		if evt.Err != nil {
			log.Fatalf("cidr subset lookup failed: %v", evt.Err)
		}
		fmt.Printf("Matched: prefix=%s peer=%s peer_as=%d as_path=%v\n", evt.Data.Prefix.String(), evt.Data.Peer, evt.Data.PeerAS, evt.Data.ASPath)
	}

	originAS := uint32(64565)
	fmt.Printf("\n--- Lookup Origin AS %d ---\n", originAS)
	originCh := reader.GetMRTEntriesByOriginAS(ctx, originAS, provider)
	for evt := range originCh {
		if evt.Err != nil {
			log.Fatalf("origin as lookup failed: %v", evt.Err)
		}
		fmt.Printf("Matched: prefix=%s peer=%s peer_as=%d as_path=%v\n", evt.Data.Prefix.String(), evt.Data.Peer, evt.Data.PeerAS, evt.Data.ASPath)
	}

	neighborAS := uint32(64514)
	fmt.Printf("\n--- Lookup Neighbor AS %d ---\n", neighborAS)
	neighborCh := reader.GetMRTEntriesByNeighborAS(ctx, neighborAS, provider)
	for evt := range neighborCh {
		if evt.Err != nil {
			log.Fatalf("neighbor as lookup failed: %v", evt.Err)
		}
		fmt.Printf("Matched: prefix=%s peer=%s peer_as=%d as_path=%v\n", evt.Data.Prefix.String(), evt.Data.Peer, evt.Data.PeerAS, evt.Data.ASPath)
	}

	asSegments := []uint32{64515, 64516}
	fmt.Printf("\n--- Lookup AS Segments %v ---\n", asSegments)
	segCh := reader.GetMRTEntriesByASSegments(ctx, asSegments, provider)
	for evt := range segCh {
		if evt.Err != nil {
			log.Fatalf("as segments lookup failed: %v", evt.Err)
		}
		fmt.Printf("Matched: prefix=%s peer=%s peer_as=%d as_path=%v\n", evt.Data.Prefix.String(), evt.Data.Peer, evt.Data.PeerAS, evt.Data.ASPath)
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Printf("failed to load .env file: %v", err)
	}

	ctx := context.Background()

	user := os.Getenv("TEST_PG_USER")
	password := os.Getenv("TEST_PG_PASSWORD")
	pghost := os.Getenv("TEST_PG_HOSTPORT")
	pgdbname := os.Getenv("TEST_PG_DBNAME")
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, password, pghost, pgdbname)

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	tableBuilder, err := pkgdb.NewMRTEntriesTableBuilder(pool, defaultMRTEntriesTablePrefix)
	if err != nil {
		log.Fatalf("failed to create table builder: %v", err)
	}

	pgReadWriter, err := pkgdb.NewPgSqlMRTEntriesReadWriter(
		pool,
		tableBuilder,
		pkgdb.WithMaxReadyGenerationsAllowed(1),
	)
	if err != nil {
		log.Fatalf("failed to create mrt_entries readwriter: %v", err)
	}

	testWrite(ctx, pgReadWriter, defaultProvider)
	testRead(ctx, pgReadWriter, defaultProvider)
}

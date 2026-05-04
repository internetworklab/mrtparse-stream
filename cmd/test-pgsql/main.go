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

const defaultProvider = "ripe-ris"

func testWrite(ctx context.Context, readWriter pkgdb.MRTEntriesReadWriter) {
	// 构造示例 MRTEntry
	entries := pkgmodel.GetExampleMRTEntries()

	if err := readWriter.WriteMRTEntries(ctx, entries); err != nil {
		log.Fatalf("failed to write records to db: %v", err)
	}

	// 独立获取当前最新的 ready generation（与插入阶段无关）
	latestReadyGen, err := readWriter.GetLatestReadyGen(ctx)
	if err != nil {
		log.Fatalf("no ready generation found: %v", err)
	}
	fmt.Printf("\n--- Latest ready generation: %d ---\n", latestReadyGen)

	// 拉取该 generation 全量数据并做 round-trip 校验
	var fetched []*pkgmodel.MRTEntry
	genCh := readWriter.GetAllMRTEntries(ctx)
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

func testRead(ctx context.Context, reader pkgdb.MRTEntriesReader) {

	// 独立获取当前最新的 ready generation（与插入阶段无关）
	latestReadyGen, err := reader.GetLatestReadyGen(ctx)
	if err != nil {
		log.Fatalf("no ready generation found: %v", err)
	}
	fmt.Printf("\n--- Latest ready generation: %d ---\n", latestReadyGen)

	targetIP := net.ParseIP("192.168.1.100")
	fmt.Printf("\n--- Lookup IP %s in prefixes ---\n", targetIP.String())
	ipCh := reader.GetMRTEntriesByIP(ctx, targetIP)
	for evt := range ipCh {
		if evt.Err != nil {
			log.Fatalf("prefix lookup failed: %v", evt.Err)
		}
		fmt.Printf("Matched: prefix=%s peer=%s peer_as=%d as_path=%v\n", evt.Data.Prefix.String(), evt.Data.Peer, evt.Data.PeerAS, evt.Data.ASPath)
	}

	_, targetCIDR, _ := net.ParseCIDR("192.168.1.128/28")
	fmt.Printf("\n--- Lookup CIDR %s in prefixes ---\n", targetCIDR.String())
	cidrCh := reader.GetMRTEntriesByCIDR(ctx, *targetCIDR)
	for evt := range cidrCh {
		if evt.Err != nil {
			log.Fatalf("cidr subset lookup failed: %v", evt.Err)
		}
		fmt.Printf("Matched: prefix=%s peer=%s peer_as=%d as_path=%v\n", evt.Data.Prefix.String(), evt.Data.Peer, evt.Data.PeerAS, evt.Data.ASPath)
	}

	originAS := uint32(64565)
	fmt.Printf("\n--- Lookup Origin AS %d ---\n", originAS)
	originCh := reader.GetMRTEntriesByOriginAS(ctx, originAS)
	for evt := range originCh {
		if evt.Err != nil {
			log.Fatalf("origin as lookup failed: %v", evt.Err)
		}
		fmt.Printf("Matched: prefix=%s peer=%s peer_as=%d as_path=%v\n", evt.Data.Prefix.String(), evt.Data.Peer, evt.Data.PeerAS, evt.Data.ASPath)
	}

	neighborAS := uint32(64514)
	fmt.Printf("\n--- Lookup Neighbor AS %d ---\n", neighborAS)
	neighborCh := reader.GetMRTEntriesByNeighborAS(ctx, neighborAS)
	for evt := range neighborCh {
		if evt.Err != nil {
			log.Fatalf("neighbor as lookup failed: %v", evt.Err)
		}
		fmt.Printf("Matched: prefix=%s peer=%s peer_as=%d as_path=%v\n", evt.Data.Prefix.String(), evt.Data.Peer, evt.Data.PeerAS, evt.Data.ASPath)
	}

	asSegments := []uint32{64515, 64516}
	fmt.Printf("\n--- Lookup AS Segments %v ---\n", asSegments)
	segCh := reader.GetMRTEntriesByASSegments(ctx, asSegments)
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

	provider := defaultProvider

	pgReadWriter, err := pkgdb.NewPgSqlMRTEntriesReadWriter(
		pool,
		provider,
		pkgdb.WithMaxReadyGenerationsAllowed(1),
	)
	if err != nil {
		log.Fatalf("failed to create mrt_entries readwriter: %v", err)
	}

	testWrite(ctx, pgReadWriter)
	testRead(ctx, pgReadWriter)
}

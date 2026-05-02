package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/internetworklab/mrtparse-stream/pkg/parse"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

func normalizeIP(ip net.IP) net.IP {
	if v4 := ip.To4(); v4 != nil {
		return v4 // 返回 4 字节形式，不是 ::ffff:...
	}
	return ip
}

const defaultProvider = "ripe-ris"
const maxReadyGenerationsAllowed = 1

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

	// 构造示例 MRTEntry
	entries := []*parse.MRTEntry{
		{
			Prefix: mustParseCIDR("192.168.1.0/24"),
			Peer:   net.ParseIP("10.0.0.1"),
			PeerAS: 64512,
			ASPath: []uint32{64512, 64513},
		},
		{
			Prefix: mustParseCIDR("10.0.0.0/8"),
			Peer:   net.ParseIP("10.0.0.2"),
			PeerAS: 64514,
			ASPath: []uint32{64514, 64515, 64516},
		},
		{
			Prefix: mustParseCIDR("172.16.5.0/24"),
			Peer:   net.ParseIP("10.0.0.3"),
			PeerAS: 64517,
			ASPath: []uint32{64517},
		},
	}

	// 创建一个新的 generation
	var generationID int
	err = pool.QueryRow(ctx, `INSERT INTO generations DEFAULT VALUES RETURNING id`).Scan(&generationID)
	if err != nil {
		log.Fatalf("create generation failed: %v", err)
	}
	fmt.Printf("Created generation id=%d\n", generationID)

	// 插入 mrt_entries
	for _, e := range entries {
		_, err := pool.Exec(ctx,
			`INSERT INTO mrt_entries (generation, source, prefix, peer, peer_as, as_path) VALUES ($1, $2, $3, $4, $5, $6)`,
			generationID, defaultProvider, e.Prefix.String(), normalizeIP(e.Peer), int64(e.PeerAS), uint32SliceToInt64(e.ASPath),
		)
		if err != nil {
			log.Fatalf("insert failed: %v", err)
		}
	}
	fmt.Println("Inserted", len(entries), "entries")

	// 将 generation 状态更新为 ready
	_, err = pool.Exec(ctx,
		`UPDATE generations SET status = 'ready' WHERE id = $1`,
		generationID,
	)
	if err != nil {
		log.Fatalf("update generation status failed: %v", err)
	}
	fmt.Println("Generation", generationID, "status set to ready")

	// 循环清理过期的 ready generation，直到数量符合限制
	for {
		var readyCount int
		err = pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM generations WHERE status = 'ready'`,
		).Scan(&readyCount)
		if err != nil {
			log.Fatalf("count ready generations failed: %v", err)
		}
		fmt.Printf("Current ready generations: %d (max allowed: %d)\n", readyCount, maxReadyGenerationsAllowed)

		if readyCount > maxReadyGenerationsAllowed {
			var oldestGenID int
			err = pool.QueryRow(ctx,
				`SELECT id FROM generations WHERE status = 'ready' ORDER BY id ASC LIMIT 1`,
			).Scan(&oldestGenID)
			if err != nil {
				log.Fatalf("find oldest ready generation failed: %v", err)
			}

			_, err = pool.Exec(ctx, `DELETE FROM mrt_entries WHERE generation = $1`, oldestGenID)
			if err != nil {
				log.Fatalf("delete mrt_entries for generation %d failed: %v", oldestGenID, err)
			}

			_, err = pool.Exec(ctx, `DELETE FROM generations WHERE id = $1`, oldestGenID)
			if err != nil {
				log.Fatalf("delete generation %d failed: %v", oldestGenID, err)
			}

			fmt.Printf("Deleted oldest ready generation %d and its mrt_entries\n", oldestGenID)
		} else {
			break
		}
	}

	// 独立获取当前最新的 ready generation（与插入阶段无关）
	var latestReadyGen int
	err = pool.QueryRow(ctx,
		`SELECT id FROM generations WHERE status = 'ready' ORDER BY id DESC LIMIT 1`,
	).Scan(&latestReadyGen)
	if err != nil {
		log.Fatalf("no ready generation found: %v", err)
	}
	fmt.Printf("\n--- Latest ready generation: %d ---\n", latestReadyGen)

	// Select all（限定在最新 ready generation）
	rows, err := pool.Query(ctx,
		`SELECT id, generation, source, prefix, peer, peer_as, as_path FROM mrt_entries WHERE generation = $1`,
		latestReadyGen,
	)
	if err != nil {
		log.Fatalf("select all failed: %v", err)
	}
	defer rows.Close()

	fmt.Println("--- All entries ---")
	for rows.Next() {
		var id int64
		var generation int
		var source string
		var prefix net.IPNet
		var peer net.IP
		var peerAS int64
		var asPath []int64
		if err := rows.Scan(&id, &generation, &source, &prefix, &peer, &peerAS, &asPath); err != nil {
			log.Fatalf("scan failed: %v", err)
		}
		fmt.Printf("id=%d generation=%d source=%s prefix=%s peer=%s peer_as=%d as_path=%v\n", id, generation, source, prefix.String(), peer, peerAS, asPath)
	}

	// 开始模拟查询情景，与上述插入情景无关。

	// 利用 cidr >> inet 查询 IP 归属（GiST 索引）
	targetIP := net.ParseIP("192.168.1.100")
	fmt.Printf("\n--- Lookup IP %s in prefixes ---\n", targetIP.String())
	rows2, err := pool.Query(ctx,
		`SELECT id, generation, source, prefix, peer, peer_as, as_path FROM mrt_entries WHERE prefix >> $1 AND generation = $2`,
		normalizeIP(targetIP), latestReadyGen,
	)
	if err != nil {
		log.Fatalf("prefix lookup failed: %v", err)
	}
	defer rows2.Close()

	for rows2.Next() {
		var id int64
		var generation int
		var source string
		var prefix net.IPNet
		var peer net.IP
		var peerAS int64
		var asPath []int64
		if err := rows2.Scan(&id, &generation, &source, &prefix, &peer, &peerAS, &asPath); err != nil {
			log.Fatalf("scan failed: %v", err)
		}
		fmt.Printf("Matched: id=%d generation=%d source=%s prefix=%s peer=%s peer_as=%d as_path=%v\n", id, generation, source, prefix.String(), peer, peerAS, asPath)
	}

	_, targetCIDR, _ := net.ParseCIDR("192.168.1.128/28")
	fmt.Printf("\n--- Lookup CIDR %s in prefixes ---\n", targetCIDR.String())
	rows3, err := pool.Query(ctx,
		`SELECT id, generation, source, prefix, peer, peer_as, as_path FROM mrt_entries WHERE prefix >>= $1 AND generation = $2`,
		*targetCIDR, latestReadyGen,
	)
	if err != nil {
		log.Fatalf("cidr subset lookup failed: %v", err)
	}
	defer rows3.Close()

	for rows3.Next() {
		var id int64
		var generation int
		var source string
		var prefix net.IPNet
		var peer net.IP
		var peerAS int64
		var asPath []int64
		if err := rows3.Scan(&id, &generation, &source, &prefix, &peer, &peerAS, &asPath); err != nil {
			log.Fatalf("scan failed: %v", err)
		}
		fmt.Printf("Matched: id=%d generation=%d source=%s prefix=%s peer=%s peer_as=%d as_path=%v\n", id, generation, source, prefix.String(), peer, peerAS, asPath)
	}
}

func mustParseCIDR(s string) net.IPNet {
	_, ipNet, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	return *ipNet
}

func uint32SliceToInt64(in []uint32) []int64 {
	out := make([]int64, len(in))
	for i, v := range in {
		out[i] = int64(v)
	}
	return out
}

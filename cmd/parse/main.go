package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pkgparse "github.com/internetworklab/mrtparse-stream/pkg/parse"
)

// Hint: in actual use, better fully download the gzip ball before start to parse it, the server have imposed the
// constraint regarding the duration of tcp connection, they will cut off the long-running tcp connection whether
// there is ongoing data transfer or not.
func main() {

	url := "https://data.ris.ripe.net/rrc00/2026.05/bview.20260502.1600.gz"

	fmt.Printf("Fetching %s ...\n", url)
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		panic(fmt.Sprintf("HTTP %d", resp.StatusCode))
	}

	fmt.Println("Pipeline: HTTP -> gzip.Reader -> MRTParser")

	gr, err := gzip.NewReader(resp.Body)
	if err != nil {
		panic(err)
	}
	defer gr.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 处理 Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nReceived signal, shutting down...")
		cancel()
	}()

	parser := pkgparse.NewMRTParser(gr)
	parser.Run(ctx)

	count := 0
	for {
		entry, err := parser.ReadEntry(ctx)
		if err != nil {
			fmt.Printf("ReadEntry finished: %v\n", err)
			break
		}

		asPathStr := "-"
		if len(entry.ASPath) > 0 {
			var sb strings.Builder
			for i, asn := range entry.ASPath {
				if i > 0 {
					sb.WriteString(" ")
				}
				fmt.Fprintf(&sb, "%d", asn)
			}
			asPathStr = sb.String()
		}
		fmt.Printf("[%05d] prefix=%-18s peer=%-15s peerAS=%-6d AS_PATH=%s\n",
			count+1, entry.Prefix.String(), entry.Peer.String(), entry.PeerAS, asPathStr)

		count++
		if count >= 10000 {
			fmt.Println("Reached 10000 entries, exiting.")
			cancel()
			break
		}

		time.Sleep(30 * time.Millisecond)
	}
}

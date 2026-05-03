package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
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

	parser := pkgmodel.NewMRTParser(gr)
	parser.Run(ctx)

	count := 0
	for {
		entry, err := parser.ReadEntry(ctx)
		if err != nil {
			fmt.Printf("ReadEntry finished: %v\n", err)
			break
		}

		fmt.Printf("[%05d]\n", count+1)
		fmt.Print(entry.PrettyString())
		fmt.Println()

		count++
		if count >= 10000 {
			fmt.Println("Reached 10000 entries, exiting.")
			cancel()
			break
		}

		time.Sleep(30 * time.Millisecond)
	}
}

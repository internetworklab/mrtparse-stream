package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"net/http"
	"time"

	"github.com/osrg/gobgp/v3/pkg/packet/mrt"
)

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

	fmt.Println("Pipeline: HTTP body -> gzip.Reader -> bufio.Scanner -> mrt.SplitMrt")

	// 2. gzip 解压器把 gzip 流转为 MRT 字节流
	gr, err := gzip.NewReader(resp.Body)
	if err != nil {
		panic(err)
	}
	defer gr.Close()

	// 3. 创建 Scanner，Split 函数设为 mrt.SplitMrt
	scanner := bufio.NewScanner(gr)
	scanner.Split(mrt.SplitMrt)

	// bview 里的 RIB 记录可能较大，默认 64K token 上限不够安全，提升到 1MB
	const maxCapacity = 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxCapacity)

	// 4. 循环扫描、解析、打印、睡眠 300ms，满 100 条退出
	count := 0
	for scanner.Scan() {
		raw := scanner.Bytes()

		msg, ts, err := parseRawRecord(raw)
		if err != nil {
			fmt.Printf("[%03d] [PARSE ERROR] %v\n", count+1, err)
		} else {
			printMessage(msg, ts, count+1)
		}

		count++
		if count >= 100 {
			fmt.Println("Reached 100 records, exiting.")
			break
		}

		time.Sleep(300 * time.Millisecond)
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("[SCANNER ERROR] %v\n", err)
	}
}

// parseRawRecord 把一条完整的 MRT 记录（header + body）解析为 MRTMessage，
// 并顺手把 header 里的 Unix timestamp 吐出来，避免后续再从 msg 里反射取值。
func parseRawRecord(raw []byte) (*mrt.MRTMessage, uint32, error) {
	if len(raw) < 12 {
		return nil, 0, fmt.Errorf("record too short: %d bytes", len(raw))
	}

	// RFC 6396: bytes 0-3 of header = timestamp (big-endian)
	ts := binary.BigEndian.Uint32(raw[0:4])

	h := &mrt.MRTHeader{}
	if err := h.DecodeFromBytes(raw[:12]); err != nil {
		return nil, 0, fmt.Errorf("decode header: %w", err)
	}

	msg, err := mrt.ParseMRTBody(h, raw[12:])
	if err != nil {
		return nil, ts, fmt.Errorf("parse body: %w", err)
	}

	return msg, ts, nil
}

// printMessage 根据 Body 的实际类型打印基本信息
func printMessage(msg *mrt.MRTMessage, ts uint32, idx int) {
	t := time.Unix(int64(ts), 0).UTC().Format("2006-01-02T15:04:05Z")

	switch body := msg.Body.(type) {
	case *mrt.BGP4MPMessage:
		fmt.Printf("[%03d] BGP4MPMessage  ts=%s peer=%s peerAS=%d\n",
			idx, t, body.PeerIpAddress, body.PeerAS)

	case *mrt.BGP4MPStateChange:
		fmt.Printf("[%03d] BGP4MPStateChg ts=%s peer=%s oldState=%d newState=%d\n",
			idx, t, body.PeerIpAddress, body.OldState, body.NewState)

	case *mrt.PeerIndexTable:
		fmt.Printf("[%03d] PeerIndexTable ts=%s collector=%s peers=%d\n",
			idx, t, body.CollectorBgpId, len(body.Peers))

	case *mrt.Rib:
		prefix := "-"
		if body.Prefix != nil {
			prefix = body.Prefix.String()
		}
		fmt.Printf("[%03d] Rib            ts=%s prefix=%s entries=%d\n",
			idx, t, prefix, len(body.Entries))

	case *mrt.GeoPeerTable:
		fmt.Printf("[%03d] GeoPeerTable   ts=%s collector=%s peers=%d\n",
			idx, t, body.CollectorBgpId, len(body.Peers))

	default:
		fmt.Printf("[%03d] Unknown(%T)    ts=%s\n", idx, body, t)
	}
}


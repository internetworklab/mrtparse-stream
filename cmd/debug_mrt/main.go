package main

import (
	"fmt"
	"os"

	"github.com/osrg/gobgp/v3/pkg/packet/mrt"
)

func main() {
	data, _ := os.ReadFile(os.Args[1])
	fmt.Printf("File size: %d\n", len(data))

	// Record 1
	h := &mrt.MRTHeader{}
	h.DecodeFromBytes(data[:12])
	msg, err := mrt.ParseMRTBody(h, data[12:12+int(h.Len)])
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	pit, ok := msg.Body.(*mrt.PeerIndexTable)
	if !ok {
		fmt.Printf("Expected PeerIndexTable, got %T\n", msg.Body)
		return
	}
	fmt.Printf("Peers: %d\n", len(pit.Peers))
	for i, p := range pit.Peers {
		fmt.Printf("  %d: ip=%v as=%d\n", i, p.IpAddress, p.AS)
	}

	// Record 2
	off := 12 + int(h.Len)
	h2 := &mrt.MRTHeader{}
	h2.DecodeFromBytes(data[off : off+12])
	msg2, err := mrt.ParseMRTBody(h2, data[off+12:off+12+int(h2.Len)])
	if err != nil {
		fmt.Printf("Error rec2: %v\n", err)
		return
	}
	rib, ok := msg2.Body.(*mrt.Rib)
	if !ok {
		fmt.Printf("Expected Rib, got %T\n", msg2.Body)
		return
	}
	fmt.Printf("\nRec 2: subtype=%d prefix=%v entries=%d\n", h2.SubType, rib.Prefix, len(rib.Entries))
	for i, e := range rib.Entries {
		if i >= 5 {
			break
		}
		fmt.Printf("  %d: peerIdx=%d originated=%d pathId=%d\n", i, e.PeerIndex, e.OriginatedTime, e.PathIdentifier)
	}
}

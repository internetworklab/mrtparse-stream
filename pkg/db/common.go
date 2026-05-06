package db

import (
	"fmt"
	"net"
	"regexp"
)

// mrtEntryChannelBufferSize is the buffer size for channels returned by MRT entry query methods.
// A buffered channel allows the query goroutine to prefetch rows while the consumer
// is processing the previous batch, reducing round-trip latency between DB reads and
// application-level consumption.
const mrtEntryChannelBufferSize = 16

var rfc1035LabelAugmented = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-_]{0,61}[a-zA-Z0-9])?$`)

func sanitizeString(s string) error {
	if len(s) < 1 || len(s) > 63 {
		return fmt.Errorf("invalid RFC1035 label %q: length must be 1-63", s)
	}
	if !rfc1035LabelAugmented.MatchString(s) {
		return fmt.Errorf("invalid RFC1035 label %q: must contain only ASCII letters, digits, and hyphens, and cannot start or end with a hyphen", s)
	}
	return nil
}

func prefixLen(ipNet net.IPNet) int {
	ones, _ := ipNet.Mask.Size()
	return ones
}

func normalizeIP(ip net.IP) net.IP {
	if ip == nil {
		return nil
	}
	if v4 := ip.To4(); v4 != nil {
		return v4 // 返回 4 字节形式，不是 ::ffff:...
	}
	return ip
}

func uint32SliceToInt32(in []uint32) []int32 {
	out := make([]int32, len(in))
	for i, v := range in {
		out[i] = int32(v)
	}
	return out
}

func communityHigh(in []uint32) []int32 {
	out := make([]int32, len(in))
	for i, v := range in {
		out[i] = int32(v >> 16)
	}
	return out
}

func communityLow(in []uint32) []int32 {
	out := make([]int32, len(in))
	for i, v := range in {
		out[i] = int32(v & 0xFFFF)
	}
	return out
}

func largeCommunityHigh(in [][3]uint32) []int32 {
	out := make([]int32, len(in))
	for i, v := range in {
		out[i] = int32(v[0])
	}
	return out
}

func largeCommunityMid(in [][3]uint32) []int32 {
	out := make([]int32, len(in))
	for i, v := range in {
		out[i] = int32(v[1])
	}
	return out
}

func largeCommunityLow(in [][3]uint32) []int32 {
	out := make([]int32, len(in))
	for i, v := range in {
		out[i] = int32(v[2])
	}
	return out
}

func int32SliceToUint32(in []int32) []uint32 {
	out := make([]uint32, len(in))
	for i, v := range in {
		out[i] = uint32(v)
	}
	return out
}

func extendedCommunityHigh(in []uint64) []int32 {
	out := make([]int32, len(in))
	for i, v := range in {
		out[i] = int32(v >> 32)
	}
	return out
}

func extendedCommunityLow(in []uint64) []int32 {
	out := make([]int32, len(in))
	for i, v := range in {
		out[i] = int32(v & 0xFFFFFFFF)
	}
	return out
}

func zipCommunities(high, low []int32) []uint32 {
	n := min(len(high), len(low))
	out := make([]uint32, n)
	for i := range n {
		out[i] = uint32(high[i])<<16 | uint32(low[i])
	}
	return out
}

func zipExtendedCommunities(high, low []int32) []uint64 {
	n := min(len(high), len(low))
	out := make([]uint64, n)
	for i := range n {
		out[i] = uint64(uint32(high[i]))<<32 | uint64(uint32(low[i]))
	}
	return out
}

func zipLargeCommunities(high, mid, low []int32) [][3]uint32 {
	n := min(len(high), len(mid), len(low))
	out := make([][3]uint32, n)
	for i := range n {
		out[i] = [3]uint32{uint32(high[i]), uint32(mid[i]), uint32(low[i])}
	}
	return out
}

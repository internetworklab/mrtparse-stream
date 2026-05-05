package model

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"

	"github.com/osrg/gobgp/v3/pkg/packet/bgp"
	"github.com/osrg/gobgp/v3/pkg/packet/mrt"
)

// MRTEntry 是从 MRT 消息中提取的一条路由条目
type MRTEntry struct {
	Prefix net.IPNet
	Peer   net.IP

	PeerAS uint32

	ASPath []uint32

	// Communities 对应标准 BGP Community (RFC 1997)，gobgp 原始类型为 []uint32
	Communities []uint32

	// LargeCommunities 对应 BGP Large Community (RFC 8092)，每个元素为 [GlobalAdmin, LocalData1, LocalData2]
	LargeCommunities [][3]uint32

	// ExtendedCommunities 对应 BGP Extended Community (RFC 4360)。
	// 标准 Extended Community 在报文中为 8 字节，这里按大端序打包为 uint64 存储。
	// 非 8 字节的变体（如 rfc5701 IPv6 Address Specific，20 字节）会被丢弃。
	// 如需保留所有变体，请改用 [][]byte 并调用 v.Serialize()。
	ExtendedCommunities []uint64
}

func (u MRTEntry) MarshalJSON() ([]byte, error) {
	// 1. Create a type alias to avoid recursion
	type MRTEntryAlias MRTEntry

	peerStr := ""
	if ip4 := u.Peer.To4(); ip4 != nil {
		peerStr = ip4.String()
	} else {
		peerStr = u.Peer.To16().String()
	}

	// 2. Wrap the alias in a new struct and override the field
	return json.Marshal(&struct {
		*MRTEntryAlias
		Prefix string
		Peer   string
	}{
		Prefix:        u.Prefix.String(),
		Peer:          peerStr,
		MRTEntryAlias: (*MRTEntryAlias)(&u),
	})
}

func IsDeepEqual(a, b []*MRTEntry) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	unvisitedSet := make(map[int]*MRTEntry)
	for i, x := range b {
		unvisitedSet[i] = x
	}

	for _, xa := range a {
		matched := false
		var matchedIdx int
		for i, xb := range unvisitedSet {
			if xa.IsDeepEqual(xb) {
				matched = true
				matchedIdx = i
				break
			}
		}
		if !matched {
			return false
		} else {
			delete(unvisitedSet, matchedIdx)
		}
	}
	return true
}

func (e *MRTEntry) IsDeepEqual(other *MRTEntry) bool {
	if e == nil || other == nil {
		return e == other
	}

	if e.Prefix.String() != other.Prefix.String() {
		return false
	}

	if !peerEqual(e.Peer, other.Peer) {
		return false
	}

	if e.PeerAS != other.PeerAS {
		return false
	}

	if !uint32SliceEqual(e.ASPath, other.ASPath) {
		return false
	}

	if !uint32SliceEqual(e.Communities, other.Communities) {
		return false
	}

	if !largeCommunitiesEqual(e.LargeCommunities, other.LargeCommunities) {
		return false
	}

	if !uint64SliceEqual(e.ExtendedCommunities, other.ExtendedCommunities) {
		return false
	}

	return true
}

func peerEqual(a, b net.IP) bool {
	a4 := a.To4()
	b4 := b.To4()
	if a4 != nil && b4 != nil {
		return bytes.Equal(a4, b4)
	}
	if a4 == nil && b4 == nil {
		return bytes.Equal(a, b)
	}
	return false
}

func uint32SliceEqual(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func largeCommunitiesEqual(a, b [][3]uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i][0] != b[i][0] || a[i][1] != b[i][1] || a[i][2] != b[i][2] {
			return false
		}
	}
	return true
}

func uint64SliceEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (e *MRTEntry) PrettyString() string {
	var b strings.Builder
	fmt.Fprintf(&b, "Prefix: %s\n", e.Prefix.String())
	fmt.Fprintf(&b, "Peer: %s\n", e.Peer.String())
	fmt.Fprintf(&b, "PeerAS: %d\n", e.PeerAS)

	asPathStrs := make([]string, len(e.ASPath))
	for i, as := range e.ASPath {
		asPathStrs[i] = fmt.Sprintf("%d", as)
	}
	fmt.Fprintf(&b, "ASPath: %s\n", strings.Join(asPathStrs, ", "))

	commStrs := make([]string, len(e.Communities))
	for i, c := range e.Communities {
		commStrs[i] = FormatBGPCommunity(c)
	}
	fmt.Fprintf(&b, "Communities: %s\n", strings.Join(commStrs, " "))

	largeCommStrs := make([]string, len(e.LargeCommunities))
	for i, c := range e.LargeCommunities {
		largeCommStrs[i] = FormatBGPLargeCommunity(c)
	}
	fmt.Fprintf(&b, "LargeCommunities: %s\n", strings.Join(largeCommStrs, " "))

	extCommStrs := make([]string, len(e.ExtendedCommunities))
	for i, c := range e.ExtendedCommunities {
		extCommStrs[i] = FormatBGPExtendedCommunity(c)
	}
	fmt.Fprintf(&b, "ExtendedCommunities: %s\n", strings.Join(extCommStrs, " "))

	return b.String()
}

// FormatBGPCommunity 将标准 BGP Community 格式化为 "high:low" 字符串。
func FormatBGPCommunity(community uint32) string {
	return fmt.Sprintf("%d:%d", community>>16, community&0xFFFF)
}

// FormatBGPLargeCommunity 将 BGP Large Community 格式化为 "asn:action:value" 字符串。
func FormatBGPLargeCommunity(community [3]uint32) string {
	return fmt.Sprintf("%d:%d:%d", community[0], community[1], community[2])
}

// FormatBGPExtendedCommunity 将 BGP Extended Community 格式化为数字字符串。
func FormatBGPExtendedCommunity(community uint64) string {
	return fmt.Sprintf("%d", community)
}

// MRTParser 流式解析 MRT 数据，提取路由条目
type MRTParser struct {
	r              io.Reader
	peerIndexTable *mrt.PeerIndexTable
	ch             chan *MRTEntry
}

// NewMRTParser 创建一个新的 MRT 解析器
func NewMRTParser(r io.Reader) *MRTParser {
	return &MRTParser{
		r:  r,
		ch: make(chan *MRTEntry, 256),
	}
}

// Run 启动生产者 goroutine，在 ctx 取消时退出并关闭 channel
func (p *MRTParser) Run(ctx context.Context) {
	go p.run(ctx)
}

func (p *MRTParser) run(ctx context.Context) {
	defer close(p.ch)

	scanner := bufio.NewScanner(p.r)
	scanner.Split(mrt.SplitMrt)
	const maxCapacity = 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		raw := scanner.Bytes()
		msg, _, err := p.parseRawRecord(raw)
		if err != nil {
			continue // 跳过解析失败的消息
		}

		switch body := msg.Body.(type) {
		case *mrt.PeerIndexTable:
			p.peerIndexTable = body
		case *mrt.Rib:
			entries := p.ribToEntries(body)
			for _, e := range entries {
				select {
				case p.ch <- e:
				case <-ctx.Done():
					return
				}
			}
		case *mrt.BGP4MPMessage:
			entries := p.bgp4mpToEntries(body)
			for _, e := range entries {
				select {
				case p.ch <- e:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

func (p *MRTParser) parseRawRecord(raw []byte) (*mrt.MRTMessage, uint32, error) {
	if len(raw) < 12 {
		return nil, 0, fmt.Errorf("record too short: %d bytes", len(raw))
	}
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

func (p *MRTParser) ribToEntries(rib *mrt.Rib) []*MRTEntry {
	prefix := net.IPNet{}
	if rib.Prefix != nil {
		if _, ipNet, err := net.ParseCIDR(rib.Prefix.String()); err == nil {
			prefix = *ipNet
		}
	}

	var entries []*MRTEntry
	for _, e := range rib.Entries {
		peerIP := net.IP{}
		peerAS := uint32(0)
		if p.peerIndexTable != nil && int(e.PeerIndex) < len(p.peerIndexTable.Peers) {
			peer := p.peerIndexTable.Peers[e.PeerIndex]
			if peer != nil {
				peerIP = peer.IpAddress
				peerAS = peer.AS
			}
		}
		entries = append(entries, &MRTEntry{
			Prefix:              prefix,
			Peer:                peerIP,
			PeerAS:              peerAS,
			ASPath:              extractASPathSegments(e.PathAttributes),
			Communities:         extractCommunities(e.PathAttributes),
			LargeCommunities:    extractLargeCommunities(e.PathAttributes),
			ExtendedCommunities: extractExtendedCommunities(e.PathAttributes),
		})
	}
	return entries
}

func (p *MRTParser) bgp4mpToEntries(msg *mrt.BGP4MPMessage) []*MRTEntry {
	peerIP := net.IP{}
	peerAS := uint32(0)
	if msg.BGP4MPHeader != nil {
		peerIP = msg.PeerIpAddress
		peerAS = msg.PeerAS
	}

	var entries []*MRTEntry
	if msg.BGPMessage == nil {
		return entries
	}

	update, ok := msg.BGPMessage.Body.(*bgp.BGPUpdate)
	if !ok {
		return entries
	}

	asPath := extractASPathSegments(update.PathAttributes)
	communities := extractCommunities(update.PathAttributes)
	largeCommunities := extractLargeCommunities(update.PathAttributes)
	extendedCommunities := extractExtendedCommunities(update.PathAttributes)

	if len(update.NLRI) == 0 {
		// 没有 NLRI，可能是一个纯 withdraw 的 UPDATE，跳过
		return entries
	}

	for _, nlri := range update.NLRI {
		prefix := net.IPNet{}
		if nlri != nil {
			if _, ipNet, err := net.ParseCIDR(nlri.String()); err == nil {
				prefix = *ipNet
			}
		}
		entries = append(entries, &MRTEntry{
			Prefix:              prefix,
			Peer:                peerIP,
			PeerAS:              peerAS,
			ASPath:              asPath,
			Communities:         communities,
			LargeCommunities:    largeCommunities,
			ExtendedCommunities: extendedCommunities,
		})
	}
	return entries
}

// ReadEntry 从生产者 channel 读取下一条路由条目。
// 当 channel 被关闭（解析结束或 Run 的 ctx 取消）时返回 io.EOF。
// 当传入的 ctx 被取消时返回 ctx.Err()，允许调用者自行控制超时或中断。
func (p *MRTParser) ReadEntry(ctx context.Context) (*MRTEntry, error) {
	select {
	case entry, ok := <-p.ch:
		if !ok {
			return nil, io.EOF
		}
		return entry, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// extractASPathSegments 从 PathAttributes 切片中提取 AS_PATH，返回按顺序展开的 AS 号列表
func extractASPathSegments(attrs []bgp.PathAttributeInterface) []uint32 {
	for _, attr := range attrs {
		if asPathAttr, ok := attr.(*bgp.PathAttributeAsPath); ok {
			var parts []uint32
			for _, param := range asPathAttr.Value {
				parts = append(parts, param.GetAS()...)
			}
			return parts
		}
	}
	return nil
}

// extractCommunities 从 PathAttributes 中提取标准 BGP Community（RFC 1997）
// gobgp 原始类型为 []uint32，直接复制
func extractCommunities(attrs []bgp.PathAttributeInterface) []uint32 {
	for _, attr := range attrs {
		if commAttr, ok := attr.(*bgp.PathAttributeCommunities); ok {
			out := make([]uint32, len(commAttr.Value))
			copy(out, commAttr.Value)
			return out
		}
	}
	return nil
}

// extractLargeCommunities 从 PathAttributes 中提取 BGP Large Community（RFC 8092）
// gobgp 原始类型为 []*LargeCommunity，平铺为 [][3]uint32
func extractLargeCommunities(attrs []bgp.PathAttributeInterface) [][3]uint32 {
	for _, attr := range attrs {
		if lcAttr, ok := attr.(*bgp.PathAttributeLargeCommunities); ok {
			out := make([][3]uint32, len(lcAttr.Values))
			for i, v := range lcAttr.Values {
				out[i] = [3]uint32{v.ASN, v.LocalData1, v.LocalData2}
			}
			return out
		}
	}
	return nil
}

// extractExtendedCommunities 从 PathAttributes 中提取 BGP Extended Community（RFC 4360）
// 将每个 community 的 8 字节 wire format 按大端序打包为 uint64。
// 非 8 字节变体将被跳过。
func extractExtendedCommunities(attrs []bgp.PathAttributeInterface) []uint64 {
	for _, attr := range attrs {
		if ecAttr, ok := attr.(*bgp.PathAttributeExtendedCommunities); ok {
			var out []uint64
			for _, v := range ecAttr.Value {
				b, err := v.Serialize()
				if err != nil {
					continue
				}
				if len(b) == 8 {
					out = append(out, binary.BigEndian.Uint64(b))
				}
			}
			return out
		}
	}
	return nil
}

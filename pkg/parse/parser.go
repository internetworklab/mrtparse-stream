package parse

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/osrg/gobgp/v3/pkg/packet/bgp"
	"github.com/osrg/gobgp/v3/pkg/packet/mrt"
)

// MRTEntry 是从 MRT 消息中提取的一条路由条目
type MRTEntry struct {
	Prefix net.IPNet
	Peer   net.IP

	PeerAS uint32
	// ASPath 是每一段 AS_PATH 的字符串切片，外部可按需拼接（如 strings.Join）
	ASPath []uint32
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
			Prefix: prefix,
			Peer:   peerIP,
			PeerAS: peerAS,
			ASPath: extractASPathSegments(e.PathAttributes),
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
			Prefix: prefix,
			Peer:   peerIP,
			PeerAS: peerAS,
			ASPath: asPath,
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

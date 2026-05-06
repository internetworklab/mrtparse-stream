package lister

import (
	"context"
	"net"

	pkgdb "github.com/internetworklab/mrtparse-stream/pkg/db"
)

type ChannelListerWrapper struct {
	ch <-chan pkgdb.MRTEntryDataEvent
}

func ChannelLister(ch <-chan pkgdb.MRTEntryDataEvent) *ChannelListerWrapper {
	return &ChannelListerWrapper{ch: ch}
}

func (w *ChannelListerWrapper) List(ctx context.Context) (any, error) {
	stream, err := w.ListAsStream(ctx)
	if err != nil {
		return nil, err
	}
	var entries []any
	for item := range stream {
		entries = append(entries, item)
	}
	return entries, nil
}

func (w *ChannelListerWrapper) ListAsStream(ctx context.Context) (<-chan any, error) {
	out := make(chan any)
	go func() {
		defer close(out)
		for {
			select {
			case event, ok := <-w.ch:
				if !ok {
					return
				}
				if event.Err != nil {
					return
				}
				select {
				case out <- event.Data:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

type PG_SQL_MRTEntriesQuerier struct {
	reader pkgdb.MRTEntriesReader
}

func NewPG_SQL_MRTEntriesQuerier(reader pkgdb.MRTEntriesReader) *PG_SQL_MRTEntriesQuerier {
	return &PG_SQL_MRTEntriesQuerier{reader: reader}
}

// QueryMRTEntriesByOriginASN returns entries whose AS path originates
// from the given origin ASN (i.e. the last AS in the AS path).
func (q *PG_SQL_MRTEntriesQuerier) QueryMRTEntriesByOriginASN(ctx context.Context, originASN uint32, provider string) (Lister, error) {
	return ChannelLister(q.reader.GetMRTEntriesByOriginAS(ctx, originASN, provider)), nil
}

// QueryMRTEntriesByASSegments returns entries whose AS path contains
// all of the given AS numbers (in any order).
func (q *PG_SQL_MRTEntriesQuerier) QueryMRTEntriesByASSegments(ctx context.Context, asSegments []uint32, provider string) (Lister, error) {
	return ChannelLister(q.reader.GetMRTEntriesByASSegments(ctx, asSegments, provider)), nil
}

// QueryMRTEntriesByNeighborASN returns entries whose AS path starts
// with the given neighbor ASN (i.e. the first AS in the AS path).
func (q *PG_SQL_MRTEntriesQuerier) QueryMRTEntriesByNeighborASN(ctx context.Context, neighborASN uint32, provider string) (Lister, error) {
	return ChannelLister(q.reader.GetMRTEntriesByNeighborAS(ctx, neighborASN, provider)), nil
}

// QueryMRTEntriesByIP returns entries whose prefix contains the given
// IP address (i.e. prefix >> targetIP).
func (q *PG_SQL_MRTEntriesQuerier) QueryMRTEntriesByIP(ctx context.Context, targetIP net.IP, provider string) (Lister, error) {
	return ChannelLister(q.reader.GetMRTEntriesByIP(ctx, targetIP, provider)), nil
}

// QueryMRTEntriesByCIDR returns entries whose prefix encompasses or
// matches the given CIDR (i.e. prefix >>= targetCIDR).
func (q *PG_SQL_MRTEntriesQuerier) QueryMRTEntriesByCIDR(ctx context.Context, targetCIDR net.IPNet, provider string) (Lister, error) {
	return ChannelLister(q.reader.GetMRTEntriesByCIDR(ctx, targetCIDR, provider)), nil
}

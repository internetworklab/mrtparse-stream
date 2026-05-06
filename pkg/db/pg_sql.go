package db

import (
	"context"
	"fmt"
	"net"

	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func getSelectStatement() string {
	return `prefix, peer, peer_as, as_path, community, extended_community_high, extended_community_low, large_community_high, large_community_mid, large_community_low, next_hop`
}

func getInsertStatement() string {
	return `INSERT INTO mrt_entries (
			generation, source, prefix, prefix_len, peer, peer_as, as_path,
			community,
			community_high, community_low,
			extended_community_high, extended_community_low,
			large_community_high, large_community_mid, large_community_low,
			next_hop
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)`
}

func mrtEntryToInsertArgs(generationID int, provider string, e *pkgmodel.MRTEntry) []interface{} {
	return []interface{}{
		generationID, provider, e.Prefix.String(), prefixLen(e.Prefix), normalizeIP(e.Peer), int32(e.PeerAS), uint32SliceToInt32(e.ASPath),
		uint32SliceToInt32(e.Communities),
		communityHigh(e.Communities), communityLow(e.Communities),
		extendedCommunityHigh(e.ExtendedCommunities), extendedCommunityLow(e.ExtendedCommunities),
		largeCommunityHigh(e.LargeCommunities), largeCommunityMid(e.LargeCommunities), largeCommunityLow(e.LargeCommunities),
		normalizeIP(e.NextHop),
	}
}

func consumeRows(ctx context.Context, ch chan<- MRTEntryDataEvent, rows pgx.Rows) {
	for rows.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		entry, err := doScan(rows)
		if err != nil {
			select {
			case ch <- MRTEntryDataEvent{Err: err}:
			case <-ctx.Done():
			}
			return
		}
		select {
		case ch <- MRTEntryDataEvent{Data: entry}:
		case <-ctx.Done():
			return
		}
	}

	if err := rows.Err(); err != nil {
		select {
		case ch <- MRTEntryDataEvent{Err: fmt.Errorf("pgsql data source error: %w", err)}:
		case <-ctx.Done():
		}
	}
}

func doScan(rows pgx.Rows) (*pkgmodel.MRTEntry, error) {
	var prefix net.IPNet
	var peer net.IP
	var peerAS int32
	var asPath []int32
	var community []int32
	var ecHigh []int32
	var ecLow []int32
	var lcHigh []int32
	var lcMid []int32
	var lcLow []int32
	var nextHop net.IP
	if err := rows.Scan(&prefix, &peer, &peerAS, &asPath, &community, &ecHigh, &ecLow, &lcHigh, &lcMid, &lcLow, &nextHop); err != nil {
		return nil, err
	}
	return &pkgmodel.MRTEntry{
		Prefix:              prefix,
		Peer:                peer,
		PeerAS:              uint32(peerAS),
		ASPath:              int32SliceToUint32(asPath),
		Communities:         int32SliceToUint32(community),
		ExtendedCommunities: zipExtendedCommunities(ecHigh, ecLow),
		LargeCommunities:    zipLargeCommunities(lcHigh, lcMid, lcLow),
		NextHop:             nextHop,
	}, nil
}

type MRTEntryDataEvent struct {
	Data *pkgmodel.MRTEntry
	Err  error
}

type MRTEntriesWriteCloser interface {
	WriteMRTEntry(ctx context.Context, entry *pkgmodel.MRTEntry) error

	// Close stop writing and the implementation should increase the ready version of the underlying immutable collection.
	// And no further `WriteMRTEntry` should be call from this instance of `MRTEntriesWriteCloser`.
	Close() error
}

type MRTEntriesWriter interface {
	GetLatestReadyGen(ctx context.Context) (int, error)
	GetProvider(ctx context.Context) (string, error)

	// A functional MRTEntriesWriter should maintain a immutable collection of mrt_entries internally,
	// and it should also keep track of a `latest_ready_generation` variable which increases each time when
	// there is a call to `WriteMRTEntries`. In short, the collection in its entire should be immutable,
	// a user from outside can just either delete it or put an entire new collection on top of it.
	WriteMRTEntries(ctx context.Context, entries []*pkgmodel.MRTEntry) error
}

type MRTEntriesReader interface {
	GetLatestReadyGen(ctx context.Context) (int, error)
	GetProvider(ctx context.Context) (string, error)

	// Returning all MRT entries from the underlying latest ready collection.
	GetAllMRTEntries(ctx context.Context) <-chan MRTEntryDataEvent

	// Returning all MRT entries whose prefix can cover the given `targetIP`, in descending order of prefix length.
	GetMRTEntriesByIP(ctx context.Context, targetIP net.IP) <-chan MRTEntryDataEvent

	// Returning all MRT entries whose prefix is the super set of the given `targetCIDR`, in descending order of prefix length.
	GetMRTEntriesByCIDR(ctx context.Context, targetCIDR net.IPNet) <-chan MRTEntryDataEvent

	// Returning all MRT entries that originate from the the given `originAS`, i.e. the last element of the ASN path match the given value.
	GetMRTEntriesByOriginAS(ctx context.Context, originAS uint32) <-chan MRTEntryDataEvent

	// Returning all MRT entries that are announced by the given `neighborAS`, i.e. the first element of the ASN path match the given value.
	GetMRTEntriesByNeighborAS(ctx context.Context, neighborAS uint32) <-chan MRTEntryDataEvent

	// Returning all MRT entries whose AS path contains the given AS segments (subset), using the PostgreSQL @> (contains) operator.
	GetMRTEntriesByASSegments(ctx context.Context, asSegments []uint32) <-chan MRTEntryDataEvent
}

type MRTEntriesReadWriter interface {
	MRTEntriesReader
	MRTEntriesWriter
}

type PG_SQL_MRTEntriesReadWriter struct {
	pool          *pgxpool.Pool
	provider      string
	maxGensAllows int
}

func (p *PG_SQL_MRTEntriesReadWriter) Clone() *PG_SQL_MRTEntriesReadWriter {
	newP := new(PG_SQL_MRTEntriesReadWriter)
	*newP = *p
	return newP
}

type PG_SQL_MRTEntriesReadWriterConfigurer func(*PG_SQL_MRTEntriesReadWriter) *PG_SQL_MRTEntriesReadWriter

func WithMaxReadyGenerationsAllowed(maxAllowed int) PG_SQL_MRTEntriesReadWriterConfigurer {
	return func(p *PG_SQL_MRTEntriesReadWriter) *PG_SQL_MRTEntriesReadWriter {
		newP := p.Clone()
		newP.maxGensAllows = maxAllowed
		return newP
	}
}

func NewPgSqlMRTEntriesReadWriter(
	pool *pgxpool.Pool,
	provider string,
	options ...PG_SQL_MRTEntriesReadWriterConfigurer,
) (*PG_SQL_MRTEntriesReadWriter, error) {
	readWriter := &PG_SQL_MRTEntriesReadWriter{
		pool:     pool,
		provider: provider,
	}

	if err := sanitizeString(provider); err != nil {
		return nil, fmt.Errorf("the provider (source) field didn't parse sanitizeString: %w", err)
	}

	for _, opt := range options {
		readWriter = opt(readWriter)
	}

	return readWriter, nil
}

const defaultMaxGensAllows = 1

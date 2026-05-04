package db

import (
	"context"
	"fmt"
	"net"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Query by IP address or CIDR

func (pgWriter *PG_SQL_MRTEntriesReadWriter) GetMRTEntriesByIP(ctx context.Context, targetIP net.IP) <-chan MRTEntryDataEvent {

	var pool *pgxpool.Pool = pgWriter.pool
	ch := make(chan MRTEntryDataEvent, mrtEntryChannelBufferSize)

	go func() {
		defer close(ch)

		var err error
		var generation int
		var provider string = pgWriter.provider
		if generation, err = pgWriter.GetLatestReadyGen(ctx); err != nil {
			ch <- MRTEntryDataEvent{Err: fmt.Errorf("failed to latest gen for provider %s: %w", provider, err)}
			return
		}

		rows, err := pool.Query(ctx,
			`SELECT `+getSelectStatement()+` FROM mrt_entries WHERE prefix >> $1 AND generation = $2 AND source = $3 ORDER BY prefix_len DESC`,
			normalizeIP(targetIP), generation, provider,
		)
		if err != nil {
			select {
			case ch <- MRTEntryDataEvent{Err: fmt.Errorf("error when building query: %w", err)}:
			case <-ctx.Done():
			}
			return
		}
		defer rows.Close()

		consumeRows(ctx, ch, rows)
	}()

	return ch
}

func (pgWriter *PG_SQL_MRTEntriesReadWriter) GetMRTEntriesByCIDR(ctx context.Context, targetCIDR net.IPNet) <-chan MRTEntryDataEvent {
	var pool *pgxpool.Pool = pgWriter.pool
	ch := make(chan MRTEntryDataEvent, mrtEntryChannelBufferSize)

	go func() {
		defer close(ch)

		var err error
		var generation int
		var provider string = pgWriter.provider
		if generation, err = pgWriter.GetLatestReadyGen(ctx); err != nil {
			ch <- MRTEntryDataEvent{Err: fmt.Errorf("failed to latest gen for provider %s: %w", provider, err)}
			return
		}

		rows, err := pool.Query(ctx,
			`SELECT `+getSelectStatement()+` FROM mrt_entries WHERE prefix >>= $1 AND generation = $2 AND source = $3 ORDER BY prefix_len DESC`,
			targetCIDR, generation, provider,
		)
		if err != nil {
			select {
			case ch <- MRTEntryDataEvent{Err: fmt.Errorf("error when building query: %w", err)}:
			case <-ctx.Done():
			}
			return
		}
		defer rows.Close()

		consumeRows(ctx, ch, rows)
	}()

	return ch
}

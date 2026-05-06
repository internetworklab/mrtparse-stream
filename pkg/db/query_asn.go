package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Queries related to BGP ASN or AS Path

func (pgWriter *PG_SQL_MRTEntriesReadWriter) GetMRTEntriesByOriginAS(ctx context.Context, originAS uint32) <-chan MRTEntryDataEvent {
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

		tableName := pgWriter.tableBD.TableName(generation)
		rows, err := pool.Query(ctx,
			fmt.Sprintf(`SELECT %s FROM %s WHERE as_path[array_length(as_path, 1)] = $1 ORDER BY prefix_len DESC`, getSelectStatement(), tableName),
			int32(originAS),
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

func (pgWriter *PG_SQL_MRTEntriesReadWriter) GetMRTEntriesByASSegments(ctx context.Context, asSegments []uint32) <-chan MRTEntryDataEvent {
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

		tableName := pgWriter.tableBD.TableName(generation)
		rows, err := pool.Query(ctx,
			fmt.Sprintf(`SELECT %s FROM %s WHERE as_path @> $1 ORDER BY prefix_len DESC`, getSelectStatement(), tableName),
			uint32SliceToInt32(asSegments),
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

func (pgWriter *PG_SQL_MRTEntriesReadWriter) GetMRTEntriesByNeighborAS(ctx context.Context, neighborAS uint32) <-chan MRTEntryDataEvent {
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

		tableName := pgWriter.tableBD.TableName(generation)
		rows, err := pool.Query(ctx,
			fmt.Sprintf(`SELECT %s FROM %s WHERE as_path[1] = $1 ORDER BY prefix_len DESC`, getSelectStatement(), tableName),
			int32(neighborAS),
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

package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// General query stuffs

func (pgWriter *PG_SQL_MRTEntriesReadWriter) GetLatestReadyGen(ctx context.Context, provider string) (int, error) {
	var pool *pgxpool.Pool = pgWriter.pool
	var err error
	// 独立获取当前最新的 ready generation（与插入阶段无关）
	var latestReadyGen int = -1
	err = pool.QueryRow(ctx,
		`SELECT id FROM generations WHERE status = 'ready' AND source = $1 ORDER BY id DESC LIMIT 1`, provider,
	).Scan(&latestReadyGen)
	if err != nil {
		return -1, fmt.Errorf("failed to query latest ready gen: %v", err)
	}

	return latestReadyGen, nil
}

func (pgWriter *PG_SQL_MRTEntriesReadWriter) GetAllMRTEntries(ctx context.Context, provider string) <-chan MRTEntryDataEvent {
	var pool *pgxpool.Pool = pgWriter.pool
	ch := make(chan MRTEntryDataEvent, mrtEntryChannelBufferSize)

	go func() {
		defer close(ch)

		var err error
		var generation int

		if generation, err = pgWriter.GetLatestReadyGen(ctx, provider); err != nil {
			ch <- MRTEntryDataEvent{Err: fmt.Errorf("failed to latest gen for provider %s: %w", provider, err)}
			return
		}

		tableName := pgWriter.tableBD.TableName(generation)
		rows, err := pool.Query(ctx,
			fmt.Sprintf(`SELECT %s FROM %s ORDER BY prefix_len DESC`, getSelectStatement(), tableName),
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

package db

import (
	"context"
	"fmt"

	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Compile-time check that PG_SQL_MRTEntries_Write_Channel implements MRTEntriesWriteCloser.
var _ MRTEntriesWriteCloser = (*PG_SQL_MRTEntries_Write_Channel)(nil)

// PG_SQL_MRTEntries_Write_Channel implements MRTEntriesWriteCloser for streaming use cases.
// It creates a new generation on initialization, accepts individual MRTEntry writes
// (buffered in an internal batch queue), and finalizes (marks ready + prunes stale
// generations) on Close.
type PG_SQL_MRTEntries_Write_Channel struct {
	pool          *pgxpool.Pool
	provider      string
	generationID  int
	maxGensAllows int
	queueLen      int
	batch         *pgx.Batch
	batchCount    int
	closedCh      chan any // a closed closedCh indicate that this instance is no longer usable.
	tableBD       TableBuildDestroyer
}

type PG_SQL_MRTEntries_Write_ChannelConfigurer func(*PG_SQL_MRTEntries_Write_Channel) *PG_SQL_MRTEntries_Write_Channel

func WithStreamMaxReadyGenerationsAllowed(maxAllowed int) PG_SQL_MRTEntries_Write_ChannelConfigurer {
	return func(w *PG_SQL_MRTEntries_Write_Channel) *PG_SQL_MRTEntries_Write_Channel {
		return &PG_SQL_MRTEntries_Write_Channel{
			pool:          w.pool,
			provider:      w.provider,
			generationID:  w.generationID,
			maxGensAllows: maxAllowed,
			queueLen:      w.queueLen,
			batch:         w.batch,
			batchCount:    w.batchCount,
			closedCh:      w.closedCh,
			tableBD:       w.tableBD,
		}
	}
}

const defaultBatchSize = 4000

// WithStreamQueueLen returns a configurer that sets the internal batch queue length.
// When the queue fills up, all queued inserts are flushed to PostgreSQL in a single
// round-trip via pgx.Batch. Default is `defaultBatchSize`.
func WithStreamQueueLen(queueLen int) PG_SQL_MRTEntries_Write_ChannelConfigurer {
	return func(w *PG_SQL_MRTEntries_Write_Channel) *PG_SQL_MRTEntries_Write_Channel {
		return &PG_SQL_MRTEntries_Write_Channel{
			pool:          w.pool,
			provider:      w.provider,
			generationID:  w.generationID,
			maxGensAllows: w.maxGensAllows,
			queueLen:      queueLen,
			batch:         w.batch,
			batchCount:    w.batchCount,
			closedCh:      w.closedCh,
			tableBD:       w.tableBD,
		}
	}
}

func (w *PG_SQL_MRTEntries_Write_Channel) getQueueLen() int {
	if x := w.queueLen; x > 0 {
		return x
	}
	return defaultBatchSize
}

func (w *PG_SQL_MRTEntries_Write_Channel) getMaxGensAllows() int {
	if x := w.maxGensAllows; x != 0 {
		return x
	}
	return defaultMaxGensAllows
}

// NewPG_SQL_MRTEntries_Write_Channel creates a new streaming writer.
// A fresh generation is created immediately; callers then stream individual entries
// via WriteMRTEntry and finalize the batch with Close.
func NewPG_SQL_MRTEntries_Write_Channel(
	ctx context.Context,
	pool *pgxpool.Pool,
	provider string,
	tableBD TableBuildDestroyer,
	options ...PG_SQL_MRTEntries_Write_ChannelConfigurer,
) (*PG_SQL_MRTEntries_Write_Channel, error) {
	if err := sanitizeString(provider); err != nil {
		return nil, fmt.Errorf("sanitize provider failed: %v", err)
	}
	if tableBD == nil {
		return nil, fmt.Errorf("tableBD must not be nil")
	}

	w := &PG_SQL_MRTEntries_Write_Channel{
		pool:     pool,
		provider: provider,
		batch:    &pgx.Batch{},
		tableBD:  tableBD,
	}

	for _, opt := range options {
		w = opt(w)
	}

	// Create a new generation up front.
	var generationID int
	if err := pool.QueryRow(ctx, `INSERT INTO generations (source) VALUES ($1) RETURNING id`, provider).Scan(&generationID); err != nil {
		return nil, fmt.Errorf("create generation failed: %w", err)
	}
	w.generationID = generationID
	fmt.Printf("Created generation id=%d\n", generationID)

	// Build per-generation table.
	if err := w.tableBD.BuildTable(ctx, generationID); err != nil {
		return nil, fmt.Errorf("build table for generation %d failed: %w", generationID, err)
	}
	fmt.Printf("Built table %s for generation %d\n", w.tableBD.TableName(generationID), generationID)

	w.closedCh = make(chan any)

	return w, nil
}

// WriteMRTEntry queues a single MRT entry for insertion. The entry is buffered
// internally and flushed to PostgreSQL when the queue reaches the configured length.
func (w *PG_SQL_MRTEntries_Write_Channel) WriteMRTEntry(ctx context.Context, entry *pkgmodel.MRTEntry) error {
	select {
	case <-w.closedCh:
		return fmt.Errorf("channel is closed")
	default:
	}

	w.batch.Queue(getInsertStatement(w.tableBD.TableName(w.generationID)), mrtEntryToInsertArgs(w.generationID, entry)...)
	w.batchCount++

	if w.batchCount >= w.getQueueLen() {
		return w.flush(ctx)
	}
	return nil
}

// flush sends the accumulated batch to PostgreSQL in a single round-trip.
func (w *PG_SQL_MRTEntries_Write_Channel) flush(ctx context.Context) error {
	if w.batchCount == 0 {
		return nil
	}

	br := w.pool.SendBatch(ctx, w.batch)
	defer br.Close()

	for i := 0; i < w.batchCount; i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert failed at row %d: %v", i, err)
		}
	}

	w.batch = &pgx.Batch{}
	w.batchCount = 0
	return nil
}

// Close flushes any remaining queued entries, marks the current generation as
// ready, and prunes stale generations that exceed the configured maximum.
// No further WriteMRTEntry calls are permitted after Close.
func (w *PG_SQL_MRTEntries_Write_Channel) Close() error {
	select {
	case <-w.closedCh:
		return fmt.Errorf("channel is closed")
	default:
	}

	if err := w.flush(context.Background()); err != nil {
		close(w.closedCh)
		return fmt.Errorf("flush remaining entries failed: %w", err)
	}

	close(w.closedCh)

	return finalizeCollectionCreate(context.Background(), w.pool, w.provider, w.tableBD, w.generationID, w.getMaxGensAllows())
}

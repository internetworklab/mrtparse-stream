package db

import (
	"context"
	"fmt"

	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Compile-time check that PG_SQL_MRTEntries_Write_Channel implements MRTEntriesWriteCloser.
var _ MRTEntriesWriteCloser = (*PG_SQL_MRTEntries_Write_Channel)(nil)

// PG_SQL_MRTEntries_Write_Channel implements MRTEntriesWriteCloser for streaming use cases.
// It creates a new generation on initialization, accepts individual MRTEntry writes,
// and finalizes (marks ready + prunes stale generations) on Close.
type PG_SQL_MRTEntries_Write_Channel struct {
	pool          *pgxpool.Pool
	provider      string
	generationID  int
	maxGensAllows int
	closedCh      chan any // a closed closedCh indicate that this instance is no longer usable.
}

type PG_SQL_MRTEntries_Write_ChannelConfigurer func(*PG_SQL_MRTEntries_Write_Channel) *PG_SQL_MRTEntries_Write_Channel

func WithStreamMaxReadyGenerationsAllowed(maxAllowed int) PG_SQL_MRTEntries_Write_ChannelConfigurer {
	return func(w *PG_SQL_MRTEntries_Write_Channel) *PG_SQL_MRTEntries_Write_Channel {
		newW := &PG_SQL_MRTEntries_Write_Channel{
			pool:          w.pool,
			provider:      w.provider,
			generationID:  w.generationID,
			maxGensAllows: maxAllowed,
		}
		return newW
	}
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
	options ...PG_SQL_MRTEntries_Write_ChannelConfigurer,
) (*PG_SQL_MRTEntries_Write_Channel, error) {
	if err := sanitizeString(provider); err != nil {
		return nil, fmt.Errorf("sanitize provider failed: %v", err)
	}

	w := &PG_SQL_MRTEntries_Write_Channel{
		pool:     pool,
		provider: provider,
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

	w.closedCh = make(chan any)

	return w, nil
}

// WriteMRTEntry inserts a single MRT entry into the current generation.
// Returns an error if the writer has been closed or if a concurrent write is in progress.
func (w *PG_SQL_MRTEntries_Write_Channel) WriteMRTEntry(ctx context.Context, entry *pkgmodel.MRTEntry) error {
	select {
	case <-w.closedCh:
		return fmt.Errorf("channel is closed")
	default:
	}

	_, err := w.pool.Exec(ctx, getInsertStatement(), mrtEntryToInsertArgs(w.generationID, w.provider, entry)...)
	if err != nil {
		return fmt.Errorf("insert failed: %v", err)
	}
	return nil
}

// Close stops the ticket feeder, marks the current generation as ready,
// and prunes stale generations that exceed the configured maximum.
// No further WriteMRTEntry calls are permitted after Close.
func (w *PG_SQL_MRTEntries_Write_Channel) Close() error {
	select {
	case <-w.closedCh:
		return fmt.Errorf("channel is closed")
	default:
	}

	close(w.closedCh)

	return finalizeCollectionCreate(context.Background(), w.pool, w.provider, w.generationID, w.getMaxGensAllows())
}

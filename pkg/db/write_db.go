package db

import (
	"context"
	"fmt"

	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Defines how to save data to DB

func (pgWriter *PG_SQL_MRTEntriesReadWriter) getMaxGensAllows() int {
	if x := pgWriter.maxGensAllows; x != 0 {
		return x
	}
	return defaultMaxGensAllows
}

func (pgWriter *PG_SQL_MRTEntriesReadWriter) WriteMRTEntries(ctx context.Context, entries []*pkgmodel.MRTEntry, provider string) error {
	maxReadyGenerationsAllowed := pgWriter.getMaxGensAllows()

	var err error
	var pool *pgxpool.Pool = pgWriter.pool

	if err := sanitizeString(provider); err != nil {
		return fmt.Errorf("sanitize provider failed: %v", err)
	}

	// 创建一个新的 generation
	var generationID int
	err = pool.QueryRow(ctx, `INSERT INTO generations (source) VALUES ($1) RETURNING id`, provider).Scan(&generationID)
	if err != nil {
		return fmt.Errorf("create generation failed: %w", err)
	}
	fmt.Printf("Created generation id=%d\n", generationID)

	// Build per-generation table
	if err := pgWriter.tableBD.BuildTable(ctx, generationID); err != nil {
		return fmt.Errorf("build table for generation %d failed: %w", generationID, err)
	}
	fmt.Printf("Built table %s for generation %d\n", pgWriter.tableBD.TableName(generationID), generationID)

	// Insert into per-generation table using batch insertion
	insertSQL := getInsertStatement(pgWriter.tableBD.TableName(generationID))
	batch := &pgx.Batch{}
	batchCount := 0

	for _, e := range entries {
		batch.Queue(insertSQL, mrtEntryToInsertArgs(e)...)
		batchCount++

		if batchCount >= defaultBatchSize {
			if err := flushBatch(ctx, pool, batch, batchCount); err != nil {
				return fmt.Errorf("batch insert failed: %w", err)
			}
			batch = &pgx.Batch{}
			batchCount = 0
		}
	}

	// Flush remaining entries
	if batchCount > 0 {
		if err := flushBatch(ctx, pool, batch, batchCount); err != nil {
			return fmt.Errorf("batch insert failed: %w", err)
		}
	}

	fmt.Println("Inserted", len(entries), "entries")

	return finalizeCollectionCreate(ctx, pool, provider, pgWriter.tableBD, generationID, maxReadyGenerationsAllowed)
}

// flushBatch sends the accumulated batch to PostgreSQL in a single round-trip.
func flushBatch(ctx context.Context, pool *pgxpool.Pool, batch *pgx.Batch, count int) error {
	br := pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := range count {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert failed at row %d: %v", i, err)
		}
	}
	return nil
}

// finalizeCollectionCreate marks the given generation as ready and prunes stale generations
// that exceed maxReadyGenerationsAllowed for the given provider.
func finalizeCollectionCreate(ctx context.Context, pool *pgxpool.Pool, provider string, tableBD TableBuildDestroyer, generationID int, maxReadyGenerationsAllowed int) error {
	// 将 generation 状态更新为 ready
	// todo: we should drop the 'created_at' field and use 'last_modified' instead since the semantics are for representing the status of immutable collection.
	if _, err := pool.Exec(ctx,
		`UPDATE generations SET status = 'ready', created_at = now() WHERE id = $1 AND source = $2`,
		generationID, provider,
	); err != nil {
		return fmt.Errorf("update generation status failed: %w", err)
	}
	fmt.Println("Generation", generationID, "status set to ready")

	// 循环清理过期的 ready generation，直到数量符合限制
	for {
		var readyCount int
		if err := pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM generations WHERE status = 'ready' AND source = $1`,
			provider,
		).Scan(&readyCount); err != nil {
			return fmt.Errorf("count ready generations failed: %v", err)
		}
		fmt.Printf("Current ready generations for provider %s: %d (max allowed: %d)\n", provider, readyCount, maxReadyGenerationsAllowed)

		if readyCount > maxReadyGenerationsAllowed {
			var oldestGenID int
			if err := pool.QueryRow(ctx,
				`SELECT id FROM generations WHERE status = 'ready' AND source = $1 ORDER BY id ASC LIMIT 1`,
				provider,
			).Scan(&oldestGenID); err != nil {
				return fmt.Errorf("find oldest ready generation failed: %w", err)
			}

			if err := tableBD.DestroyTable(ctx, oldestGenID); err != nil {
				return fmt.Errorf("destroy table for generation %d failed: %w", oldestGenID, err)
			}

			if _, err := pool.Exec(ctx, `DELETE FROM generations WHERE id = $1 AND source = $2`, oldestGenID, provider); err != nil {
				return fmt.Errorf("delete generation %d failed: %w", oldestGenID, err)
			}

			fmt.Printf("Deleted oldest ready generation %d and its mrt_entries for provider %s\n", oldestGenID, provider)
		} else {
			break
		}
	}
	return nil
}

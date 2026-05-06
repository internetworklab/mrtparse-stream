package db

import (
	"context"
	"fmt"

	pkgmodel "github.com/internetworklab/mrtparse-stream/pkg/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Defines how to save data to DB

func (pgWriter *PG_SQL_MRTEntriesReadWriter) getMaxGensAllows() int {
	if x := pgWriter.maxGensAllows; x != 0 {
		return x
	}
	return defaultMaxGensAllows
}

func (pgWriter *PG_SQL_MRTEntriesReadWriter) WriteMRTEntries(ctx context.Context, entries []*pkgmodel.MRTEntry) error {
	maxReadyGenerationsAllowed := pgWriter.getMaxGensAllows()

	var provider string = pgWriter.provider
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

	// 插入 mrt_entries
	for _, e := range entries {
		_, err := pool.Exec(ctx, getInsertStatement(), mrtEntryToInsertArgs(generationID, e)...)
		if err != nil {
			return fmt.Errorf("insert failed: %v", err)
		}
	}
	fmt.Println("Inserted", len(entries), "entries")

	return finalizeCollectionCreate(ctx, pool, provider, generationID, maxReadyGenerationsAllowed)
}

// finalizeCollectionCreate marks the given generation as ready and prunes stale generations
// that exceed maxReadyGenerationsAllowed for the given provider.
func finalizeCollectionCreate(ctx context.Context, pool *pgxpool.Pool, provider string, generationID int, maxReadyGenerationsAllowed int) error {
	// 将 generation 状态更新为 ready
	if _, err := pool.Exec(ctx,
		`UPDATE generations SET status = 'ready' WHERE id = $1 AND source = $2`,
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

			if _, err := pool.Exec(ctx, `DELETE FROM mrt_entries WHERE generation = $1`, oldestGenID); err != nil {
				return fmt.Errorf("delete mrt_entries for generation %d failed: %w", oldestGenID, err)
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

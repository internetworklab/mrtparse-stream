package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ProvidersReader interface {
	GetProviders(ctx context.Context) ([]ProviderEntry, error)
	GetProvidersAsStream(ctx context.Context) (<-chan ProviderEntry, error)
}

type ProviderStatus string

const (
	ProviderStatusProvisioning ProviderStatus = "provisioning"
	ProviderStatusReady        ProviderStatus = "ready"
)

type ProviderEntry struct {
	Name         string
	Status       ProviderStatus
	LastModified time.Time
}

type PG_SQL_ProvidersReaderConfigurer func(*PG_SQL_ProvidersReader) *PG_SQL_ProvidersReader

type PG_SQL_ProvidersReader struct {
	pool      *pgxpool.Pool
	readyOnly bool
}

func WithReadyOnly(readyOnly bool) PG_SQL_ProvidersReaderConfigurer {
	return func(r *PG_SQL_ProvidersReader) *PG_SQL_ProvidersReader {
		r.readyOnly = readyOnly
		return r
	}
}

func NewPGSqlProvidersReader(pool *pgxpool.Pool, opts ...PG_SQL_ProvidersReaderConfigurer) *PG_SQL_ProvidersReader {
	r := &PG_SQL_ProvidersReader{pool: pool}
	for _, opt := range opts {
		r = opt(r)
	}
	return r
}

func (r *PG_SQL_ProvidersReader) getSelectClause() string {
	return `SELECT DISTINCT ON (source) source, status, created_at FROM generations`
}

func (r *PG_SQL_ProvidersReader) GetProvidersAsStream(ctx context.Context) (<-chan ProviderEntry, error) {
	sqlParts := []string{
		r.getSelectClause(),
	}

	var args []any

	if r.readyOnly {
		sqlParts = append(sqlParts, `WHERE status = $1`)
		args = append(args, ProviderStatusReady)
	}

	sqlParts = append(sqlParts, `ORDER BY source, id DESC`)

	query := strings.Join(sqlParts, " ")

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query providers: %w", err)
	}

	ch := make(chan ProviderEntry)

	go func() {
		defer close(ch)
		defer rows.Close()

		for rows.Next() {
			var entry ProviderEntry
			if err := rows.Scan(&entry.Name, &entry.Status, &entry.LastModified); err != nil {
				return
			}
			select {
			case ch <- entry:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

func (r *PG_SQL_ProvidersReader) GetProviders(ctx context.Context) ([]ProviderEntry, error) {
	ch, err := r.GetProvidersAsStream(ctx)
	if err != nil {
		return nil, err
	}

	var providers []ProviderEntry
	for entry := range ch {
		providers = append(providers, entry)
	}
	return providers, nil
}

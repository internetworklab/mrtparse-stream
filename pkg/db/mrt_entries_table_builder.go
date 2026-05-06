package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type TableBuilder interface {
	BuildTable(ctx context.Context, generation int) error
	TableName(generation int) string
}

type TableDestroyer interface {
	DestroyTable(ctx context.Context, generation int) error
}

type TableBuildDestroyer interface {
	TableBuilder
	TableDestroyer
}

// MRTEntriesTableBuilder creates per-generation MRT entries tables that conform
// to the canonical schema defined in schema.sql.  Each call to BuildTable
// produces a table named {tablePrefix}_{generation} with the same columns,
// indexes, and comments as mrt_entries.
type MRTEntriesTableBuilder struct {
	pool        *pgxpool.Pool
	tablePrefix string
}

// NewMRTEntriesTableBuilder validates its arguments and returns a builder that
// will create tables named {tablePrefix}_{generation}.  tablePrefix must be a
// non-empty RFC 1035 label (alphanumeric / hyphen, 1–63 chars).
func NewMRTEntriesTableBuilder(pool *pgxpool.Pool, tablePrefix string) (*MRTEntriesTableBuilder, error) {
	if pool == nil {
		return nil, fmt.Errorf("pool must not be nil")
	}
	if tablePrefix == "" {
		return nil, fmt.Errorf("tablePrefix must not be empty")
	}
	if err := sanitizeString(tablePrefix); err != nil {
		return nil, fmt.Errorf("invalid tablePrefix: %w", err)
	}
	return &MRTEntriesTableBuilder{
		pool:        pool,
		tablePrefix: tablePrefix,
	}, nil
}

func (b *MRTEntriesTableBuilder) TableName(generation int) string {
	return fmt.Sprintf("%s_%d", b.tablePrefix, generation)
}

// ---------------------------------------------------------------------------
// Schema definition — mirrors schema.sql (mrt_entries) verbatim.
// ---------------------------------------------------------------------------

type columnDef struct {
	name        string
	dataType    string
	constraints string // e.g. "NOT NULL", "NOT NULL DEFAULT '{}'", "PRIMARY KEY"
}

type indexDef struct {
	name    string
	using   string // "" (btree), "gist", "gin"
	column  string
	opclass string // e.g. "gin__int_ops"
}

func mrtEntriesColumns() []columnDef {
	return []columnDef{
		{name: "id", dataType: "bigserial", constraints: "PRIMARY KEY"},
		{name: "prefix", dataType: "cidr", constraints: "NOT NULL"},
		{name: "prefix_len", dataType: "smallint", constraints: "NOT NULL"},
		{name: "peer", dataType: "inet", constraints: "NOT NULL DEFAULT '::'"},
		{name: "next_hop", dataType: "inet", constraints: "NOT NULL DEFAULT '::'"},
		{name: "peer_as", dataType: "int", constraints: "NOT NULL"},
		{name: "as_path", dataType: "int[]", constraints: "NOT NULL DEFAULT '{}'"},
		{name: "extended_community_high", dataType: "int[]", constraints: "NOT NULL DEFAULT '{}'"},
		{name: "extended_community_low", dataType: "int[]", constraints: "NOT NULL DEFAULT '{}'"},
		{name: "community_high", dataType: "int[]", constraints: "NOT NULL DEFAULT '{}'"},
		{name: "community_low", dataType: "int[]", constraints: "NOT NULL DEFAULT '{}'"},
		{name: "large_community_high", dataType: "int[]", constraints: "NOT NULL DEFAULT '{}'"},
		{name: "large_community_mid", dataType: "int[]", constraints: "NOT NULL DEFAULT '{}'"},
		{name: "large_community_low", dataType: "int[]", constraints: "NOT NULL DEFAULT '{}'"},
	}
}

func mrtEntriesIndexes(table string) []indexDef {
	return []indexDef{
		{name: fmt.Sprintf("idx_%s_prefix_gist", table), using: "gist", column: "prefix"},
		{name: fmt.Sprintf("idx_%s_as_path_gin", table), using: "gin", column: "as_path", opclass: "gin__int_ops"},
	}
}

func mrtEntriesComments(table string) []string {
	return []string{
		fmt.Sprintf("COMMENT ON TABLE %s IS '从 MRTDump 解析的原始 BGP 路由条目'", table),
		fmt.Sprintf("COMMENT ON COLUMN %s.peer_as IS 'Peer ASN，bigint 兼容 32-bit ASN (0-4294967295)'", table),
		fmt.Sprintf("COMMENT ON COLUMN %s.as_path IS 'AS Path，元素类型 bigint 兼容 32-bit ASN'", table),
	}
}

// ---------------------------------------------------------------------------
// BuildTable
// ---------------------------------------------------------------------------

// BuildTable creates the table {tablePrefix}_{generation} with the full
// mrt_entries schema (columns, indexes, comments).  It is safe to call
// multiple times — the table and indexes use IF NOT EXISTS.
func (b *MRTEntriesTableBuilder) BuildTable(ctx context.Context, generation int) error {
	tbl := b.TableName(generation)

	// --- CREATE TABLE ---
	cols := mrtEntriesColumns()
	colDefs := make([]string, len(cols))
	for i, c := range cols {
		colDefs[i] = fmt.Sprintf("    %s %s %s", c.name, c.dataType, c.constraints)
	}
	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n%s\n)", tbl, strings.Join(colDefs, ",\n"))

	if _, err := b.pool.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("failed to create table %s: %w", tbl, err)
	}

	// --- CREATE INDEX ---
	for _, idx := range mrtEntriesIndexes(tbl) {
		colExpr := idx.column
		if idx.opclass != "" {
			colExpr = idx.column + " " + idx.opclass
		}

		var idxSQL string
		if idx.using != "" {
			idxSQL = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s USING %s (%s)", idx.name, tbl, idx.using, colExpr)
		} else {
			idxSQL = fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)", idx.name, tbl, colExpr)
		}

		if _, err := b.pool.Exec(ctx, idxSQL); err != nil {
			return fmt.Errorf("failed to create index %s on %s: %w", idx.name, tbl, err)
		}
	}

	// --- COMMENTS ---
	for _, c := range mrtEntriesComments(tbl) {
		if _, err := b.pool.Exec(ctx, c); err != nil {
			return fmt.Errorf("failed to set comment on table %s: %w", tbl, err)
		}
	}

	return nil
}

// DestroyTable drops the table {tablePrefix}_{generation}. It is a no-op if the
// table does not exist.
func (b *MRTEntriesTableBuilder) DestroyTable(ctx context.Context, generation int) error {
	tbl := b.TableName(generation)
	if _, err := b.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", tbl)); err != nil {
		return fmt.Errorf("failed to drop table %s: %w", tbl, err)
	}
	return nil
}

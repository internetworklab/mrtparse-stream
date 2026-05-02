-- ============================================================
-- CloudPing PostgreSQL Schema
-- 执行方式: psql -d cloudping -f schema.sql
-- ============================================================

-- 扩展（每个库只需一次）
CREATE EXTENSION IF NOT EXISTS intarray;      -- 整数数组高级操作 @*
CREATE EXTENSION IF NOT EXISTS btree_gist;    -- GiST 支持复合索引（可选）

-- 设置
SET timezone = 'UTC';

-- ------------------------------------------------------------
-- 5. Generations（MRT 解析批次）
-- ------------------------------------------------------------
CREATE TYPE generation_status AS ENUM ('provisioning', 'ready');

CREATE TABLE IF NOT EXISTS generations (
    id         serial PRIMARY KEY,
    status     generation_status NOT NULL DEFAULT 'provisioning',
    created_at timestamptz NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------
-- 6. MRT 原始路由条目（从 MRTDump 解析）
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mrt_entries (
    id          bigserial PRIMARY KEY,
    generation  integer NOT NULL,
    source      text NOT NULL,
    prefix      cidr NOT NULL,              -- net.IPNet
    peer        inet NOT NULL,              -- net.IP
    peer_as     bigint NOT NULL,            -- uint32，必须用 bigint 兼容 32-bit ASN
    as_path     bigint[] NOT NULL DEFAULT '{}',  -- []uint32，同上
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- GiST：prefix 归属查询（如 prefix >> '10.7.6.55'）
CREATE INDEX IF NOT EXISTS idx_mrt_prefix_gist ON mrt_entries USING gist (prefix);

-- GIN：as_path 包含查询（如 as_path @> ARRAY[64513]）
CREATE INDEX IF NOT EXISTS idx_mrt_as_path_gin ON mrt_entries USING gin (as_path);

-- B-tree：peer 精确匹配、Peer AS 过滤、时间范围
CREATE INDEX IF NOT EXISTS idx_mrt_peer ON mrt_entries (peer);
CREATE INDEX IF NOT EXISTS idx_mrt_peer_as ON mrt_entries (peer_as);
CREATE INDEX IF NOT EXISTS idx_mrt_generation ON mrt_entries (generation);
CREATE INDEX IF NOT EXISTS idx_mrt_created_at ON mrt_entries (created_at DESC);

COMMENT ON TABLE mrt_entries IS '从 MRTDump 解析的原始 BGP 路由条目';
COMMENT ON COLUMN mrt_entries.peer_as IS 'Peer ASN，bigint 兼容 32-bit ASN (0-4294967295)';
COMMENT ON COLUMN mrt_entries.as_path IS 'AS Path，元素类型 bigint 兼容 32-bit ASN';

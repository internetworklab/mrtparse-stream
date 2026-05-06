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
-- 1. Generations（MRT 解析批次）
-- ------------------------------------------------------------
CREATE TYPE generation_status AS ENUM ('provisioning', 'ready');

CREATE TABLE IF NOT EXISTS generations (
    id         serial PRIMARY KEY,
    source     text NOT NULL,
    status     generation_status NOT NULL DEFAULT 'provisioning',
    created_at timestamptz NOT NULL DEFAULT now()
);

-- ------------------------------------------------------------
-- 2. MRT 原始路由条目（从 MRTDump 解析）
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mrt_entries (
    id          bigserial PRIMARY KEY,
    generation  integer NOT NULL,
    source      text NOT NULL,
    prefix      cidr NOT NULL,              -- net.IPNet
    prefix_len  smallint NOT NULL,          -- net.IPNet.Mask.Size()
    peer        inet NOT NULL DEFAULT '::',       -- net.IP
    next_hop    inet NOT NULL DEFAULT '::',        -- net.IP
    peer_as     int NOT NULL,            -- uint32，必须用 bigint 兼容 32-bit ASN
    as_path           int[] NOT NULL DEFAULT '{}',  -- postgres 的 int 对应 golang 的 int32，这里 golang 存的是 uint32
    community         int[] NOT NULL DEFAULT '{}',  -- []uint32，标准 BGP Community (RFC 1997)
    extended_community_high int[] NOT NULL DEFAULT '{}',  -- []uint32，BGP Extended Community (RFC 4360) 高 32 位
    extended_community_low  int[] NOT NULL DEFAULT '{}',  -- []uint32，BGP Extended Community (RFC 4360) 低 32 位
    community_high      int[] NOT NULL DEFAULT '{}',  -- community 的高 16 位
    community_low       int[] NOT NULL DEFAULT '{}',  -- community 的低 16 位
    large_community_high int[] NOT NULL DEFAULT '{}', -- large_community 第 1 分量 (GlobalAdmin)
    large_community_mid  int[] NOT NULL DEFAULT '{}', -- large_community 第 2 分量 (LocalData1)
    large_community_low  int[] NOT NULL DEFAULT '{}', -- large_community 第 3 分量 (LocalData2)
    created_at          timestamptz NOT NULL DEFAULT now()
);

-- GiST：prefix 归属查询（如 prefix >> '10.7.6.55'）
CREATE INDEX IF NOT EXISTS idx_mrt_prefix_gist ON mrt_entries USING gist (prefix);

-- GIN：as_path 包含查询（如 as_path @> ARRAY[64513]）
CREATE INDEX IF NOT EXISTS idx_mrt_as_path_gin ON mrt_entries USING gin (as_path gin__int_ops);

-- Disable community indexes for speed (for now, in future we might use separate table for them).
-- GIN：community 包含查询
-- CREATE INDEX IF NOT EXISTS idx_mrt_community_gin ON mrt_entries USING gin (community gin__int_ops);

-- GIN：community 高/低 16 位、large_community 各分量 包含查询
-- CREATE INDEX IF NOT EXISTS idx_mrt_community_high_gin ON mrt_entries USING gin (community_high gin__int_ops);
-- CREATE INDEX IF NOT EXISTS idx_mrt_community_low_gin  ON mrt_entries USING gin (community_low gin__int_ops);
-- CREATE INDEX IF NOT EXISTS idx_mrt_large_community_high_gin ON mrt_entries USING gin (large_community_high gin__int_ops);
-- CREATE INDEX IF NOT EXISTS idx_mrt_large_community_mid_gin  ON mrt_entries USING gin (large_community_mid gin__int_ops);
-- CREATE INDEX IF NOT EXISTS idx_mrt_large_community_low_gin  ON mrt_entries USING gin (large_community_low gin__int_ops);

-- B-tree：peer 精确匹配、Peer AS 过滤、时间范围
CREATE INDEX IF NOT EXISTS idx_mrt_peer_as ON mrt_entries (peer_as);
CREATE INDEX IF NOT EXISTS idx_mrt_generation ON mrt_entries (generation);
CREATE INDEX IF NOT EXISTS idx_mrt_created_at ON mrt_entries (created_at DESC);

COMMENT ON TABLE mrt_entries IS '从 MRTDump 解析的原始 BGP 路由条目';
COMMENT ON COLUMN mrt_entries.peer_as IS 'Peer ASN，bigint 兼容 32-bit ASN (0-4294967295)';
COMMENT ON COLUMN mrt_entries.as_path IS 'AS Path，元素类型 bigint 兼容 32-bit ASN';

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
--     Schema is managed dynamically by MRTEntriesTableBuilder.
--     See mrt_entries_table_builder.go for the authoritative definition.
-- ------------------------------------------------------------

-- recreate_schema.sql
-- 删除旧表并重建最小可用 schema（documents / chunks）

BEGIN;

-- 先删子表再删父表
DROP TABLE IF EXISTS chunks CASCADE;
DROP TABLE IF EXISTS documents CASCADE;

-- documents：文档级（整篇）
CREATE TABLE documents (
  id              BIGSERIAL PRIMARY KEY,
  url             TEXT NOT NULL UNIQUE,      -- 以 URL 去重，方便幂等导入
  title           TEXT,
  lang            TEXT,
  domain          TEXT,
  fetched_at_iso  TIMESTAMP,
  text            TEXT NOT NULL,
  extra_json      JSONB,                     -- 可选：留存 meta/bearers 等底稿
  created_at      TIMESTAMP DEFAULT now(),
  updated_at      TIMESTAMP DEFAULT now()
);

-- 常用索引
CREATE INDEX idx_documents_domain        ON documents (domain);
CREATE INDEX idx_documents_lang          ON documents (lang);
CREATE INDEX idx_documents_fetched_atiso ON documents (fetched_at_iso);

-- chunks：语义块级（分块/向量检索的基本单位）
CREATE TABLE chunks (
  id             BIGSERIAL PRIMARY KEY,
  doc_id         BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
  chunk_index    INTEGER NOT NULL,          -- 每个文档内从 0 递增
  content        TEXT    NOT NULL,
  char_start     INTEGER,
  char_end       INTEGER,
  token_estimate INTEGER,
  content_md5    TEXT,
  created_at     TIMESTAMP DEFAULT now(),
  updated_at     TIMESTAMP DEFAULT now(),
  UNIQUE (doc_id, chunk_index)              -- 幂等导入的关键约束
);

-- 常用索引
CREATE INDEX idx_chunks_doc_id ON chunks (doc_id);
CREATE INDEX idx_chunks_md5    ON chunks (content_md5);

COMMIT;


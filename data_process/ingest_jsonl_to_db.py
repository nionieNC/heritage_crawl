#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ingest_jsonl_to_db.py
---------------------
将 JSONL 导入到已存在的 PostgreSQL 表：
  documents(id BIGSERIAL PK, url UNIQUE, title, lang, domain, fetched_at_iso, text, extra_json)
  chunks(id BIGSERIAL PK, doc_id FK -> documents.id, chunk_index, content, char_start, char_end, token_estimate, content_md5, UNIQUE(doc_id, chunk_index))

特性：
- 幂等导入：documents 以 url 唯一；chunks 以 (doc_id, chunk_index) 唯一
- 可选：若无 chunks.jsonl，则从 documents.text 自动切块后导入（--auto-chunk-*）
- 不包含“重建表”功能（你已完成重建）

依赖：
  pip install psycopg2-binary tqdm
环境变量：
  PGHOST / PGPORT / PGUSER / PGPASSWORD / PGDATABASE
"""

import os, json, hashlib, math, datetime, argparse, re
from typing import Optional, List, Tuple
import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm

# -------- 连接 ----------
def connect():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
        dbname=os.environ.get("PGDATABASE", "postgres"),
    )

# -------- 助手 ----------
def ensure_url(url: Optional[str], title: str, text: str) -> str:
    if url and isinstance(url, str) and url.strip():
        return url.strip()
    # 构造一个稳定的占位 url，保证幂等
    base = (title or "") + "|" + (text or "")
    md5 = hashlib.md5(base.encode("utf-8")).hexdigest()
    return f"missing://{md5}"

def to_iso(val) -> Optional[str]:
    if val is None: return None
    try:
        v = float(val)
        if v > 1e12: v = v / 1000.0
        return datetime.datetime.utcfromtimestamp(v).replace(microsecond=0).isoformat()
    except Exception:
        s = str(val).strip()
        return s or None

def est_tokens_by_chars(s: str) -> int:
    # 简单估计：~4 chars / token
    return max(1, math.ceil(len(s) / 4))

# 简单中文断句切块（可按需替换为更精细的 token 切块）
SPLIT_PAT = re.compile(r"(?:\n{2,}|。|；|！|\?|？)")
def split_text(text: str, max_chars=1000, min_chars=600) -> List[str]:
    if not text:
        return []
    parts = [p.strip() for p in SPLIT_PAT.split(text) if p.strip()]
    chunks, buf = [], ""
    for p in parts:
        if len(buf) + len(p) + 1 <= max_chars:
            buf = (buf + " " + p).strip() if buf else p
        else:
            if buf: chunks.append(buf)
            buf = p
    if buf: chunks.append(buf)
    # 合并过短块
    merged = []
    for c in chunks:
        if merged and len(c) < min_chars and len(merged[-1]) + len(c) + 1 <= int(max_chars*1.5):
            merged[-1] = (merged[-1] + " " + c).strip()
        else:
            merged.append(c)
    return merged

# -------- documents upsert ----------
DOCS_SQL_WITH_ID = """
INSERT INTO documents
(id, url, domain, fetched_at_iso, title, lang, text, extra_json, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
ON CONFLICT (url) DO UPDATE SET
  domain         = EXCLUDED.domain,
  fetched_at_iso = EXCLUDED.fetched_at_iso,
  title          = EXCLUDED.title,
  lang           = EXCLUDED.lang,
  text           = EXCLUDED.text,
  extra_json     = COALESCE(EXCLUDED.extra_json, documents.extra_json),
  updated_at     = now()
RETURNING id;
"""

DOCS_SQL_AUTO_ID = """
INSERT INTO documents
(url, domain, fetched_at_iso, title, lang, text, extra_json, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, now(), now())
ON CONFLICT (url) DO UPDATE SET
  domain         = EXCLUDED.domain,
  fetched_at_iso = EXCLUDED.fetched_at_iso,
  title          = EXCLUDED.title,
  lang           = EXCLUDED.lang,
  text           = EXCLUDED.text,
  extra_json     = COALESCE(EXCLUDED.extra_json, documents.extra_json),
  updated_at     = now()
RETURNING id;
"""

def upsert_document(cur, rec: dict) -> int:
    """
    优先使用 JSON 里的 id 写入 documents.id，保证与 chunks.jsonl 对齐。
    若文件没有 id，才走自增 id 的分支。
    """
    url   = ensure_url(rec.get("url"), rec.get("title") or "", rec.get("text") or "")
    domain= rec.get("domain")
    fetched_at_iso = rec.get("fetched_at_iso") or to_iso(rec.get("fetched_at"))
    title = rec.get("title")
    lang  = rec.get("lang")
    text  = rec.get("text") or ""
    extra = rec.get("extra_json")

    extra_json = json.dumps(extra, ensure_ascii=False) if extra is not None else None

    # 是否有传入 id
    rid = rec.get("id")
    if rid is not None and str(rid).strip() != "":
        rid = int(rid)
        cur.execute(DOCS_SQL_WITH_ID, (rid, url, domain, fetched_at_iso, title, lang, text, extra_json))
    else:
        cur.execute(DOCS_SQL_AUTO_ID, (url, domain, fetched_at_iso, title, lang, text, extra_json))

    return cur.fetchone()[0]

# -------- chunks upsert ----------
CHUNK_UPSERT = """
INSERT INTO chunks
(doc_id, chunk_index, content, char_start, char_end, token_estimate, content_md5, created_at, updated_at)
VALUES (%s,%s,%s,%s,%s,%s,%s, now(), now())
ON CONFLICT (doc_id, chunk_index) DO UPDATE SET
  content        = EXCLUDED.content,
  char_start     = EXCLUDED.char_start,
  char_end       = EXCLUDED.char_end,
  token_estimate = EXCLUDED.token_estimate,
  content_md5    = EXCLUDED.content_md5,
  updated_at     = now();
"""

def bulk_upsert_chunks(cur, rows: list):
    if not rows: return 0
    execute_batch(cur, CHUNK_UPSERT, rows, page_size=500)
    return len(rows)

def ingest_chunks_from_file(cur, path: str, url2id: dict) -> int:
    """从 chunks.jsonl 导入：字段为 document_id/chunk_index/content/char_start/char_end"""
    total = 0
    batch = []
    with open(path, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Import chunks.jsonl"):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            doc_id = obj.get("document_id") or obj.get("doc_id")
            if isinstance(doc_id, str) and doc_id.startswith("http"):
                doc_id = url2id.get(doc_id)
            if not doc_id:
                continue
            content = obj.get("content") or ""
            cstart  = obj.get("char_start")
            cend    = obj.get("char_end")
            token_est = est_tokens_by_chars(content)
            md5 = hashlib.md5(content.encode("utf-8")).hexdigest()
            batch.append((int(doc_id), int(obj.get("chunk_index", 0)), content, cstart, cend, token_est, md5))
            if len(batch) >= 2000:
                total += bulk_upsert_chunks(cur, batch); batch = []
    if batch:
        total += bulk_upsert_chunks(cur, batch)
    return total

def auto_chunk_all(cur, max_chars=1000, overlap=100) -> int:
    """从 documents.text 自动切块入 chunks（按已存在 documents 全量）"""
    total = 0
    cur2 = cur.connection.cursor()
    cur2.execute("SELECT id, text FROM documents WHERE text IS NOT NULL;")
    rows = cur2.fetchall()
    batch = []
    for doc_id, text in tqdm(rows, desc="Auto chunking from documents"):
        if not text: continue
        pieces = split_text(text, max_chars=max_chars, min_chars=max_chars//2)
        cursor = 0
        for idx, piece in enumerate(pieces):
            start = text.find(piece, cursor)
            if start < 0:
                start = text.find(piece)
            end = start + len(piece)
            cursor = max(cursor, end)
            token_est = est_tokens_by_chars(piece)
            md5 = hashlib.md5(piece.encode("utf-8")).hexdigest()
            batch.append((doc_id, idx, piece, start, end, token_est, md5))
            if len(batch) >= 2000:
                total += bulk_upsert_chunks(cur, batch); batch = []
    if batch:
        total += bulk_upsert_chunks(cur, batch)
    return total

# -------- main ----------
def main():
    ap = argparse.ArgumentParser(description="Ingest JSONL into existing documents/chunks tables.")
    ap.add_argument("--docs", required=True, help="Path to documents_min.jsonl")
    ap.add_argument("--chunks", default=None, help="Optional: chunks.jsonl (if omitted, will auto-chunk from documents)")
    ap.add_argument("--auto-chunk-size", type=int, default=1000, help="Auto chunk size in chars when --chunks is not provided")
    args = ap.parse_args()

    conn = connect()
    conn.autocommit = False
    cur = conn.cursor()

    # 1) 导入 documents
    print(f"[*] Import documents from: {args.docs}")
    url2id = {}
    docs_cnt = 0
    with open(args.docs, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Upsert documents"):
            line = line.strip()
            if not line: continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            doc_id = upsert_document(cur, rec)
            docs_cnt += 1
            if rec.get("url"):
                url2id[rec["url"]] = doc_id
            if docs_cnt % 500 == 0:
                conn.commit()
    conn.commit()
    print(f"[+] documents upserted: {docs_cnt}")

    cur.execute("""
      SELECT setval(pg_get_serial_sequence('documents','id'), COALESCE((SELECT MAX(id) FROM documents), 0));
    """)
    conn.commit()

    # 2) 导入 chunks（来自文件，或自动从 documents.text 生成）
    if args.chunks:
        print(f"[*] Import chunks from: {args.chunks}")
        ch_cnt = ingest_chunks_from_file(cur, args.chunks, url2id)
        conn.commit()
        print(f"[+] chunks upserted: {ch_cnt}")
    else:
        print(f"[*] Auto-chunk from documents (size={args.auto_chunk_size})")
        ch_cnt = auto_chunk_all(cur, max_chars=args.auto_chunk_size)
        conn.commit()
        print(f"[+] chunks upserted: {ch_cnt}")

    cur.close()
    conn.close()
    print("✅ Done.")

if __name__ == "__main__":
    main()

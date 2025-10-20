# ingest_jsonl_to_polardb.py
# ------------------------------------------------------------
# 用法（Windows PowerShell 示例）：
#   pip install psycopg2-binary python-dateutil tqdm
#
#   $env:PGHOST="nioniedb.rwlb.rds.aliyuncs.com"
#   $env:PGPORT="5432"
#   $env:PGUSER="nionie1"
#   $env:PGPASSWORD="<你的数据库密码>"
#   $env:PGDATABASE="postgres"
#
#   python ingest_jsonl_to_polardb.py --jsonl "C:\Users\Dell\Desktop\heritage.jsonl" --chunk-size 1000 --overlap 100
#
# 依赖前提：
# 1) 已在 PolarDB 执行建表脚本（documents / chunks）
# 2) 已执行：CREATE EXTENSION IF NOT EXISTS vector;（可选）
# ------------------------------------------------------------

import os
import json
import argparse
import math
import hashlib
import datetime
from typing import List, Tuple, Optional

import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm


def make_chunks(text: Optional[str], size: int = 1000, overlap: int = 100) -> List[Tuple[int, int, str, int, str]]:
    """
    将长文本切块：
    返回列表元素为 (char_start, char_end, chunk_text, token_estimate, md5)
    token_estimate 采用 len(chars)/4 的粗略估计，便于后续控制向量化成本。
    """
    if not text:
        return []
    text = str(text)
    n = len(text)
    if n == 0:
        return []
    chunks = []
    start = 0
    while start < n:
        end = min(start + size, n)
        chunk = text[start:end]
        token_est = max(1, math.ceil(len(chunk) / 4))
        md5 = hashlib.md5(chunk.encode("utf-8")).hexdigest()
        chunks.append((start, end, chunk, token_est, md5))
        if end == n:
            break
        # 产生重叠
        start = max(0, end - overlap)
    return chunks


def connect():
    """使用环境变量建立数据库连接。"""
    conn = psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", "5432")),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
        dbname=os.environ.get("PGDATABASE", "postgres"),
    )
    conn.autocommit = False
    return conn


def to_iso_from_unix(ts) -> Optional[str]:
    """将 Unix 时间戳（秒）转为 ISO 字符串，失败则返回 None。"""
    try:
        if ts is None:
            return None
        v = float(ts)
        return datetime.datetime.utcfromtimestamp(v).replace(microsecond=0).isoformat() + "Z"
    except Exception:
        return None


def ensure_url(obj: dict) -> str:
    """
    确保有可用于幂等 upsert 的 URL。
    如果原始数据缺失 url，则基于 title+text 计算 md5 并合成占位 url：
      missing://<md5>
    这样 ON CONFLICT (url) 仍然能工作，避免重复写入。
    """
    url = obj.get("url")
    if url and isinstance(url, str) and url.strip():
        return url.strip()
    # 尝试用 title + text 生成稳定占位
    base = (obj.get("title") or "") + "|" + (obj.get("text") or obj.get("html") or obj.get("raw_html") or "")
    md5 = hashlib.md5(base.encode("utf-8")).hexdigest()
    return f"missing://{md5}"


def upsert_document(cur, obj) -> int:
    """
    将一条 JSON 对象写入 documents 并返回 doc_id。
    若 url 冲突则更新；保留 extra_json 以存储未显式建表的字段。
    """
    url = ensure_url(obj)
    domain = obj.get("domain")
    fetched_at = obj.get("fetched_at")
    fetched_at_iso = to_iso_from_unix(fetched_at)

    status = obj.get("status")
    content_type = obj.get("content_type")
    title = obj.get("title")
    lang = obj.get("lang")
    text = obj.get("text")
    raw_html = obj.get("html") or obj.get("raw_html")

    known = {"url", "domain", "fetched_at", "status", "content_type", "title", "lang", "text", "html", "raw_html"}
    extra = {k: v for k, v in obj.items() if k not in known} or None

    cur.execute(
        """
        INSERT INTO documents
        (url, domain, fetched_at, fetched_at_iso, status, content_type, title, lang, text, raw_html, extra_json, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_jsonb(%s::json), now(), now())
        ON CONFLICT (url) DO UPDATE SET
            domain = EXCLUDED.domain,
            fetched_at = EXCLUDED.fetched_at,
            fetched_at_iso = EXCLUDED.fetched_at_iso,
            status = EXCLUDED.status,
            content_type = EXCLUDED.content_type,
            title = EXCLUDED.title,
            lang = EXCLUDED.lang,
            text = EXCLUDED.text,
            raw_html = EXCLUDED.raw_html,
            extra_json = EXCLUDED.extra_json,
            updated_at = now()
        RETURNING id;
        """,
        (
            url,
            domain,
            fetched_at,
            fetched_at_iso,
            status,
            content_type,
            title,
            lang,
            text,
            raw_html,
            json.dumps(extra, ensure_ascii=False),
        ),
    )
    doc_id = cur.fetchone()[0]
    return doc_id


def insert_chunks(cur, doc_id: int, base_text: Optional[str], size: int, overlap: int) -> int:
    """将文本切块写入 chunks 表。"""
    rows = []
    if base_text:
        for idx, (start, end, chunk, token_est, md5) in enumerate(make_chunks(base_text, size=size, overlap=overlap)):
            rows.append((doc_id, idx, chunk, start, end, token_est, md5))

    if not rows:
        return 0

    execute_batch(
        cur,
        """
        INSERT INTO chunks
        (doc_id, chunk_index, content, char_start, char_end, token_estimate, content_md5, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT DO NOTHING
        """,
        rows,
        page_size=500,
    )
    return len(rows)


def main():
    ap = argparse.ArgumentParser(description="Import JSONL into PolarDB (documents + chunks).")
    ap.add_argument("--jsonl", required=True, help="Path to the JSONL file.")
    ap.add_argument("--chunk-size", type=int, default=1000, help="Chunk size in characters (default: 1000).")
    ap.add_argument("--overlap", type=int, default=100, help="Overlap between chunks in characters (default: 100).")
    args = ap.parse_args()

    if not os.path.exists(args.jsonl):
        raise FileNotFoundError(f"JSONL not found: {args.jsonl}")

    conn = connect()
    cur = conn.cursor()

    inserted_docs = 0
    inserted_chunks = 0
    bad_lines = 0

    with open(args.jsonl, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Importing"):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                bad_lines += 1
                continue

            # Upsert document
            try:
                doc_id = upsert_document(cur, obj)
            except Exception as e:
                conn.rollback()
                bad_lines += 1
                continue

            # Prefer plain text; fallback to html/raw_html if no text
            base_text = obj.get("text") or obj.get("html") or obj.get("raw_html")

            try:
                c = insert_chunks(cur, doc_id, base_text, args.chunk_size, args.overlap)
            except Exception:
                conn.rollback()
                bad_lines += 1
                continue

            inserted_docs += 1
            inserted_chunks += c

            if inserted_docs % 100 == 0:
                conn.commit()

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Done. docs={inserted_docs}, chunks={inserted_chunks}, bad_lines={bad_lines}")


if __name__ == "__main__":
    main()

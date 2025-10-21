#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
一键把“清晰版 JSONL”转换为：
  1) documents_min.jsonl（导入 documents 表）

  2) chunks.jsonl（导入 chunks 表）

默认路径已按你的项目设置，如需修改，改下面 CONFIG 部分即可。
"""

import sys, json, re, argparse, hashlib, datetime
from pathlib import Path

# ========== CONFIG（按需修改） ==========
BASE = Path(r"C:\Users\Dell\Desktop\heritage_crawl")
INPUT_JSONL = BASE / r"data\text\ihchina.cn.jsonl"             # 你的清晰版 JSONL（改成你的文件名）
OUT_DOC = BASE / r"data\text\documents_min.jsonl"                 # 输出 documents
OUT_CH = BASE / r"data\text\chunks.jsonl"                         # 输出 chunks
KEEP_EXTRA_JSON = False                                            # 是否把 meta/bearers 放到 documents.extra_json
# ========== CONFIG END ==========

SPLIT_PAT = re.compile(r"(?:\n{2,}|。|；|！|\?|？)")  # 粗略断句
MAX_CHARS = 1000
MIN_CHARS = 600

def stable_id(obj):
    """优先用已有 id；没有就对 url 做64位hash生成稳定 id。"""
    if "id" in obj and obj["id"] not in (None, ""):
        try:
            return int(obj["id"])
        except Exception:
            pass
    url = (obj.get("url") or "").strip()
    if not url:
        # 无 url 时，退化到整条 json 的 md5 前 16 位
        h = hashlib.md5(json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8")).hexdigest()[:16]
    else:
        h = hashlib.sha1(url.encode("utf-8")).hexdigest()[:16]
    return int(h, 16) & ((1<<63)-1)  # 压到 64bit

def to_iso(ts):
    """将 epoch 秒/毫秒转换为 ISO8601；非数值返回 None。"""
    if ts is None:
        return None
    try:
        t = float(ts)
        if t > 1e12:  # 毫秒
            t = t / 1000.0
        return datetime.datetime.utcfromtimestamp(t).isoformat() + "Z"
    except Exception:
        return None

def split_text(text):
    if not text:
        return []
    parts = [p.strip() for p in SPLIT_PAT.split(text) if p.strip()]
    chunks = []
    buf = ""
    for p in parts:
        if len(buf) + len(p) + 1 <= MAX_CHARS:
            buf = (buf + " " + p).strip() if buf else p
        else:
            if buf: chunks.append(buf)
            buf = p
    if buf: chunks.append(buf)
    # 合并过短块
    merged = []
    for c in chunks:
        if merged and len(c) < MIN_CHARS and len(merged[-1]) + len(c) + 1 <= int(MAX_CHARS*1.5):
            merged[-1] = (merged[-1] + " " + c).strip()
        else:
            merged.append(c)
    return merged

def convert(input_path: Path, out_doc: Path, out_ch: Path, keep_extra_json: bool = True):
    input_path = Path(input_path)
    out_doc = Path(out_doc)
    out_ch = Path(out_ch)

    if not input_path.exists():
        raise FileNotFoundError(f"Input not found: {input_path}")

    out_doc.parent.mkdir(parents=True, exist_ok=True)
    out_ch.parent.mkdir(parents=True, exist_ok=True)

    total_docs = 0
    total_chunks = 0

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(out_doc, "w", encoding="utf-8") as docs_out, \
         open(out_ch, "w", encoding="utf-8") as ch_out:

        for line in fin:
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue

            doc_id = stable_id(obj)
            url = obj.get("url") or ""
            title = obj.get("title") or ""
            lang = obj.get("lang") or ""
            domain = obj.get("domain") or ""
            text = obj.get("text") or ""
            fetched_at_iso = obj.get("fetched_at_iso") or to_iso(obj.get("fetched_at"))

            # 写 documents_min.jsonl
            doc_rec = {
                "id": doc_id,
                "url": url,
                "title": title,
                "lang": lang,
                "domain": domain,
                "fetched_at_iso": fetched_at_iso,
                "text": text
            }
            if keep_extra_json:
                extra = {}
                if isinstance(obj.get("meta"), dict) and obj.get("meta"):
                    extra["meta"] = obj["meta"]
                if isinstance(obj.get("bearers"), list) and obj.get("bearers"):
                    extra["bearers"] = obj["bearers"]
                if extra:
                    doc_rec["extra_json"] = extra

            docs_out.write(json.dumps(doc_rec, ensure_ascii=False) + "\n")
            total_docs += 1

            # 切分 text -> chunks.jsonl
            produced = split_text(text)
            cursor = 0
            for idx, piece in enumerate(produced):
                start = text.find(piece, cursor)
                if start < 0:  # 兜底
                    start = text.find(piece)
                end = start + len(piece)
                cursor = max(cursor, end)
                ch_rec = {
                    "document_id": doc_id,
                    "chunk_index": idx,
                    "content": piece,
                    "char_start": start,
                    "char_end": end
                }
                ch_out.write(json.dumps(ch_rec, ensure_ascii=False) + "\n")
                total_chunks += 1

    return {
        "input": str(input_path),
        "documents_min": str(out_doc),
        "chunks": str(out_ch),
        "docs": total_docs,
        "chunks_count": total_chunks
    }

def main():
    print("=== Convert to documents & chunks ===")
    print(f"Input: {INPUT_JSONL}")
    try:
        res = convert(INPUT_JSONL, OUT_DOC, OUT_CH, keep_extra_json=KEEP_EXTRA_JSON)
    except Exception as e:
        print(f"[ERROR] {e}")
        if sys.platform.startswith("win"):
            input("按回车退出...")
        sys.exit(1)

    print(json.dumps(res, ensure_ascii=False, indent=2))
    if sys.platform.startswith("win"):
        input("完成，按回车退出...")

if __name__ == "__main__":
    main()

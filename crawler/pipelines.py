# crawler/pipelines.py
import os, sqlite3, hashlib, orjson, time, tldextract
from pathlib import Path
from langdetect import detect
from trafilatura import extract
from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter  # 方便安全地读/写 Item 字段

DB_PATH = "db/crawl.sqlite"
TEXT_DIR = Path("data/text")
RAW_DIR = Path("data/raw")

class DedupeAndStorePipeline:
    def open_spider(self, spider):
        Path("db").mkdir(exist_ok=True, parents=True)
        TEXT_DIR.mkdir(exist_ok=True, parents=True)
        RAW_DIR.mkdir(exist_ok=True, parents=True)
        self.conn = sqlite3.connect(DB_PATH)
        self.conn.execute("""CREATE TABLE IF NOT EXISTS seen(
            url TEXT PRIMARY KEY, checksum TEXT, fetched_at REAL)""")
        # 用“文本内容”的 checksum 做内容去重索引
        self.conn.execute("""CREATE TABLE IF NOT EXISTS text_index(
            checksum TEXT PRIMARY KEY, url TEXT, title TEXT, lang TEXT)""")
        self.conn.commit()

    def close_spider(self, spider):
        self.conn.close()

    def _seen(self, url):
        cur = self.conn.execute("SELECT 1 FROM seen WHERE url=?", (url,))
        return cur.fetchone() is not None

    def _mark_seen(self, url, checksum):
        self.conn.execute(
            "INSERT OR REPLACE INTO seen(url, checksum, fetched_at) VALUES (?,?,?)",
            (url, checksum, time.time())
        )
        self.conn.commit()

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        # 1) URL 级去重
        url = adapter.get("url")
        if not url:
            raise DropItem("no-url")
        if self._seen(url):
            raise DropItem("dup-url")

        # 2) 读取原始 HTML（用于回退抽取 & 生成原始校验）
        html_path = adapter.get("html_path")
        raw = b""
        if html_path and os.path.exists(html_path):
            with open(html_path, "rb") as f:
                raw = f.read()
        raw_checksum = hashlib.sha1(raw).hexdigest() if raw else None

        # 3) 选择“要保存的文本”
        #    优先用蜘蛛给的 text_lines / bodytext / text，最后才回退 trafilatura
        text = None
        lines = adapter.get("text_lines") or adapter.get("bodytext")
        if lines and isinstance(lines, (list, tuple)):
            text = "\n".join(str(x) for x in lines).strip()

        if not text:
            t = adapter.get("text")
            if isinstance(t, (bytes, bytearray)):
                try:
                    t = t.decode("utf-8", "ignore")
                except Exception:
                    t = str(t)
            text = (t or "").strip()

        if not text:
            # 回退：从原始 HTML 里抽
            if raw:
                try:
                    text = extract(raw.decode("utf-8", errors="ignore"),
                                   include_links=False, include_images=False) or ""
                except Exception:
                    text = ""
            else:
                text = ""

        # 4) 文本长度检查（过短丢弃）
        if len(text.strip()) < 20:
            # 仍然把 URL 标记为已见，避免重复抓
            self._mark_seen(url, raw_checksum or "")
            raise DropItem("short")

        # 5) 语言检测
        try:
            lang = detect(text)
        except Exception:
            lang = "und"

        adapter["lang"] = lang  # 这行安全，即使 item 未定义该字段也不会报错（ItemAdapter 兜底）

        # 6) 内容级 checksum（基于文本内容）
        checksum = hashlib.sha1(text.encode("utf-8")).hexdigest()

        # 7) 内容去重：相同文本就视为重复
        cur = self.conn.execute("SELECT 1 FROM text_index WHERE checksum=?", (checksum,))
        if cur.fetchone():
            self._mark_seen(url, raw_checksum or checksum)
            raise DropItem("dup-content")

        # 8) 回写 item（尽量不破坏蜘蛛的原始字段；若字段不存在也不会抛错）
        adapter["text"] = text
        # 如果 item 支持这些字段，再写入
        if "checksum" in getattr(adapter.item, "fields", {}):
            adapter["checksum"] = checksum
        if "raw_checksum" in getattr(adapter.item, "fields", {}):
            adapter["raw_checksum"] = raw_checksum

        # 9) 组织输出 JSON（一并输出 bodytext/text_lines，便于你核对）
        out = {
            "url": url,
            "domain": adapter.get("domain"),
            "fetched_at": adapter.get("fetched_at"),
            "status": adapter.get("status"),
            "content_type": adapter.get("content_type"),
            "title": adapter.get("title"),
            "lang": lang,
            "text": text,
            "checksum": checksum,
            "license": adapter.get("license"),
            "robots": adapter.get("robots"),
            "outlinks": adapter.get("outlinks", []) or [],
            "meta": adapter.get("meta"),
            "bearers": adapter.get("bearers"),
            # 新增两个辅助观测字段（若 spider 有给的话）
            "bodytext": adapter.get("bodytext"),
            "text_lines": adapter.get("text_lines"),
        }

        # 10) 以 domain 归档写入 JSONL
        dom = adapter.get("domain") or tldextract.extract(url).domain
        path = TEXT_DIR / f"{dom}.jsonl"
        with open(path, "ab") as fw:
            fw.write(orjson.dumps(out) + b"\n")

        # 11) 写入内容索引
        self.conn.execute(
            "INSERT OR REPLACE INTO text_index(checksum, url, title, lang) VALUES (?,?,?,?)",
            (checksum, url, adapter.get("title"), lang)
        )
        self.conn.commit()

        # 12) 标记 URL 已见（记录原始校验或内容校验）
        self._mark_seen(url, raw_checksum or checksum)
        return item

import re
import time
import scrapy
import tldextract
from pathlib import Path
from w3lib.html import remove_tags
from ..items import PageItem


META_KEYS = [
    "项目序号", "项目编号", "公布时间", "公布批次", "批次",
    "类别", "所属地区", "类型", "申报地区或单位", "保护单位"
]
META_KEYS_SET = set(META_KEYS)

BEARER_KEYS = [
    "编号", "姓名", "性别", "出生日期", "民族",
    "类别", "项目编号", "项目名称", "申报地区或单位"
]
BEARER_KEYS_SET = set(BEARER_KEYS)


class FocusedSpider(scrapy.Spider):
    name = "focused"
    allowed_domains = ["ihchina.cn"]

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "LOG_LEVEL": "INFO",
        "ITEM_PIPELINES": {"crawler.pipelines.DedupeAndStorePipeline": 300},
        "CLOSESPIDER_ITEMCOUNT": 1,
        "REQUEST_FINGERPRINTER_IMPLEMENTATION": "2.7",
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    def __init__(self, start=13774, end=13774,
                 enrich_mode="none",          # NEW: none|append|replace
                 enrich_format="readable",    # NEW: readable|json
                 add_json_summary="0",        # NEW: "1"/"0"
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_id = int(start)
        self.end_id = int(end)

        # NEW: 参数归一化
        self.enrich_mode = (enrich_mode or "none").lower()     # none/append/replace
        self.enrich_format = (enrich_format or "readable").lower()
        self.add_json_summary = str(add_json_summary).strip() in ("1","true","yes","on")

        # 常量
        self.META_ORDER = ["项目序号", "项目编号", "公布时间", "类别", "所属地区", "类型", "申报地区或单位", "保护单位"]
        self.BEARER_ORDER = ["编号", "姓名", "性别", "出生日期", "民族", "类别", "项目编号", "项目名称",
                             "申报地区或单位"]
        self.RULER = "\n\n——\n\n"

        # 预编译……
        label_alt = "|".join(map(re.escape, META_KEYS))
        self.meta_kv_regex = re.compile(rf"({label_alt})\s*[：:]\s*")

        bearer_label_alt = "|".join(map(re.escape, BEARER_KEYS))
        self.bearer_prefix_regex = re.compile(rf"^({bearer_label_alt})\s*[：:\u3000 ]*")

        # ---------- NEW: 可读文本块 ----------
    def _block_meta_readable(self, meta: dict) -> str:
            if not isinstance(meta, dict) or not meta:
                return ""
            order = ["项目序号", "项目编号", "公布时间", "公布批次", "批次",
                     "类别", "所属地区", "类型", "申报地区或单位", "保护单位"]
            lines = []
            for k in order:
                v = meta.get(k)
                if v and str(v).strip():
                    lines.append(f"{k}：{str(v).strip()}")
            # 其余键追加
            for k, v in meta.items():
                if k not in order and v and str(v).strip():
                    lines.append(f"{k}：{str(v).strip()}")
            return "【项目基本信息】\n" + "\n".join(lines) if lines else ""

    def _block_bearers_readable(self, bearers: list) -> str:
            if not isinstance(bearers, list) or not bearers:
                return ""
            order = ["编号", "姓名", "性别", "出生日期", "民族", "类别", "项目编号", "项目名称", "申报地区或单位"]
            lines = []
            for b in bearers:
                if not isinstance(b, dict):
                    continue
                head = []
                name = (b.get("姓名") or "").strip()
                gender = (b.get("性别") or "").strip()
                eth = (b.get("民族") or "").strip()
                dob = (b.get("出生日期") or "").strip()
                if name:
                    s = f"姓名：{name}"
                    attrs = []
                    if gender: attrs.append(f"性别：{gender}")
                    if eth:    attrs.append(f"民族：{eth}")
                    if dob:    attrs.append(f"出生日期：{dob}")
                    if attrs:
                        s += "（" + "，".join(attrs) + "）"
                    head.append(s)
                for k in order:
                    if k in ("姓名", "性别", "民族", "出生日期"):
                        continue
                    v = b.get(k)
                    if v and str(v).strip():
                        head.append(f"{k}：{str(v).strip()}")
                # 额外键
                for k, v in b.items():
                    if k in order:
                        continue
                    if v and str(v).strip():
                        head.append(f"{k}：{str(v).strip()}")
                if head:
                    lines.append("- " + "；".join(head))
            return "【代表性传承人】\n" + "\n".join(lines) if lines else ""

        # ---------- NEW: JSON 文本块 ----------
    def _block_meta_json(self, meta: dict) -> str:
            if not isinstance(meta, dict) or not meta:
                return ""
            import json
            return "【项目基本信息-JSON】\n" + json.dumps(meta, ensure_ascii=False)

    def _block_bearers_json(self, bearers: list) -> str:
            if not isinstance(bearers, list) or not bearers:
                return ""
            import json
            return "【代表性传承人-JSON】\n" + json.dumps(bearers, ensure_ascii=False)

    def _block_summary_json(self, meta: dict, bearers: list) -> str:
            import json
            payload = {}
            if isinstance(meta, dict) and meta: payload["meta"] = meta
            if isinstance(bearers, list) and bearers: payload["bearers"] = bearers
            return "【JSON摘要】\n" + json.dumps(payload, ensure_ascii=False) if payload else ""

    # ---------------- 工具函数 ----------------
    def _norm(self, s: str | None) -> str:
        if not s:
            return ""
        s = s.replace("\u3000", " ").replace("\xa0", " ")
        return " ".join(s.split())

    # —— 正文抽取 —— #
    def _pick_body_container(self, response, main_sel):
        # 同时抓 .inherit_xx1(.article-mod2) 与 .inherit_xx2
        nodes = response.css(".inherit_xx1, .inherit_xx2")
        if nodes:
            return nodes  # SelectorList
        # 其次：老模板
        container = response.css(".inherit_xx1 .text")
        if container:
            return container
        cands = [
            ".text", ".project_content", ".projectContent",
            ".article .content", ".details .text", ".details .content",
            ".container .content", ".content"
        ]
        for css in cands:
            c = main_sel.css(css)
            if c:
                return c
        return main_sel or response

    def _html_to_lines(self, html: str) -> list[str]:
        html = (html or "").replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
        text = remove_tags(html)
        return [self._norm(x) for x in text.splitlines() if self._norm(x)]

    def _extract_body_paragraphs(self, response, main_sel) -> list[str]:
        """
        同时支持多个正文容器（如 .inherit_xx1.article-mod2 与 .inherit_xx2），
        优先按 <div class="p"> 分段，保留 <br> 为换行。
        """
        containers = self._pick_body_container(response, main_sel)

        # 统一成列表（Selector 或 SelectorList 都兼容）
        try:
            it = list(containers)  # SelectorList -> list[Selector]
        except TypeError:
            it = [containers]

        paras: list[str] = []
        for cont in it:
            # 兼容 .inherit_xx1.article-mod2 里可能还有 .text 包一层
            c = cont.css(".text") or cont

            p_blocks = c.css(".p")
            if p_blocks:
                for blk in p_blocks:
                    lines = self._html_to_lines(blk.get() or "")
                    para = self._norm(" ".join(lines))  # 同段合并
                    if para:
                        paras.append(para)
            else:
                paras.extend(self._html_to_lines(c.get() or ""))
        return paras

    # —— 项目基本信息表 —— #
    def _parse_meta_row_by_pairs(self, cells: list[str]) -> dict:
        """
        情况A：一行多格，形如 [项目序号：, 283, 项目编号：, Ⅵ-1, ...]
        """
        out = {}
        i = 0
        while i < len(cells):
            key_raw = cells[i].rstrip("：:").strip()
            if key_raw in META_KEYS_SET:
                val = cells[i + 1].strip() if i + 1 < len(cells) else ""
                # 如果下一格仍是 key，则视为该 key 值缺失
                nxt = cells[i + 1].rstrip("：:").strip() if i + 1 < len(cells) else ""
                if nxt in META_KEYS_SET:
                    val = ""
                    i += 1
                else:
                    i += 2
                out[key_raw] = self._norm(val)
                continue
            i += 1
        return out

    def _parse_meta_row_inside_cell(self, cell_text: str) -> dict:
        """
        情况B：同一个 <td> 里塞了多对“键：值”
        """
        out = {}
        text = cell_text
        matches = list(self.meta_kv_regex.finditer(text))
        if not matches:
            return out
        for idx, m in enumerate(matches):
            key = m.group(1)
            start = m.end()
            end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
            val = self._norm(text[start:end])
            if key in META_KEYS_SET:
                out[key] = val
        return out

    def _kv_table(self, sel) -> dict:
        data = {}
        for tr in sel.css("tr"):
            # 每个格子独立取文本，便于“跨格成对”解析
            cells = [self._norm("".join(td.css("*::text, ::text").getall())) for td in tr.css("th, td")]
            cells = [c for c in cells if c]
            if not cells:
                continue

            # 先尝试“跨格成对”
            got = self._parse_meta_row_by_pairs(cells)
            for k, v in got.items():
                if k not in data and v != "":
                    data[k] = v

            # 再尝试“同格多对”
            for c in cells:
                got2 = self._parse_meta_row_inside_cell(c)
                for k, v in got2.items():
                    if k not in data or not data[k]:
                        data[k] = v
        return data

    # —— 代表性传承人表 —— #
    def _strip_bearer_prefix(self, value: str) -> str:
        return self.bearer_prefix_regex.sub("", value).strip()

    def _derive_headers_from_first_row(self, cells: list[str]) -> list[str]:
        """
        当没有明确表头行时，尝试从第一行的“前缀”里推断列名。
        """
        headers = []
        for c in cells:
            m = self.bearer_prefix_regex.match(c)
            if m:
                hdr = self._norm(m.group(1))
                headers.append(hdr)
            else:
                headers.append("")  # 占位
        # 回填空白列名
        for i, h in enumerate(headers):
            if not h:
                headers[i] = f"列{i+1}"
        return headers

    def _table_rows(self, sel) -> list:
        rows = []
        # 先看看第一行是否是表头
        first_tr = sel.css("tr:first-child")
        header_cells = [self._norm("".join(c.css("*::text, ::text").getall()))
                        for c in first_tr.css("th, td")]
        header_cells = [h.replace("：", "").strip() for h in header_cells if self._norm(h)]

        headers = []
        if header_cells and sum(1 for h in header_cells if h in BEARER_KEYS_SET) >= max(3, len(header_cells)//2):
            # 视作有效表头
            headers = header_cells
            data_trs = sel.css("tr")[1:]
        else:
            # 没有表头：用第一行推断列名
            data_trs = sel.css("tr")
            if data_trs:
                first_cells = [self._norm("".join(c.css("*::text, ::text").getall()))
                               for c in data_trs[0].css("td")]
                headers = self._derive_headers_from_first_row(first_cells)

        for tr in data_trs:
            cells = [self._norm("".join(c.css("*::text, ::text").getall())) for c in tr.css("td")]
            if not cells:
                continue
            row = {}
            for i, val in enumerate(cells):
                key = headers[i] if i < len(headers) else f"列{i+1}"
                # 若值以列名作前缀，剥掉
                cleaned = self._strip_bearer_prefix(val)
                row[key] = cleaned
            if any(v for v in row.values()):
                rows.append(row)
        return rows

    # ---------------- 解析主逻辑 ----------------
    def start_requests(self):
        base = "https://www.ihchina.cn/project_details/{}.html"
        for i in range(self.start_id, self.end_id + 1):
            url = base.format(i)
            self.logger.info(f"[REQ] {url}")
            yield scrapy.Request(url, callback=self.parse_article, dont_filter=True)

    def parse_article(self, response):
        self.logger.info(f"[RESP] {response.status} {response.url}")
        if response.status != 200:
            return

        # 保存原始 HTML
        ts = str(int(time.time()))
        ext = tldextract.extract(response.url)
        dom = f"{ext.domain}.{ext.suffix}"
        raw_dir = Path("data/raw") / dom
        raw_dir.mkdir(parents=True, exist_ok=True)
        html_path = raw_dir / f"{ts}.html"
        html_path.write_bytes(response.body)

        # 标题/时间
        title = (
            response.css("h1::text").get()
            or response.css("meta[property='og:title']::attr(content)").get()
            or response.css("title::text").get()
            or ""
        )
        title = self._norm(title)
        pub_time = self._norm(response.css("time::text, .date::text, .pubtime::text").get() or "")

        # 主体容器
        main = response.css(
            ".project_detail, .project-details, .projectDetails, "
            ".details, .article, .content, .project_content, .container"
        ) or response

        # 基本信息 & 传承人
        tables = main.css("table")
        meta = {}
        bearers = []
        if tables:
            meta = self._kv_table(tables[0]) if tables else {}
            # 其余表作为“传承人”候选
            for tb in tables[1:]:
                header_text = " ".join(tb.css("tr:first-child *::text, tr:first-child::text").getall())
                if any(k in header_text for k in BEARER_KEYS) or len(tb.css("tr")) >= 2:
                    rows = self._table_rows(tb)
                    if rows:
                        bearers.extend(rows)

        # 正文
        body_paras = self._extract_body_paragraphs(response, main)
        # 2) 观测：日志里直观看到是否抓到了正文
        self.logger.info(f"[BODY] got {len(body_paras)} paras; first={body_paras[0][:40] if body_paras else ''}")

        # ---------- NEW: 合成最终 text ----------
        RULER = "\n\n——\n\n"  # 分隔线
        # 基线文本（不含 meta 行，避免和 enriched 块重复）
        base_body = "\n".join(body_paras).strip()

        # 构建可选块
        if self.enrich_format == "json":
            meta_block = self._block_meta_json(meta)
            bearers_block = self._block_bearers_json(bearers)
        else:
            meta_block = self._block_meta_readable(meta)
            bearers_block = self._block_bearers_readable(bearers)

        blocks = [b for b in (meta_block, bearers_block) if b]

        if self.enrich_mode == "replace":
            # 仅保留结构化块（无原始正文）
            text_clean = RULER.join(blocks).strip() if blocks else base_body
        elif self.enrich_mode == "append":
            # 原始正文 + 结构化块
            if base_body and blocks:
                text_clean = (base_body + RULER + RULER.join(blocks)).strip()
            elif blocks:
                text_clean = RULER.join(blocks).strip()
            else:
                text_clean = base_body
        else:
            # none：保持你原来的行为（meta 行 + 正文）
            meta_lines = [f"{k}：{v}" for k, v in meta.items()]
            text_clean = "\n".join(meta_lines + ([""] if meta_lines and body_paras else []) + body_paras).strip()

        # item
        item = PageItem()
        item["url"] = response.url
        item["domain"] = dom
        item["fetched_at"] = time.time()
        item["status"] = response.status
        item["title"] = title
        item["pub_time"] = pub_time
        item["text"] = text_clean          # 纯文本（含换行）
        item["html_path"] = str(html_path)
        item["meta"] = meta
        item["bearers"] = bearers
        item["text_augmented"] = (self.enrich_mode in ("append", "replace"))  # NEW
        yield item

from scrapy import cmdline

cmdline.execute([
    "scrapy", "crawl", "focused",
    "-a", "start=13774",
    "-a", "end=13784",
    "-a", "enrich_mode=append",          # 保留正文并追加结构化块
    "-a", "enrich_format=readable",      # 可读文本块
    "-a", "add_json_summary=1",          # 末尾再加 JSON 摘要（可选）
])


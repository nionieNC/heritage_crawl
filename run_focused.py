from scrapy import cmdline

cmdline.execute([
    "scrapy", "crawl", "focused",
    "-a", "start=13774",
    "-a", "end=13774",
])

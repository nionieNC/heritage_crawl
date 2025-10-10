import scrapy, time, feedparser
from pathlib import Path
from ..items import PageItem

class RSSSpider(scrapy.Spider):
    name = "rss"
    def start_requests(self):
        feeds = Path("seeds/rss.txt").read_text(encoding="utf-8").splitlines()
        for f in feeds:
            f = f.strip()
            if not f:
                continue
            d = feedparser.parse(f)
            for e in d.entries:
                url = e.link
                yield scrapy.Request(url, callback=self.parse_article)

    def parse_article(self, response):
        item = PageItem()
        item["url"] = response.url
        item["fetched_at"] = time.time()
        item["status"] = response.status
        item["content_type"] = response.headers.get("Content-Type", b"").decode("utf-8", "ignore")
        item["title"] = response.css("title::text").get() or ""
        p = Path("data/raw/rss"); p.mkdir(parents=True, exist_ok=True)
        html_path = p / f"{int(time.time()*1000)}.html"
        html_path.write_bytes(response.body)
        item["html_path"] = str(html_path)
        yield item

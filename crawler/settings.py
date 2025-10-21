BOT_NAME = "crawler"
SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

ROBOTSTXT_OBEY = False

DOWNLOAD_DELAY = 0.5
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS = 8
RETRY_ENABLED = True
RETRY_TIMES = 2
#HTTPERROR_ALLOWED_CODES = [403, 404, 429, 500, 502, 503]

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://www.ihchina.cn/",
}
# 兜底 UA：你的 SimpleRandomUserAgent 失效时用这个
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
COOKIES_ENABLED = True
DOWNLOADER_MIDDLEWARES = {
    "crawler.middlewares.SimpleRandomUserAgent": 200,
}
ITEM_PIPELINES = {
    "crawler.pipelines.DedupeAndStorePipeline": 300,
}
IMAGES_STORE = "data/media"


FILES_STORE = "data/raw"


LOG_LEVEL = "INFO"
FEED_EXPORT_ENCODING = "utf-8"


AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0
AUTOTHROTTLE_MAX_DELAY = 6.0
AUTOTHROTTLE_TARGET_CONCURRENCY = 2.0


BOT_NAME = "crawler"
SPIDER_MODULES = ["crawler.spiders"]
NEWSPIDER_MODULE = "crawler.spiders"

ROBOTSTXT_OBEY = True

DOWNLOAD_DELAY = 1.0
RANDOMIZE_DOWNLOAD_DELAY = True
CONCURRENT_REQUESTS = 8
RETRY_ENABLED = True
RETRY_TIMES = 2
HTTPERROR_ALLOWED_CODES = [403, 404, 429, 500, 502, 503]

DEFAULT_REQUEST_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

DOWNLOADER_MIDDLEWARES = {
    "crawler.middlewares.SimpleRandomUserAgent": 400,
}
ITEM_PIPELINES = {
    "crawler.pipelines.DedupeAndStorePipeline": 300,
}
IMAGES_STORE = "data/media"


FILES_STORE = "data/raw"


LOG_LEVEL = "INFO"
FEED_EXPORT_ENCODING = "utf-8"

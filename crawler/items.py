# crawler/items.py
import scrapy

class PageItem(scrapy.Item):
    # 核心字段
    url = scrapy.Field()
    domain = scrapy.Field()
    fetched_at = scrapy.Field()
    status = scrapy.Field()
    title = scrapy.Field()
    pub_time = scrapy.Field()
    text = scrapy.Field()
    html_path = scrapy.Field()

    # 结构化
    meta = scrapy.Field()         # dict：项目基本信息
    bearers = scrapy.Field()      # list[dict]：代表性传承人

    # 抓取/解析附加信息（你的 pipeline/输出里已出现过）
    checksum = scrapy.Field()     # 去重/签名
    license = scrapy.Field()      # 版权（若有）
    robots = scrapy.Field()       # robots 提示（若有）
    outlinks = scrapy.Field()     # list[str]：外链（若收集）
    content_type = scrapy.Field() # Content-Type（若记录）
    lang = scrapy.Field()         # 语言（若记录）

    text_augmented = scrapy.Field()   # True/False
    augmented_at = scrapy.Field()     # ISO 时间戳
    enrich_mode = scrapy.Field()      # none/append/replace
    enrich_format = scrapy.Field()    # readable/json
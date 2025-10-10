# heritage_crawl — 本地零成本爬虫起步包

## 一步到位（Windows / Mac / Linux 通用）
1) 安装 Python 3.10+ 与 pip（你已具备）。
2) 在项目根目录创建虚拟环境：
   - Windows (PowerShell):
     python -m venv .venv
     .\.venv\Scripts\Activate.ps1
   - macOS / Linux (bash/zsh):
     python3 -m venv .venv
     source .venv/bin/activate

3) 安装依赖：
   pip install -r requirements.txt

4) 编辑白名单与种子：
   - seeds/allow_domains.txt   # 只允许这些主域名（强烈建议只放可信站点）
   - seeds/seeds.txt           # 抓取的入口页面（栏目页/首页/列表页）

5) 运行爬虫（推荐在项目根目录执行）：
   # 断点续爬，避免中途失败重来
   scrapy crawl seed -s JOBDIR=jobs/seed

   # 如果系统找不到 scrapy 命令
   python -m scrapy crawl seed -s JOBDIR=jobs/seed

6) 输出在哪里？
   - 原始HTML：data/raw/<domain>/timestamp.html
   - 清洗后正文：data/text/<domain>.jsonl  （一行一条，后续RAG最方便）
   - 去重索引：db/crawl.sqlite

## 常见问题
- 抓不到正文？
  - 站点可能强依赖JS，改用无头浏览器（scrapy-playwright）。

- 速度太快被限流？
  - 在 crawler/settings.py 下调 CONCURRENT_REQUESTS，或把 DOWNLOAD_DELAY 调到 2.0 以上。

- Windows 上激活虚拟环境失败？
  - 用 PowerShell 以管理员身份执行：
    Set-ExecutionPolicy RemoteSigned
  - 然后重新运行 Activate.ps1。

- 想抓RSS：
  - 在 seeds/rss.txt 填RSS地址，然后：
    scrapy crawl rss -s JOBDIR=jobs/rss

## 结构
heritage_crawl/
├─ seeds/                # 站点白名单/入口
├─ data/                 # 原始与清洗数据
├─ db/                   # sqlite 去重与索引
├─ crawler/              # Scrapy 项目代码
└─ requirements.txt      # 依赖

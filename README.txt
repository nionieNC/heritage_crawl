& "C:\Program Files\PostgreSQL\14\bin\psql.exe" `
  -h nioniedb.rwlb.rds.aliyuncs.com -p 5432 -U nionie1 -d postgres

#shangchuan
$env:PGHOST="nioniedb.rwlb.rds.aliyuncs.com"
$env:PGPORT="5432"
$env:PGUSER="nionie1"
$env:PGPASSWORD="Nc874904173"
$env:PGDATABASE="postgres"

& "C:\Users\Dell\Desktop\heritage_crawl\.venv\Scripts\python.exe" `
  "C:\Users\Dell\Desktop\heritage_crawl\data_process\ingest_jsonl_to_db.py" `
  --docs   "C:\Users\Dell\Desktop\heritage_crawl\data\text\documents_min.jsonl" `
  --chunks "C:\Users\Dell\Desktop\heritage_crawl\data\text\chunks.jsonl"




git status

git add .

git commit -m "fix: update scrapy pipeline and database schema"

git push origin master

> 每次改完代码 → `git add .` → `git commit -m "..."` → `git pull origin master` → `git push origin master`



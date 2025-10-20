& "C:\Program Files\PostgreSQL\14\bin\psql.exe" `
  -h nioniedb.rwlb.rds.aliyuncs.com -p 5432 -U nionie1 -d postgres

$env:PGHOST="nioniedb.rwlb.rds.aliyuncs.com"
$env:PGPORT="5432"
$env:PGUSER="nionie1"
$env:PGPASSWORD="Nc874904173"
$env:PGDATABASE="postgres"

# 把路径换成你本地的 JSONL 文件路径
python ingest_jsonl_to_polardb.py --jsonl "C:\Users\Dell\Desktop\heritage_crawl\data\text\ihchina.cn.jsonl" --chunk-size 1000 --overlap 100


& "C:\Users\Dell\Desktop\heritage_crawl\.venv\Scripts\python.exe" `
  "C:\Users\Dell\Desktop\heritage_crawl\data_process\ingest_jsonl_to_db.py" `
  --docs   "C:\Users\Dell\Desktop\heritage_crawl\data\text\documents_min.jsonl" `
  --chunks "C:\Users\Dell\Desktop\heritage_crawl\data\text\chunks.jsonl"



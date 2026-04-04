import os, logging
from datetime import date
import requests, duckdb
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("DUCKDB_PATH")
BASE_URL = os.getenv("TLC_BASE_URL")
start_year=int(os.getenv("START_YEAR"))
start_month=int(os.getenv("START_MONTH"))
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
def get_months_to_load(s_month,s_year):
    months=[]
    today = date.today()
    for year in range(s_year, today.year+1):
        for month in range(1, 13):
            if (year == s_year and month < s_month):
                continue
            if (year == today.year and month > today.month):
                break
            months.append((year, month))
    return months
def download_parquet(year,month,dest_dir):
    filename=f'yellow_tripdata_{year}-{month:02d}.parquet'
    url = f"{BASE_URL.rstrip('/')}/{filename}"
    dest = os.path.join(dest_dir, filename)
    if os.path.exists(dest):
        logging.info(f"File đã tồn tại: {filename}")
        return dest
    try:
        logging.info(f"Đang tải {filename}...")
        response = requests.get(url, stream=True)
        response.raise_for_status() 
        with open(dest, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                if chunk: 
                    f.write(chunk)      
        return dest
    except Exception as e:
        logging.error(f"Lỗi khi tải {filename}: {e}")
        return None
def load_parquet_to_duckdb(conn,parquet_path, table_name):
    try:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_parquet('{parquet_path}')")
        logging.info(f"Đã tải {parquet_path} vào DuckDB dưới tên bảng {table_name}")
    except Exception as e:
        logging.error(f"Lỗi khi tải {parquet_path} vào DuckDB: {e}") 
def get_loaded_months(conn):
    try:
        result = conn.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'""").fetchall()
        loaded_months = set()
        for row in result:
            name = row[0]
            if name.startswith("yellow_tripdata_"):
                parts = name.split("_") 
                if len(parts) == 4:
                    year, month = int(parts[2]), int(parts[3])
                    loaded_months.add((year, month))
        return loaded_months
    except Exception as e:
        logging.error(f"Lỗi khi truy vấn DuckDB: {e}")
        return set()
    
def download_zone_lookup(dest_dir):
    """Tải zone lookup CSV — chỉ chạy 1 lần."""
    url  = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    dest = os.path.join(dest_dir, "taxi_zone_lookup.csv")

    if os.path.exists(dest):
        logging.info("Zone lookup đã có, bỏ qua.")
        return dest

    try:
        logging.info("Đang tải zone lookup...")
        response = requests.get(url)
        response.raise_for_status()
        with open(dest, 'w', encoding='utf-8',newline='') as f:
            f.write(response.text)
        logging.info("Tải zone lookup xong.")
        return dest
    except Exception as e:
        logging.error(f"Lỗi khi tải zone lookup: {e}")
        return None

def load_zone_lookup_to_duckdb(conn, csv_path):
    try:
        abs_path = os.path.abspath(csv_path).replace("\\", "/") 
        conn.execute(f"""
            CREATE OR REPLACE TABLE taxi_zones
            AS SELECT * FROM read_csv_auto('{abs_path}')
        """)
        logging.info("Đã load taxi_zones vào DuckDB.")
    except Exception as e:
        logging.error(f"Lỗi khi load zone lookup: {e}")

def main():
    conn=duckdb.connect(DB_PATH)
    loaded=get_loaded_months(conn)
    zone_path = download_zone_lookup("data/zone_lookup")
    if zone_path:
        load_zone_lookup_to_duckdb(conn, zone_path)
    for year, month in get_months_to_load(start_month, start_year):
        if (year, month) in loaded:
            logging.info(f"Đã có trong DB: {year}-{month:02d}, bỏ qua")
            continue
        dest_dir = f"data/{year}"
        os.makedirs(dest_dir, exist_ok=True)
        parquet_path = download_parquet(year, month, dest_dir)
        if parquet_path:
            logging.info(f"Đã tải xong: {parquet_path}")
            load_parquet_to_duckdb(conn, parquet_path, f"yellow_tripdata_{year}_{month:02d}")
    conn.close()
if __name__ == "__main__":
    main()
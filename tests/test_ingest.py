import duckdb
conn = duckdb.connect("./data/nyc_taxi.duckdb")
print(conn.execute("select distinct borough from taxi_zones ").df())

import duckdb

# show tables in the database
con = duckdb.connect("bio_data.duck.db")
res = con.execute("SHOW TABLES").fetchdf()
print(res)


con.close()

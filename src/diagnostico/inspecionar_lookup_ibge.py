import pyarrow.parquet as pq
import duckdb

LOOKUP_PATH = "lookup_tables/ibge_municipios_espacial.parquet"

tbl = pq.read_table(LOOKUP_PATH)
print(f"Linhas no lookup: {tbl.num_rows}")

duckdb.sql(f"""
SELECT 
    uf_sigla,
    COUNT(*) AS n
FROM '{LOOKUP_PATH}'
GROUP BY uf_sigla
ORDER BY uf_sigla
""").show()

duckdb.sql(f"""
SELECT *
FROM '{LOOKUP_PATH}'
ORDER BY municipio_codigo_7
LIMIT 20
""").show()

import duckdb

SINAN_PATH = "datalake/sinan/meningite_br.parquet"
CNES_LOOKUP_PATH = "lookup_tables/cnes_unidades_minimo.parquet"

con = duckdb.connect()

print("Validação do lookup CNES mínimo")
print(f"SINAN:  {SINAN_PATH}")
print(f"CNES:   {CNES_LOOKUP_PATH}")

# 1) Qualidade interna do lookup CNES
print("\n" + "=" * 70)
print("Cobertura espacial dentro do lookup CNES")
print("=" * 70)

res = con.execute(f"""
SELECT
    COUNT(*) AS total_unidades,
    SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) AS com_coordenada,
    SUM(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 ELSE 0 END) AS sem_coordenada,
    ROUND(
        100.0 * SUM(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS pct_com_coordenada
FROM '{CNES_LOOKUP_PATH}'
""").fetchone()

print(f"Total de unidades: {res[0]}")
print(f"Unidades com coordenada: {res[1]}")
print(f"Unidades sem coordenada: {res[2]}")
print(f"% com coordenada: {res[3]}")

# 2) Match com o SINAN
print("\n" + "=" * 70)
print("Cobertura do CNES sobre ID_UNIDADE no SINAN")
print("=" * 70)

res2 = con.execute(f"""
WITH sinan_codes AS (
    SELECT TRIM(CAST(ID_UNIDADE AS VARCHAR)) AS cnes_codigo
    FROM '{SINAN_PATH}'
    WHERE ID_UNIDADE IS NOT NULL
      AND TRIM(CAST(ID_UNIDADE AS VARCHAR)) <> ''
),
distinct_codes AS (
    SELECT DISTINCT cnes_codigo
    FROM sinan_codes
),
matched AS (
    SELECT d.cnes_codigo
    FROM distinct_codes d
    INNER JOIN '{CNES_LOOKUP_PATH}' c
        ON d.cnes_codigo = c.cnes_codigo
),
matched_with_coord AS (
    SELECT d.cnes_codigo
    FROM distinct_codes d
    INNER JOIN '{CNES_LOOKUP_PATH}' c
        ON d.cnes_codigo = c.cnes_codigo
    WHERE c.latitude IS NOT NULL AND c.longitude IS NOT NULL
),
unmatched AS (
    SELECT d.cnes_codigo
    FROM distinct_codes d
    LEFT JOIN '{CNES_LOOKUP_PATH}' c
        ON d.cnes_codigo = c.cnes_codigo
    WHERE c.cnes_codigo IS NULL
)
SELECT
    (SELECT COUNT(*) FROM sinan_codes) AS total_registros_sinan_com_id_unidade,
    (SELECT COUNT(*) FROM distinct_codes) AS cnes_distintos_sinan,
    (SELECT COUNT(*) FROM matched) AS cnes_distintos_com_match,
    (SELECT COUNT(*) FROM matched_with_coord) AS cnes_distintos_com_match_e_coord,
    (SELECT COUNT(*) FROM unmatched) AS cnes_distintos_sem_match
""").fetchone()

print(f"Registros SINAN com ID_UNIDADE não nulo: {res2[0]}")
print(f"CNES distintos no SINAN: {res2[1]}")
print(f"CNES distintos com match: {res2[2]}")
print(f"CNES distintos com match e coordenada: {res2[3]}")
print(f"CNES distintos sem match: {res2[4]}")

if res2[4] > 0:
    print("\nPrimeiros CNES sem match:")
    rows = con.execute(f"""
        WITH distinct_codes AS (
            SELECT DISTINCT TRIM(CAST(ID_UNIDADE AS VARCHAR)) AS cnes_codigo
            FROM '{SINAN_PATH}'
            WHERE ID_UNIDADE IS NOT NULL
              AND TRIM(CAST(ID_UNIDADE AS VARCHAR)) <> ''
        )
        SELECT d.cnes_codigo
        FROM distinct_codes d
        LEFT JOIN '{CNES_LOOKUP_PATH}' c
            ON d.cnes_codigo = c.cnes_codigo
        WHERE c.cnes_codigo IS NULL
        ORDER BY d.cnes_codigo
        LIMIT 50
    """).fetchall()

    for r in rows:
        print(f" - {r[0]}")

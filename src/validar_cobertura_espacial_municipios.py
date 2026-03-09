import duckdb

SINAN_PATH = "datalake/sinan/meningite_br.parquet"
LOOKUP_PATH = "lookup_tables/ibge_municipios_espacial.parquet"

MUNICIPAL_FIELDS = [
    ("ID_MUNICIP", "municipio_notificacao"),
    ("ID_MN_RESI", "municipio_residencia"),
    ("ATE_MUNICI", "municipio_hospital"),
]

con = duckdb.connect()

def validar_campo(campo: str, descricao: str) -> None:
    print("\n" + "=" * 70)
    print(f"Campo: {campo} ({descricao})")
    print("=" * 70)

    resumo = con.execute(f"""
        WITH sinan_codes AS (
            SELECT
                TRIM(CAST({campo} AS VARCHAR)) AS codigo
            FROM '{SINAN_PATH}'
            WHERE {campo} IS NOT NULL
              AND TRIM(CAST({campo} AS VARCHAR)) <> ''
        ),
        distinct_codes AS (
            SELECT DISTINCT codigo
            FROM sinan_codes
        ),
        matched AS (
            SELECT d.codigo
            FROM distinct_codes d
            INNER JOIN '{LOOKUP_PATH}' l
                ON d.codigo = l.municipio_codigo_6
        ),
        unmatched AS (
            SELECT d.codigo
            FROM distinct_codes d
            LEFT JOIN '{LOOKUP_PATH}' l
                ON d.codigo = l.municipio_codigo_6
            WHERE l.municipio_codigo_6 IS NULL
        )
        SELECT
            (SELECT COUNT(*) FROM sinan_codes) AS total_registros_nao_nulos,
            (SELECT COUNT(*) FROM distinct_codes) AS codigos_distintos_sinan,
            (SELECT COUNT(*) FROM matched) AS codigos_distintos_com_match,
            (SELECT COUNT(*) FROM unmatched) AS codigos_distintos_sem_match
    """).fetchone()

    print(f"Total de registros não nulos: {resumo[0]}")
    print(f"Códigos distintos no SINAN: {resumo[1]}")
    print(f"Códigos distintos com match: {resumo[2]}")
    print(f"Códigos distintos sem match: {resumo[3]}")

    if resumo[3] > 0:
        print("\nCódigos sem correspondência no lookup:")
        problemas = con.execute(f"""
            WITH distinct_codes AS (
                SELECT DISTINCT TRIM(CAST({campo} AS VARCHAR)) AS codigo
                FROM '{SINAN_PATH}'
                WHERE {campo} IS NOT NULL
                  AND TRIM(CAST({campo} AS VARCHAR)) <> ''
            )
            SELECT d.codigo
            FROM distinct_codes d
            LEFT JOIN '{LOOKUP_PATH}' l
                ON d.codigo = l.municipio_codigo_6
            WHERE l.municipio_codigo_6 IS NULL
            ORDER BY d.codigo
            LIMIT 50
        """).fetchall()

        for row in problemas:
            print(f" - {row[0]}")
    else:
        print("Cobertura completa: todos os códigos distintos encontraram correspondência.")


def validar_ufs_derivadas() -> None:
    print("\n" + "=" * 70)
    print("Checagem opcional: coerência UF derivada do município")
    print("=" * 70)

    for campo, descricao in [
        ("ID_MUNICIP", "SG_UF_NOT"),
        ("ID_MN_RESI", "SG_UF"),
        ("ATE_MUNICI", "ATE_UF_HOS"),
    ]:
        total = con.execute(f"""
            SELECT COUNT(*)
            FROM '{SINAN_PATH}'
            WHERE {campo} IS NOT NULL
              AND TRIM(CAST({campo} AS VARCHAR)) <> ''
              AND {descricao} IS NOT NULL
              AND TRIM(CAST({descricao} AS VARCHAR)) <> ''
        """).fetchone()[0]

        inconsistentes = con.execute(f"""
            SELECT COUNT(*)
            FROM '{SINAN_PATH}'
            WHERE {campo} IS NOT NULL
              AND TRIM(CAST({campo} AS VARCHAR)) <> ''
              AND {descricao} IS NOT NULL
              AND TRIM(CAST({descricao} AS VARCHAR)) <> ''
              AND SUBSTR(TRIM(CAST({campo} AS VARCHAR)), 1, 2) <> LPAD(TRIM(CAST({descricao} AS VARCHAR)), 2, '0')
        """).fetchone()[0]

        print(f"{campo} vs {descricao}: {inconsistentes} inconsistências em {total} registros comparáveis")


def main():
    print("Validação de cobertura espacial municipal")
    print(f"SINAN:  {SINAN_PATH}")
    print(f"Lookup: {LOOKUP_PATH}")

    for campo, descricao in MUNICIPAL_FIELDS:
        validar_campo(campo, descricao)

    validar_ufs_derivadas()


if __name__ == "__main__":
    main()

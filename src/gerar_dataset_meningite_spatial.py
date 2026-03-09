# src/gerar_dataset_meningite_spatial.py
#
# Gera o dataset analítico espacial final do projeto Men_Ob.
#
# Integra:
#   1) base epidemiológica consolidada do SINAN
#   2) lookup territorial municipal do IBGE
#   3) lookup espacial das unidades CNES
#   4) camada semântica mínima baseada no YAML de metadados
#
# Saída:
#   datalake/sinan/meningite_spatial.parquet
#
# Uso:
#   uv run src/gerar_dataset_meningite_spatial.py

from __future__ import annotations

from pathlib import Path
import duckdb
import yaml

# =========================
# Caminhos
# =========================

SINAN_PATH = Path("datalake/sinan/meningite_br.parquet")
IBGE_PATH = Path("lookup_tables/ibge_municipios_espacial.parquet")
CNES_PATH = Path("lookup_tables/cnes_meningite_spatial.parquet")
METADATA_PATH = Path("metadados/sinan_meningite_metadata.yaml")

OUTPUT_PATH = Path("datalake/sinan/meningite_spatial.parquet")


# =========================
# Utilidades
# =========================

def load_metadata_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def ensure_inputs() -> None:
    for p in [SINAN_PATH, IBGE_PATH, CNES_PATH, METADATA_PATH]:
        if not p.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {p}")


def count_rows(path: Path) -> int:
    con = duckdb.connect()
    return con.execute(f"SELECT COUNT(*) FROM '{path.as_posix()}'").fetchone()[0]


# =========================
# Main
# =========================

def main() -> None:
    ensure_inputs()

    print("1/6 - Carregando metadados YAML...")
    meta = load_metadata_yaml(METADATA_PATH)
    vars_total_yaml = len(meta.get("variables", {}))
    ext_vars = meta.get("external_lookup_variables", [])

    print(f"Variáveis descritas no YAML: {vars_total_yaml}")
    print(f"Variáveis com lookup externo declaradas: {len(ext_vars)}")

    print("2/6 - Verificando inputs...")
    n_sinan = count_rows(SINAN_PATH)
    n_ibge = count_rows(IBGE_PATH)
    n_cnes = count_rows(CNES_PATH)

    print(f"SINAN: {n_sinan:,} linhas")
    print(f"IBGE municipal: {n_ibge:,} linhas")
    print(f"CNES spatial: {n_cnes:,} linhas")

    print("3/6 - Montando joins espaciais...")
    con = duckdb.connect()

    sql = f"""
    CREATE OR REPLACE TABLE meningite_spatial AS
    WITH sinan AS (
        SELECT
            *
        FROM '{SINAN_PATH.as_posix()}'
    ),

    ibge_resi AS (
        SELECT
            municipio_codigo_6 AS id_mn_resi_join,
            municipio_nome      AS municipio_residencia_nome,
            uf_codigo           AS municipio_residencia_uf_codigo,
            uf_sigla            AS municipio_residencia_uf_sigla,
            uf_nome             AS municipio_residencia_uf_nome,
            centroide_lon       AS municipio_residencia_lon,
            centroide_lat       AS municipio_residencia_lat,
            area_km2            AS municipio_residencia_area_km2
        FROM '{IBGE_PATH.as_posix()}'
    ),

    ibge_not AS (
        SELECT
            municipio_codigo_6 AS id_municip_join,
            municipio_nome      AS municipio_notificacao_nome,
            uf_codigo           AS municipio_notificacao_uf_codigo,
            uf_sigla            AS municipio_notificacao_uf_sigla,
            uf_nome             AS municipio_notificacao_uf_nome,
            centroide_lon       AS municipio_notificacao_lon,
            centroide_lat       AS municipio_notificacao_lat,
            area_km2            AS municipio_notificacao_area_km2
        FROM '{IBGE_PATH.as_posix()}'
    ),

    ibge_hosp AS (
        SELECT
            municipio_codigo_6 AS ate_munici_join,
            municipio_nome      AS municipio_hospital_nome,
            uf_codigo           AS municipio_hospital_uf_codigo,
            uf_sigla            AS municipio_hospital_uf_sigla,
            uf_nome             AS municipio_hospital_uf_nome,
            centroide_lon       AS municipio_hospital_lon,
            centroide_lat       AS municipio_hospital_lat,
            area_km2            AS municipio_hospital_area_km2
        FROM '{IBGE_PATH.as_posix()}'
    ),

    cnes AS (
        SELECT
            cnes_codigo,
            cod_cep,
            cep_limpo,
            codufmun,
            municipio_codigo_6      AS cnes_municipio_codigo_6,
            uf_codigo               AS cnes_uf_codigo,
            tp_unid,
            tpgestao,
            esfera_a,
            nat_jur,
            competen                AS cnes_competen,
            spatial_resolution_available,
            spatial_resolution_target,
            spatial_resolution_final,
            spatial_source,
            geocode_quality,
            latitude                AS cnes_latitude,
            longitude               AS cnes_longitude,
            municipio_nome          AS cnes_municipio_nome,
            uf_sigla                AS cnes_uf_sigla,
            uf_nome                 AS cnes_uf_nome
        FROM '{CNES_PATH.as_posix()}'
    )

    SELECT
        s.*,

        -- Enriquecimento territorial de residência
        r.municipio_residencia_nome,
        r.municipio_residencia_uf_codigo,
        r.municipio_residencia_uf_sigla,
        r.municipio_residencia_uf_nome,
        r.municipio_residencia_lon,
        r.municipio_residencia_lat,
        r.municipio_residencia_area_km2,

        -- Enriquecimento territorial de notificação
        n.municipio_notificacao_nome,
        n.municipio_notificacao_uf_codigo,
        n.municipio_notificacao_uf_sigla,
        n.municipio_notificacao_uf_nome,
        n.municipio_notificacao_lon,
        n.municipio_notificacao_lat,
        n.municipio_notificacao_area_km2,

        -- Enriquecimento territorial do município do hospital (quando existir)
        h.municipio_hospital_nome,
        h.municipio_hospital_uf_codigo,
        h.municipio_hospital_uf_sigla,
        h.municipio_hospital_uf_nome,
        h.municipio_hospital_lon,
        h.municipio_hospital_lat,
        h.municipio_hospital_area_km2,

        -- Enriquecimento CNES da unidade
        c.cod_cep                AS unidade_cod_cep,
        c.cep_limpo              AS unidade_cep_limpo,
        c.codufmun               AS unidade_codufmun,
        c.cnes_municipio_codigo_6,
        c.cnes_uf_codigo,
        c.tp_unid                AS unidade_tp_unid,
        c.tpgestao               AS unidade_tpgestao,
        c.esfera_a               AS unidade_esfera_a,
        c.nat_jur                AS unidade_nat_jur,
        c.cnes_competen,
        c.spatial_resolution_available AS unidade_spatial_resolution_available,
        c.spatial_resolution_target    AS unidade_spatial_resolution_target,
        c.spatial_resolution_final     AS unidade_spatial_resolution_final,
        c.spatial_source               AS unidade_spatial_source,
        c.geocode_quality              AS unidade_geocode_quality,
        c.cnes_latitude                AS unidade_latitude,
        c.cnes_longitude               AS unidade_longitude,
        c.cnes_municipio_nome          AS unidade_municipio_nome,
        c.cnes_uf_sigla                AS unidade_uf_sigla,
        c.cnes_uf_nome                 AS unidade_uf_nome,

        -- Proveniência
        'SINAN'                        AS source_dataset,
        'IBGE'                         AS territorial_lookup_source,
        'CNES'                         AS unidade_lookup_source,
        'SINAN+IBGE+CNES'              AS integration_layer,
        'v1'                           AS spatial_dataset_version

    FROM sinan s
    LEFT JOIN ibge_resi r
        ON CAST(s.ID_MN_RESI AS VARCHAR) = r.id_mn_resi_join
    LEFT JOIN ibge_not n
        ON CAST(s.ID_MUNICIP AS VARCHAR) = n.id_municip_join
    LEFT JOIN ibge_hosp h
        ON CAST(s.ATE_MUNICI AS VARCHAR) = h.ate_munici_join
    LEFT JOIN cnes c
        ON TRIM(CAST(s.ID_UNIDADE AS VARCHAR)) = c.cnes_codigo
    ;
    """

    con.execute(sql)

    print("4/6 - Validando dataset final...")
    n_out = con.execute("SELECT COUNT(*) FROM meningite_spatial").fetchone()[0]
    n_cols = len(con.execute("SELECT * FROM meningite_spatial LIMIT 0").df().columns)

    n_resi = con.execute("""
        SELECT COUNT(*) FROM meningite_spatial
        WHERE municipio_residencia_lat IS NOT NULL
          AND municipio_residencia_lon IS NOT NULL
    """).fetchone()[0]

    n_not = con.execute("""
        SELECT COUNT(*) FROM meningite_spatial
        WHERE municipio_notificacao_lat IS NOT NULL
          AND municipio_notificacao_lon IS NOT NULL
    """).fetchone()[0]

    n_cnes = con.execute("""
        SELECT COUNT(*) FROM meningite_spatial
        WHERE unidade_latitude IS NOT NULL
          AND unidade_longitude IS NOT NULL
    """).fetchone()[0]

    print(f"Linhas no dataset final: {n_out:,}")
    print(f"Colunas no dataset final: {n_cols}")
    print(f"Casos com geografia de residência: {n_resi:,}")
    print(f"Casos com geografia de notificação: {n_not:,}")
    print(f"Casos com geografia da unidade CNES: {n_cnes:,}")

    print("5/6 - Salvando parquet final...")
    con.execute(f"""
        COPY meningite_spatial
        TO '{OUTPUT_PATH.as_posix()}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """)

    print(f"Arquivo salvo em: {OUTPUT_PATH}")

    print("6/6 - Amostra final:")
    amostra = con.execute("""
        SELECT
            ID_MN_RESI,
            municipio_residencia_nome,
            municipio_residencia_uf_sigla,
            municipio_residencia_lat,
            municipio_residencia_lon,
            ID_MUNICIP,
            municipio_notificacao_nome,
            municipio_notificacao_uf_sigla,
            ID_UNIDADE,
            unidade_cod_cep,
            unidade_spatial_resolution_final,
            unidade_latitude,
            unidade_longitude
        FROM meningite_spatial
        LIMIT 10
    """).df()

    print(amostra)


if __name__ == "__main__":
    main()

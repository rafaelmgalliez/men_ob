# src/reconstruir_cnes_meningite_spatial_conservador.py
#
# Reconstrói o lookup espacial do CNES da meningite com uma estratégia conservadora.
#
# Problema:
# A geocodificação por CEP via Nominatim gerou muitos pontos implausíveis,
# com grande distância em relação ao município esperado.
#
# Estratégia conservadora:
# 1) manter coordenada por CEP apenas quando a distância ao centroide do município
#    esperado for <= LIMIAR_KM_ACEITAVEL
# 2) caso contrário, rebaixar para o centroide do município
# 3) se município faltar, usar centroide da UF
#
# Entradas:
#   lookup_tables/cnes_meningite_spatial.parquet
#   lookup_tables/ibge_municipios_espacial.parquet
#
# Saídas:
#   lookup_tables/cnes_meningite_spatial_conservador.parquet
#   diagnosticos/cnes_meningite_spatial_conservador_resumo.txt
#   diagnosticos/cnes_meningite_spatial_conservador_amostra.csv
#
# Uso:
#   uv run src/reconstruir_cnes_meningite_spatial_conservador.py

from __future__ import annotations

from math import radians, sin, cos, sqrt, asin
from pathlib import Path

import pandas as pd

CNES_SPATIAL_IN = Path("lookup_tables/cnes_meningite_spatial.parquet")
IBGE_LOOKUP = Path("lookup_tables/ibge_municipios_espacial.parquet")

CNES_SPATIAL_OUT = Path("lookup_tables/cnes_meningite_spatial_conservador.parquet")

DIAG_DIR = Path("diagnosticos")
DIAG_DIR.mkdir(parents=True, exist_ok=True)

OUT_TXT = DIAG_DIR / "cnes_meningite_spatial_conservador_resumo.txt"
OUT_CSV = DIAG_DIR / "cnes_meningite_spatial_conservador_amostra.csv"

# limiar conservador
LIMIAR_KM_ACEITAVEL = 25.0


def pct(n: int, d: int) -> float:
    return round(100.0 * n / d, 2) if d else 0.0


def haversine_km(lat1, lon1, lat2, lon2):
    if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
        return None

    r = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = (
        sin(dlat / 2) ** 2
        + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    )
    c = 2 * asin(sqrt(a))
    return r * c


def build_uf_centroids(ibge: pd.DataFrame) -> pd.DataFrame:
    return (
        ibge.groupby(["uf_codigo", "uf_sigla", "uf_nome"], as_index=False)
        .agg(
            uf_lon=("centroide_lon", "mean"),
            uf_lat=("centroide_lat", "mean"),
        )
    )


def main():
    if not CNES_SPATIAL_IN.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {CNES_SPATIAL_IN}")

    if not IBGE_LOOKUP.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {IBGE_LOOKUP}")

    print("1/6 - Carregando bases...")
    cnes = pd.read_parquet(CNES_SPATIAL_IN)
    ibge = pd.read_parquet(IBGE_LOOKUP)

    cnes["municipio_codigo_6"] = cnes["municipio_codigo_6"].fillna("").astype(str)
    cnes["uf_codigo"] = cnes["uf_codigo"].fillna("").astype(str)

    cnes["latitude"] = pd.to_numeric(cnes["latitude"], errors="coerce")
    cnes["longitude"] = pd.to_numeric(cnes["longitude"], errors="coerce")

    ibge["municipio_codigo_6"] = ibge["municipio_codigo_6"].fillna("").astype(str)
    ibge["uf_codigo"] = ibge["uf_codigo"].fillna("").astype(str)
    ibge["centroide_lat"] = pd.to_numeric(ibge["centroide_lat"], errors="coerce")
    ibge["centroide_lon"] = pd.to_numeric(ibge["centroide_lon"], errors="coerce")

    print(f"CNES spatial original: {len(cnes):,} linhas")
    print(f"IBGE lookup: {len(ibge):,} linhas")

    print("2/6 - Enriquecendo com centroides municipais...")
    ibge_mun = ibge[
        [
            "municipio_codigo_6",
            "municipio_nome",
            "uf_codigo",
            "uf_sigla",
            "uf_nome",
            "centroide_lat",
            "centroide_lon",
        ]
    ].drop_duplicates(subset=["municipio_codigo_6"])

    df = cnes.merge(
        ibge_mun,
        on=["municipio_codigo_6", "uf_codigo"],
        how="left",
        suffixes=("", "_ibge"),
    )

    print("3/6 - Calculando distância do CEP ao município esperado...")
    df["dist_km_to_expected_municipio"] = df.apply(
        lambda row: haversine_km(
            row["latitude"],
            row["longitude"],
            row["centroide_lat"],
            row["centroide_lon"],
        ),
        axis=1,
    )

    # guardar coordenadas originais do CEP / spatial inicial
    df["latitude_cep_original"] = df["latitude"]
    df["longitude_cep_original"] = df["longitude"]
    if "spatial_resolution_final" not in df.columns:
        df["spatial_resolution_final"] = pd.NA
    if "spatial_source" not in df.columns:
        df["spatial_source"] = pd.NA
    if "geocode_quality" not in df.columns:
        df["geocode_quality"] = pd.NA

    df["spatial_resolution_original"] = df["spatial_resolution_final"]
    df["spatial_source_original"] = df["spatial_source"]
    df["geocode_quality_original"] = df["geocode_quality"]

    print("4/6 - Aplicando regra conservadora...")
    # colunas finais conservadoras
    df["latitude_conservador"] = pd.NA
    df["longitude_conservador"] = pd.NA
    df["spatial_resolution_conservador"] = "sem_localizacao"
    df["spatial_source_conservador"] = pd.NA
    df["geocode_quality_conservador"] = pd.NA
    df["regra_aplicada"] = pd.NA

    # 4.1 manter CEP se plausível
    mask_cep_plausivel = (
        df["latitude"].notna()
        & df["longitude"].notna()
        & df["dist_km_to_expected_municipio"].notna()
        & (df["dist_km_to_expected_municipio"] <= LIMIAR_KM_ACEITAVEL)
    )

    df.loc[mask_cep_plausivel, "latitude_conservador"] = df.loc[mask_cep_plausivel, "latitude"]
    df.loc[mask_cep_plausivel, "longitude_conservador"] = df.loc[mask_cep_plausivel, "longitude"]
    df.loc[mask_cep_plausivel, "spatial_resolution_conservador"] = "cep"
    df.loc[mask_cep_plausivel, "spatial_source_conservador"] = df.loc[mask_cep_plausivel, "spatial_source_original"].fillna("nominatim_cep")
    df.loc[mask_cep_plausivel, "geocode_quality_conservador"] = "aproximado_por_cep_validado"
    df.loc[mask_cep_plausivel, "regra_aplicada"] = "manter_cep"

    # 4.2 fallback para município
    mask_sem_coord = df["latitude_conservador"].isna() | df["longitude_conservador"].isna()
    mask_mun = mask_sem_coord & df["centroide_lat"].notna() & df["centroide_lon"].notna()

    df.loc[mask_mun, "latitude_conservador"] = df.loc[mask_mun, "centroide_lat"]
    df.loc[mask_mun, "longitude_conservador"] = df.loc[mask_mun, "centroide_lon"]
    df.loc[mask_mun, "spatial_resolution_conservador"] = "municipio"
    df.loc[mask_mun, "spatial_source_conservador"] = "ibge_municipio_centroid"
    df.loc[mask_mun, "geocode_quality_conservador"] = "centroide_municipal"
    df.loc[mask_mun, "regra_aplicada"] = "fallback_municipio"

    # 4.3 fallback para UF
    uf_centroids = build_uf_centroids(ibge)
    df = df.merge(
        uf_centroids,
        on=["uf_codigo", "uf_sigla", "uf_nome"],
        how="left",
    )

    mask_sem_coord = df["latitude_conservador"].isna() | df["longitude_conservador"].isna()
    mask_uf = mask_sem_coord & df["uf_lat"].notna() & df["uf_lon"].notna()

    df.loc[mask_uf, "latitude_conservador"] = df.loc[mask_uf, "uf_lat"]
    df.loc[mask_uf, "longitude_conservador"] = df.loc[mask_uf, "uf_lon"]
    df.loc[mask_uf, "spatial_resolution_conservador"] = "uf"
    df.loc[mask_uf, "spatial_source_conservador"] = "ibge_uf_centroid"
    df.loc[mask_uf, "geocode_quality_conservador"] = "centroide_uf"
    df.loc[mask_uf, "regra_aplicada"] = "fallback_uf"

    print("5/6 - Preparando artefato final...")
    # substituir as colunas finais do arquivo pela versão conservadora
    out = df.copy()
    out["latitude"] = pd.to_numeric(out["latitude_conservador"], errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude_conservador"], errors="coerce")
    out["spatial_resolution_final"] = out["spatial_resolution_conservador"]
    out["spatial_source"] = out["spatial_source_conservador"]
    out["geocode_quality"] = out["geocode_quality_conservador"]

    # manter trilha de auditoria
    keep_extra = [
        "latitude_cep_original",
        "longitude_cep_original",
        "spatial_resolution_original",
        "spatial_source_original",
        "geocode_quality_original",
        "dist_km_to_expected_municipio",
        "regra_aplicada",
    ]
    # essas colunas já estarão no dataframe

    # remover colunas temporárias de join UF
    drop_cols = [
        "uf_lat",
        "uf_lon",
        "latitude_conservador",
        "longitude_conservador",
        "spatial_resolution_conservador",
        "spatial_source_conservador",
        "geocode_quality_conservador",
    ]
    drop_cols = [c for c in drop_cols if c in out.columns]
    out = out.drop(columns=drop_cols)

    out = out.sort_values("cnes_codigo").reset_index(drop=True)
    out.to_parquet(CNES_SPATIAL_OUT, index=False)

    print("6/6 - Gerando resumo...")
    n_total = len(out)

    n_original_cep = int((out["spatial_resolution_original"] == "cep").sum())
    n_final_cep = int((out["spatial_resolution_final"] == "cep").sum())
    n_final_mun = int((out["spatial_resolution_final"] == "municipio").sum())
    n_final_uf = int((out["spatial_resolution_final"] == "uf").sum())
    n_sem = int(out["spatial_resolution_final"].isna().sum())

    n_manter_cep = int((out["regra_aplicada"] == "manter_cep").sum())
    n_fb_mun = int((out["regra_aplicada"] == "fallback_municipio").sum())
    n_fb_uf = int((out["regra_aplicada"] == "fallback_uf").sum())

    resumo = []
    resumo.append("RECONSTRUÇÃO CONSERVADORA DO LOOKUP ESPACIAL CNES")
    resumo.append("=" * 80)
    resumo.append(f"Arquivo de entrada: {CNES_SPATIAL_IN}")
    resumo.append(f"Arquivo de saída: {CNES_SPATIAL_OUT}")
    resumo.append(f"Limiar de plausibilidade CEP: {LIMIAR_KM_ACEITAVEL} km")
    resumo.append("")
    resumo.append("Cobertura")
    resumo.append("-" * 80)
    resumo.append(f"Total de CNES: {n_total}")
    resumo.append(f"Resolução original = cep: {n_original_cep} ({pct(n_original_cep, n_total)}%)")
    resumo.append("")
    resumo.append("Resultado conservador")
    resumo.append("-" * 80)
    resumo.append(f"Resolução final = cep: {n_final_cep} ({pct(n_final_cep, n_total)}%)")
    resumo.append(f"Resolução final = municipio: {n_final_mun} ({pct(n_final_mun, n_total)}%)")
    resumo.append(f"Resolução final = uf: {n_final_uf} ({pct(n_final_uf, n_total)}%)")
    resumo.append(f"Sem localização final: {n_sem} ({pct(n_sem, n_total)}%)")
    resumo.append("")
    resumo.append("Regras aplicadas")
    resumo.append("-" * 80)
    resumo.append(f"CEP mantido: {n_manter_cep} ({pct(n_manter_cep, n_total)}%)")
    resumo.append(f"Fallback para município: {n_fb_mun} ({pct(n_fb_mun, n_total)}%)")
    resumo.append(f"Fallback para UF: {n_fb_uf} ({pct(n_fb_uf, n_total)}%)")

    resumo_texto = "\n".join(resumo)
    print()
    print(resumo_texto)

    with open(OUT_TXT, "w", encoding="utf-8") as f:
        f.write(resumo_texto)

    amostra_cols = [
        "cnes_codigo",
        "cod_cep",
        "cep_limpo",
        "municipio_codigo_6",
        "municipio_nome",
        "uf_codigo",
        "uf_sigla",
        "latitude_cep_original",
        "longitude_cep_original",
        "dist_km_to_expected_municipio",
        "regra_aplicada",
        "spatial_resolution_original",
        "spatial_resolution_final",
        "latitude",
        "longitude",
        "spatial_source",
        "geocode_quality",
    ]
    amostra_cols = [c for c in amostra_cols if c in out.columns]

    out[amostra_cols].sort_values(
        ["regra_aplicada", "dist_km_to_expected_municipio"],
        ascending=[True, False],
    ).to_csv(OUT_CSV, index=False)

    print()
    print(f"Parquet salvo em: {CNES_SPATIAL_OUT}")
    print(f"Resumo salvo em: {OUT_TXT}")
    print(f"Amostra salva em: {OUT_CSV}")


if __name__ == "__main__":
    main()

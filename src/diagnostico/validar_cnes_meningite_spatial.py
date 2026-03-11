# src/diagnostico/validar_cnes_meningite_spatial.py
#
# Validação de plausibilidade espacial do lookup CNES da meningite.
#
# Objetivos:
# 1) verificar cobertura espacial final do lookup CNES
# 2) detectar coordenadas repetidas suspeitas
# 3) medir consistência entre UF/CNES e localização geográfica obtida
# 4) medir distância entre coordenada da unidade e centroide do município esperado
# 5) sinalizar unidades candidatas a rebaixamento de "cep" para "municipio"
#
# Entradas:
#   lookup_tables/cnes_meningite_spatial.parquet
#   lookup_tables/ibge_municipios_espacial.parquet
#
# Saídas:
#   diagnosticos/cnes_meningite_spatial_validacao_resumo.txt
#   diagnosticos/cnes_meningite_spatial_validacao_amostra.csv
#   diagnosticos/cnes_meningite_spatial_validacao_top_coordenadas.csv
#   diagnosticos/cnes_meningite_spatial_validacao_top_distancias.csv
#
# Uso:
#   uv run src/diagnostico/validar_cnes_meningite_spatial.py

from __future__ import annotations

from math import radians, sin, cos, sqrt, asin
from pathlib import Path

import pandas as pd

CNES_SPATIAL_PATH = Path("lookup_tables/cnes_meningite_spatial.parquet")
IBGE_LOOKUP_PATH = Path("lookup_tables/ibge_municipios_espacial.parquet")

DIAG_DIR = Path("diagnosticos")
DIAG_DIR.mkdir(parents=True, exist_ok=True)

OUT_TXT = DIAG_DIR / "cnes_meningite_spatial_validacao_resumo.txt"
OUT_AMOSTRA = DIAG_DIR / "cnes_meningite_spatial_validacao_amostra.csv"
OUT_TOP_COORD = DIAG_DIR / "cnes_meningite_spatial_validacao_top_coordenadas.csv"
OUT_TOP_DIST = DIAG_DIR / "cnes_meningite_spatial_validacao_top_distancias.csv"


def pct(n: int, d: int) -> float:
    return round(100.0 * n / d, 2) if d else 0.0


def only_digits(x: object) -> str:
    if x is None:
        return ""
    return "".join(ch for ch in str(x).strip() if ch.isdigit())


def haversine_km(lat1, lon1, lat2, lon2) -> float | None:
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


def main() -> None:
    if not CNES_SPATIAL_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {CNES_SPATIAL_PATH}")

    if not IBGE_LOOKUP_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {IBGE_LOOKUP_PATH}")

    print("1/5 - Carregando bases...")
    cnes = pd.read_parquet(CNES_SPATIAL_PATH)
    ibge = pd.read_parquet(IBGE_LOOKUP_PATH)

    cnes["cnes_codigo"] = cnes["cnes_codigo"].astype(str).str.strip()
    cnes["municipio_codigo_6"] = cnes["municipio_codigo_6"].fillna("").astype(str)
    cnes["uf_codigo"] = cnes["uf_codigo"].fillna("").astype(str)

    if "latitude" in cnes.columns:
        cnes["latitude"] = pd.to_numeric(cnes["latitude"], errors="coerce")
    else:
        cnes["latitude"] = pd.NA

    if "longitude" in cnes.columns:
        cnes["longitude"] = pd.to_numeric(cnes["longitude"], errors="coerce")
    else:
        cnes["longitude"] = pd.NA

    ibge["municipio_codigo_6"] = ibge["municipio_codigo_6"].fillna("").astype(str)
    ibge["uf_codigo"] = ibge["uf_codigo"].fillna("").astype(str)
    ibge["centroide_lat"] = pd.to_numeric(ibge["centroide_lat"], errors="coerce")
    ibge["centroide_lon"] = pd.to_numeric(ibge["centroide_lon"], errors="coerce")

    print(f"CNES spatial: {len(cnes):,} linhas")
    print(f"IBGE lookup: {len(ibge):,} linhas")

    print("2/5 - Enriquecendo com lookup municipal...")
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

    print("3/5 - Calculando métricas de plausibilidade...")
    df["has_coord"] = df["latitude"].notna() & df["longitude"].notna()

    # coordenadas arredondadas para identificar colapsos
    df["lat_round6"] = df["latitude"].round(6)
    df["lon_round6"] = df["longitude"].round(6)

    # distância ao centroide do município esperado
    df["dist_km_to_expected_municipio"] = df.apply(
        lambda row: haversine_km(
            row["latitude"],
            row["longitude"],
            row["centroide_lat"],
            row["centroide_lon"],
        ),
        axis=1,
    )

    # faixas de plausibilidade
    df["dist_gt_25km"] = df["dist_km_to_expected_municipio"] > 25
    df["dist_gt_50km"] = df["dist_km_to_expected_municipio"] > 50
    df["dist_gt_100km"] = df["dist_km_to_expected_municipio"] > 100
    df["dist_gt_250km"] = df["dist_km_to_expected_municipio"] > 250

    # candidatos a rebaixamento
    # regra pragmática: coordenada por CEP mas muito distante do município esperado
    if "spatial_resolution_final" not in df.columns:
        df["spatial_resolution_final"] = None

    df["candidate_downgrade_to_municipio"] = (
        (df["spatial_resolution_final"] == "cep")
        & (df["dist_gt_100km"].fillna(False))
    )

    print("4/5 - Gerando rankings diagnósticos...")
    top_coord = (
        df[df["has_coord"]]
        .groupby(["lat_round6", "lon_round6"], dropna=False)
        .agg(
            n_cnes=("cnes_codigo", "count"),
            n_municipios=("municipio_codigo_6", "nunique"),
            n_ufs=("uf_codigo", "nunique"),
            example_spatial_resolution=("spatial_resolution_final", "first"),
        )
        .reset_index()
        .sort_values(["n_cnes", "n_municipios", "n_ufs"], ascending=[False, False, False])
    )

    top_dist = (
        df[
            [
                "cnes_codigo",
                "municipio_codigo_6",
                "municipio_nome",
                "uf_sigla",
                "latitude",
                "longitude",
                "centroide_lat",
                "centroide_lon",
                "spatial_resolution_final",
                "spatial_source",
                "geocode_quality",
                "dist_km_to_expected_municipio",
                "candidate_downgrade_to_municipio",
            ]
        ]
        .copy()
        .sort_values("dist_km_to_expected_municipio", ascending=False)
    )

    print("5/5 - Salvando diagnósticos...")
    n_total = len(df)
    n_has_coord = int(df["has_coord"].sum())

    n_res_cep = int((df["spatial_resolution_final"] == "cep").sum())
    n_res_mun = int((df["spatial_resolution_final"] == "municipio").sum())
    n_res_uf = int((df["spatial_resolution_final"] == "uf").sum())

    n_gt_25 = int(df["dist_gt_25km"].fillna(False).sum())
    n_gt_50 = int(df["dist_gt_50km"].fillna(False).sum())
    n_gt_100 = int(df["dist_gt_100km"].fillna(False).sum())
    n_gt_250 = int(df["dist_gt_250km"].fillna(False).sum())

    n_downgrade = int(df["candidate_downgrade_to_municipio"].fillna(False).sum())

    coord_repetidas_10 = int((top_coord["n_cnes"] >= 10).sum())
    coord_repetidas_25 = int((top_coord["n_cnes"] >= 25).sum())
    coord_repetidas_50 = int((top_coord["n_cnes"] >= 50).sum())

    resumo = []
    resumo.append("VALIDAÇÃO DO LOOKUP ESPACIAL CNES DA MENINGITE")
    resumo.append("=" * 80)
    resumo.append(f"Lookup CNES espacial: {CNES_SPATIAL_PATH}")
    resumo.append(f"Lookup municipal IBGE: {IBGE_LOOKUP_PATH}")
    resumo.append("")
    resumo.append("1. Cobertura geral")
    resumo.append("-" * 80)
    resumo.append(f"Total de CNES: {n_total}")
    resumo.append(f"CNES com coordenadas: {n_has_coord} ({pct(n_has_coord, n_total)}%)")
    resumo.append(f"Resolução final = cep: {n_res_cep} ({pct(n_res_cep, n_total)}%)")
    resumo.append(f"Resolução final = municipio: {n_res_mun} ({pct(n_res_mun, n_total)}%)")
    resumo.append(f"Resolução final = uf: {n_res_uf} ({pct(n_res_uf, n_total)}%)")
    resumo.append("")
    resumo.append("2. Distância entre coordenada final e município esperado")
    resumo.append("-" * 80)
    resumo.append(f"> 25 km: {n_gt_25} ({pct(n_gt_25, n_total)}%)")
    resumo.append(f"> 50 km: {n_gt_50} ({pct(n_gt_50, n_total)}%)")
    resumo.append(f"> 100 km: {n_gt_100} ({pct(n_gt_100, n_total)}%)")
    resumo.append(f"> 250 km: {n_gt_250} ({pct(n_gt_250, n_total)}%)")
    resumo.append("")
    resumo.append("3. Coordenadas repetidas")
    resumo.append("-" * 80)
    resumo.append(f"Coordenadas compartilhadas por >= 10 CNES: {coord_repetidas_10}")
    resumo.append(f"Coordenadas compartilhadas por >= 25 CNES: {coord_repetidas_25}")
    resumo.append(f"Coordenadas compartilhadas por >= 50 CNES: {coord_repetidas_50}")
    resumo.append("")
    resumo.append("4. Candidatos a rebaixamento de CEP para município")
    resumo.append("-" * 80)
    resumo.append(f"CNES marcados para rebaixamento: {n_downgrade} ({pct(n_downgrade, n_total)}%)")
    resumo.append("")
    resumo.append("5. Interpretação inicial")
    resumo.append("-" * 80)

    if n_gt_100 == 0:
        resumo.append("Não foram detectados desvios >100 km; a geocodificação por CEP parece coerente.")
    elif pct(n_gt_100, n_total) <= 5:
        resumo.append("Há poucos desvios >100 km; recomenda-se revisão pontual dos casos extremos.")
    else:
        resumo.append("Há volume relevante de desvios >100 km; a geocodificação por CEP precisa revisão sistemática.")

    if coord_repetidas_50 > 0:
        resumo.append("Existem coordenadas altamente repetidas; isso pode indicar geocodificação genérica ou colapso espacial.")
    else:
        resumo.append("Não há sinais fortes de colapso espacial por coordenadas excessivamente repetidas.")

    resumo_texto = "\n".join(resumo)

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
        "latitude",
        "longitude",
        "centroide_lat",
        "centroide_lon",
        "spatial_resolution_final",
        "spatial_source",
        "geocode_quality",
        "dist_km_to_expected_municipio",
        "candidate_downgrade_to_municipio",
    ]
    amostra_cols = [c for c in amostra_cols if c in df.columns]
    df[amostra_cols].sort_values(
        ["candidate_downgrade_to_municipio", "dist_km_to_expected_municipio"],
        ascending=[False, False],
    ).to_csv(OUT_AMOSTRA, index=False)

    top_coord.head(200).to_csv(OUT_TOP_COORD, index=False)
    top_dist.head(500).to_csv(OUT_TOP_DIST, index=False)

    print()
    print(resumo_texto)
    print()
    print(f"Resumo salvo em: {OUT_TXT}")
    print(f"Amostra salva em: {OUT_AMOSTRA}")
    print(f"Top coordenadas salvas em: {OUT_TOP_COORD}")
    print(f"Top distâncias salvas em: {OUT_TOP_DIST}")


if __name__ == "__main__":
    main()

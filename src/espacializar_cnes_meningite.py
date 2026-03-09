# src/espacializar_cnes_meningite.py
#
# Objetivo:
# Espacializar o lookup de CNES da meningite com a seguinte hierarquia:
#
#   1) CEP  -> geocodificação via Nominatim
#   2) Município -> fallback usando centroides do lookup municipal do IBGE
#   3) UF -> fallback usando centroides médios dos municípios da UF
#
# Entradas:
#   - lookup_tables/cnes_meningite_lookup.parquet
#   - lookup_tables/ibge_municipios_espacial.parquet
#
# Saídas:
#   - lookup_tables/cnes_meningite_spatial.parquet
#   - diagnosticos/cnes_meningite_spatial_diagnostico.txt
#   - diagnosticos/cnes_meningite_spatial_amostra.csv
#   - diagnosticos/cache_geocode_cep.csv
#
# Dependências sugeridas:
#   uv add geopy pandas pyarrow duckdb
#
# Uso:
#   uv run src/espacializar_cnes_meningite.py

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd
from geopy.exc import GeocoderServiceError, GeocoderTimedOut, GeocoderUnavailable
from geopy.geocoders import Nominatim

# =========================
# Configuração
# =========================

CNES_LOOKUP_PATH = Path("lookup_tables/cnes_meningite_lookup.parquet")
IBGE_LOOKUP_PATH = Path("lookup_tables/ibge_municipios_espacial.parquet")

OUTPUT_PATH = Path("lookup_tables/cnes_meningite_spatial.parquet")

DIAG_DIR = Path("diagnosticos")
DIAG_DIR.mkdir(parents=True, exist_ok=True)

DIAG_TXT = DIAG_DIR / "cnes_meningite_spatial_diagnostico.txt"
DIAG_CSV = DIAG_DIR / "cnes_meningite_spatial_amostra.csv"
CACHE_CSV = DIAG_DIR / "cache_geocode_cep.csv"

# Nominatim exige identificação clara do cliente
GEOCODER_USER_AGENT = "men_ob_cnes_spatializer_1.0"

# Respeito à política do Nominatim: ~1 req/seg
SLEEP_BETWEEN_REQUESTS = 1.1
GEOCODER_TIMEOUT = 15
MAX_RETRIES = 3

# Para teste rápido, limite o número de CEPs novos a geocodificar.
# Em produção, deixe None.
MAX_NEW_CEPS_TO_GEOCODE = None


# =========================
# Utilidades
# =========================

def pct(n: int, d: int) -> float:
    return round(100.0 * n / d, 2) if d else 0.0


def only_digits(x: object) -> str:
    if x is None:
        return ""
    return "".join(ch for ch in str(x).strip() if ch.isdigit())


def is_valid_cep(x: object) -> bool:
    s = only_digits(x)
    return len(s) == 8 and s != "00000000"


def is_valid_municipio6(x: object) -> bool:
    s = only_digits(x)
    return len(s) == 6 and s != "000000"


def is_valid_uf2(x: object) -> bool:
    s = only_digits(x)
    return len(s) == 2 and s != "00"


# =========================
# Cache de CEP
# =========================

def load_cache() -> pd.DataFrame:
    if CACHE_CSV.exists():
        cache = pd.read_csv(CACHE_CSV, dtype=str)
    else:
        cache = pd.DataFrame(
            columns=[
                "cep_limpo",
                "latitude",
                "longitude",
                "geocode_success",
                "geocode_source",
                "geocode_query",
            ]
        )

    # normaliza
    for col in ["cep_limpo", "latitude", "longitude", "geocode_success", "geocode_source", "geocode_query"]:
        if col not in cache.columns:
            cache[col] = None

    cache["cep_limpo"] = cache["cep_limpo"].fillna("").astype(str)
    cache = cache.drop_duplicates(subset=["cep_limpo"]).reset_index(drop=True)
    return cache


def save_cache(cache: pd.DataFrame) -> None:
    cache.to_csv(CACHE_CSV, index=False)


def append_cache_row(
    cache: pd.DataFrame,
    cep: str,
    latitude: Optional[float],
    longitude: Optional[float],
    success: bool,
    source: str,
    query: str,
) -> pd.DataFrame:
    row = pd.DataFrame(
        [
            {
                "cep_limpo": cep,
                "latitude": latitude,
                "longitude": longitude,
                "geocode_success": str(bool(success)),
                "geocode_source": source,
                "geocode_query": query,
            }
        ]
    )
    cache = pd.concat([cache, row], ignore_index=True)
    cache = cache.drop_duplicates(subset=["cep_limpo"], keep="last").reset_index(drop=True)
    return cache


# =========================
# Geocodificação por CEP
# =========================

def geocode_cep_nominatim(geolocator: Nominatim, cep: str) -> Tuple[Optional[float], Optional[float], bool, str, str]:
    """
    Tenta geocodificar um CEP brasileiro via Nominatim.
    Retorna: latitude, longitude, success, source, query
    """
    query = f"{cep}, Brazil"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            location = geolocator.geocode(
                query,
                exactly_one=True,
                addressdetails=False,
                language="pt-BR",
                country_codes="br",
                timeout=GEOCODER_TIMEOUT,
            )

            time.sleep(SLEEP_BETWEEN_REQUESTS)

            if location is None:
                return None, None, False, "nominatim", query

            return float(location.latitude), float(location.longitude), True, "nominatim", query

        except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError):
            if attempt == MAX_RETRIES:
                return None, None, False, "nominatim", query
            time.sleep(SLEEP_BETWEEN_REQUESTS * attempt)

        except Exception:
            return None, None, False, "nominatim", query

    return None, None, False, "nominatim", query


# =========================
# Carregamento das bases
# =========================

def load_inputs() -> tuple[pd.DataFrame, pd.DataFrame]:
    if not CNES_LOOKUP_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {CNES_LOOKUP_PATH}")

    if not IBGE_LOOKUP_PATH.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {IBGE_LOOKUP_PATH}")

    cnes = pd.read_parquet(CNES_LOOKUP_PATH)
    ibge = pd.read_parquet(IBGE_LOOKUP_PATH)

    # normalização mínima
    if "cep_limpo" not in cnes.columns:
        if "cod_cep" in cnes.columns:
            cnes["cep_limpo"] = cnes["cod_cep"].apply(only_digits)
        else:
            cnes["cep_limpo"] = ""

    cnes["cep_limpo"] = cnes["cep_limpo"].fillna("").astype(str)
    cnes["municipio_codigo_6"] = cnes["municipio_codigo_6"].fillna("").astype(str)
    cnes["uf_codigo"] = cnes["uf_codigo"].fillna("").astype(str)

    ibge["municipio_codigo_6"] = ibge["municipio_codigo_6"].fillna("").astype(str)
    ibge["uf_codigo"] = ibge["uf_codigo"].fillna("").astype(str)

    return cnes, ibge


# =========================
# Fallback territorial
# =========================

def build_uf_centroids(ibge: pd.DataFrame) -> pd.DataFrame:
    """
    Centróides médios por UF, a partir dos centroides municipais.
    """
    uf = (
        ibge.groupby(["uf_codigo", "uf_sigla", "uf_nome"], as_index=False)
        .agg(
            uf_lon=("centroide_lon", "mean"),
            uf_lat=("centroide_lat", "mean"),
        )
    )
    return uf


# =========================
# Main
# =========================

def main() -> None:
    print("1/7 - Carregando bases...")
    cnes, ibge = load_inputs()
    print(f"CNES lookup: {len(cnes):,} linhas")
    print(f"IBGE lookup: {len(ibge):,} linhas")

    print("2/7 - Carregando cache de CEP...")
    cache = load_cache()
    print(f"CEPs já em cache: {len(cache):,}")

    # subset de CEPs válidos e não cacheados
    ceps_validos = sorted(set(c for c in cnes["cep_limpo"].tolist() if is_valid_cep(c)))
    ceps_cacheados = set(cache["cep_limpo"].tolist())
    ceps_novos = [c for c in ceps_validos if c not in ceps_cacheados]

    if MAX_NEW_CEPS_TO_GEOCODE is not None:
        ceps_novos = ceps_novos[:MAX_NEW_CEPS_TO_GEOCODE]

    print("3/7 - Geocodificando CEPs novos...")
    print(f"CEPs válidos no lookup CNES: {len(ceps_validos):,}")
    print(f"CEPs novos a geocodificar: {len(ceps_novos):,}")

    geolocator = Nominatim(user_agent=GEOCODER_USER_AGENT)

    for i, cep in enumerate(ceps_novos, start=1):
        print(f"   [{i}/{len(ceps_novos)}] CEP {cep}")
        lat, lon, success, source, query = geocode_cep_nominatim(geolocator, cep)
        cache = append_cache_row(cache, cep, lat, lon, success, source, query)

        # salva incrementalmente para não perder progresso
        if i % 50 == 0:
            save_cache(cache)

    save_cache(cache)

    print("4/7 - Integrando cache de CEP ao lookup CNES...")
    cache_merge = cache.copy()
    # tipos
    cache_merge["latitude"] = pd.to_numeric(cache_merge["latitude"], errors="coerce")
    cache_merge["longitude"] = pd.to_numeric(cache_merge["longitude"], errors="coerce")
    cache_merge["geocode_success"] = cache_merge["geocode_success"].astype(str)

    out = cnes.merge(
        cache_merge[["cep_limpo", "latitude", "longitude", "geocode_success", "geocode_source"]],
        on="cep_limpo",
        how="left",
    )

    out["latitude"] = pd.to_numeric(out["latitude"], errors="coerce")
    out["longitude"] = pd.to_numeric(out["longitude"], errors="coerce")

    # resolução inicial por CEP
    out["spatial_resolution_final"] = None
    out["latitude_final"] = out["latitude"]
    out["longitude_final"] = out["longitude"]
    out["spatial_source"] = None

    mask_cep_success = out["latitude_final"].notna() & out["longitude_final"].notna()
    out.loc[mask_cep_success, "spatial_resolution_final"] = "cep"
    out.loc[mask_cep_success, "spatial_source"] = "nominatim_cep"

    print("5/7 - Aplicando fallback para município...")
    ibge_mun = ibge[
        [
            "municipio_codigo_6",
            "municipio_nome",
            "uf_codigo",
            "uf_sigla",
            "uf_nome",
            "centroide_lon",
            "centroide_lat",
        ]
    ].drop_duplicates(subset=["municipio_codigo_6"])

    out = out.merge(
        ibge_mun,
        on=["municipio_codigo_6", "uf_codigo"],
        how="left",
        suffixes=("", "_ibge"),
    )

    mask_sem_coord = out["latitude_final"].isna() | out["longitude_final"].isna()
    mask_mun_disponivel = out["centroide_lat"].notna() & out["centroide_lon"].notna()
    mask_mun_fallback = mask_sem_coord & mask_mun_disponivel

    out.loc[mask_mun_fallback, "latitude_final"] = out.loc[mask_mun_fallback, "centroide_lat"]
    out.loc[mask_mun_fallback, "longitude_final"] = out.loc[mask_mun_fallback, "centroide_lon"]
    out.loc[mask_mun_fallback, "spatial_resolution_final"] = "municipio"
    out.loc[mask_mun_fallback, "spatial_source"] = "ibge_municipio_centroid"

    print("6/7 - Aplicando fallback para UF...")
    uf_centroids = build_uf_centroids(ibge)

    out = out.merge(
        uf_centroids,
        on="uf_codigo",
        how="left",
        suffixes=("", "_uf"),
    )

    mask_sem_coord = out["latitude_final"].isna() | out["longitude_final"].isna()
    mask_uf_disponivel = out["uf_lat"].notna() & out["uf_lon"].notna()
    mask_uf_fallback = mask_sem_coord & mask_uf_disponivel

    out.loc[mask_uf_fallback, "latitude_final"] = out.loc[mask_uf_fallback, "uf_lat"]
    out.loc[mask_uf_fallback, "longitude_final"] = out.loc[mask_uf_fallback, "uf_lon"]
    out.loc[mask_uf_fallback, "spatial_resolution_final"] = "uf"
    out.loc[mask_uf_fallback, "spatial_source"] = "ibge_uf_centroid"

    # geocode_quality
    out["geocode_quality"] = out["spatial_resolution_final"].map(
        {
            "cep": "aproximado_por_cep",
            "municipio": "centroide_municipal",
            "uf": "centroide_uf",
        }
    )

    print("7/7 - Salvando artefatos...")
    final_cols = [
        # identificação
        "cnes_codigo",
        "cod_cep",
        "cep_limpo",
        "codufmun",
        "municipio_codigo_6",
        "uf_codigo",
        # administração da unidade
        "tp_unid",
        "tpgestao",
        "esfera_a",
        "nat_jur",
        "competen",
        # controles prévios
        "cep_valido",
        "municipio_valido",
        "uf_valida",
        "spatial_resolution_available",
        "spatial_resolution_target",
        "fallback_rule",
        "cnes_competencia",
        "source_system",
        "lookup_scope",
        # espacialização final
        "latitude_final",
        "longitude_final",
        "spatial_resolution_final",
        "spatial_source",
        "geocode_quality",
        # enriquecimento territorial
        "municipio_nome",
        "uf_sigla",
        "uf_nome",
    ]
    final_cols = [c for c in final_cols if c in out.columns]

    final = out[final_cols].copy()
    final = final.rename(
        columns={
            "latitude_final": "latitude",
            "longitude_final": "longitude",
        }
    ).sort_values("cnes_codigo").reset_index(drop=True)

    final.to_parquet(OUTPUT_PATH, index=False)

    # diagnóstico
    n_total = len(final)
    n_cep = int((final["spatial_resolution_final"] == "cep").sum())
    n_mun = int((final["spatial_resolution_final"] == "municipio").sum())
    n_uf = int((final["spatial_resolution_final"] == "uf").sum())
    n_none = int(final["spatial_resolution_final"].isna().sum())

    resumo = []
    resumo.append("ESPACIALIZAÇÃO DO CNES DA MENINGITE")
    resumo.append("=" * 70)
    resumo.append(f"Lookup de entrada: {CNES_LOOKUP_PATH}")
    resumo.append(f"Lookup municipal: {IBGE_LOOKUP_PATH}")
    resumo.append(f"Lookup espacial de saída: {OUTPUT_PATH}")
    resumo.append("")
    resumo.append("Cobertura final")
    resumo.append("-" * 70)
    resumo.append(f"Total de CNES: {n_total}")
    resumo.append(f"Resolvidos por CEP: {n_cep} ({pct(n_cep, n_total)}%)")
    resumo.append(f"Resolvidos por município: {n_mun} ({pct(n_mun, n_total)}%)")
    resumo.append(f"Resolvidos por UF: {n_uf} ({pct(n_uf, n_total)}%)")
    resumo.append(f"Sem localização final: {n_none} ({pct(n_none, n_total)}%)")
    resumo_text = "\n".join(resumo)

    with open(DIAG_TXT, "w", encoding="utf-8") as f:
        f.write(resumo_text)

    final.head(500).to_csv(DIAG_CSV, index=False)

    print(resumo_text)
    print()
    print(f"Parquet salvo em: {OUTPUT_PATH}")
    print(f"Diagnóstico salvo em: {DIAG_TXT}")
    print(f"Amostra salva em: {DIAG_CSV}")


if __name__ == "__main__":
    main()

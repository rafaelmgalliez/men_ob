from __future__ import annotations

from pathlib import Path

import pandas as pd
from pysus import CNES

OUTPUT_DIR = Path("lookup_tables")
OUTPUT_FILE = OUTPUT_DIR / "cnes_unidades_minimo.parquet"

# Escolha uma competência recente.
# Você pode trocar depois, se quiser.
YEAR = 2025
MONTH = 1

# Candidatos de nomes de colunas, porque o layout pode variar
CANDIDATE_CNES = ["CNES", "CO_CNES"]
CANDIDATE_NAME = ["NOME_FANTASIA", "NO_FANTASIA", "NO_FANTASIA_ESTAB", "NO_RAZAO_SOCIAL", "NO_RAZAO"]
CANDIDATE_LAT = ["NU_LATITUDE", "LATITUDE", "VL_LATITUDE"]
CANDIDATE_LON = ["NU_LONGITUDE", "LONGITUDE", "VL_LONGITUDE"]


def pick_column(df: pd.DataFrame, candidates: list[str], required: bool = True) -> str | None:
    cols_upper = {c.upper(): c for c in df.columns}
    for cand in candidates:
        if cand.upper() in cols_upper:
            return cols_upper[cand.upper()]
    if required:
        raise KeyError(f"Nenhuma das colunas candidatas encontrada: {candidates}")
    return None


def load_cnes_st_competencia(year: int, month: int) -> pd.DataFrame:
    cnes = CNES()

    # Grupo ST = Estabelecimentos
    files = cnes.get_files("ST", year=year, month=month)

    if not files:
        raise RuntimeError(f"Nenhum arquivo ST encontrado para {year}-{month:02d}")

    print(f"Arquivos ST encontrados: {len(files)}")

    parquets = cnes.download(files)

    dfs = []
    for pq_file in parquets:
        df = pq_file.to_dataframe()
        dfs.append(df)

    out = pd.concat(dfs, ignore_index=True)
    return out


def gerar_lookup_cnes_minimo() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"1/3 - Baixando CNES grupo ST para {YEAR}-{MONTH:02d}...")
    df = load_cnes_st_competencia(YEAR, MONTH)

    print(f"Linhas brutas: {len(df):,}")

    print("2/3 - Detectando colunas principais...")
    col_cnes = pick_column(df, CANDIDATE_CNES, required=True)
    col_name = pick_column(df, CANDIDATE_NAME, required=False)
    col_lat = pick_column(df, CANDIDATE_LAT, required=False)
    col_lon = pick_column(df, CANDIDATE_LON, required=False)

    print(f"CNES: {col_cnes}")
    print(f"Nome: {col_name}")
    print(f"Latitude: {col_lat}")
    print(f"Longitude: {col_lon}")

    if col_lat is None or col_lon is None:
        raise RuntimeError(
            "Não encontrei colunas de latitude/longitude. "
            "Rode primeiro um script de inspeção de colunas ou ajuste os nomes candidatos."
        )

    out = pd.DataFrame({
        "cnes_codigo": df[col_cnes].astype(str).str.strip(),
        "nome_unidade": df[col_name].astype(str).str.strip() if col_name else None,
        "latitude": pd.to_numeric(df[col_lat], errors="coerce"),
        "longitude": pd.to_numeric(df[col_lon], errors="coerce"),
    })

    # remove CNES vazio
    out = out[out["cnes_codigo"].notna() & (out["cnes_codigo"] != "")]
    out = out.drop_duplicates(subset=["cnes_codigo"]).reset_index(drop=True)

    print("3/3 - Salvando lookup CNES mínimo...")
    out.to_parquet(OUTPUT_FILE, index=False)

    print(f"Arquivo salvo em: {OUTPUT_FILE}")
    print(f"Total de unidades únicas: {len(out):,}")
    print(out.head())


if __name__ == "__main__":
    gerar_lookup_cnes_minimo()

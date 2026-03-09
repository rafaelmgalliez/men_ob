# src/gerar_lookup_cnes_meningite.py
#
# Gera o lookup canônico de CNES para o projeto Men_Ob,
# restrito aos estabelecimentos que aparecem no parquet de meningite.
#
# Estratégia:
# 1) extrai os CNES distintos do SINAN
# 2) baixa todos os arquivos ST da competência escolhida
# 3) empilha o ST nacional
# 4) filtra apenas os CNES da meningite
# 5) consolida 1 linha por CNES
# 6) prepara a hierarquia espacial:
#       CEP -> município -> UF
# 7) salva parquet canônico em lookup_tables/
#
# Uso:
# uv run src/gerar_lookup_cnes_meningite.py

import ftplib
import os
import re
import tempfile
from pathlib import Path

import duckdb
import pandas as pd
from dbfread import DBF
from readdbc import dbc2dbf

SINAN_PATH = "datalake/sinan/meningite_br.parquet"

FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR_ST = "/dissemin/publicos/CNES/200508_/Dados/ST"

COMPETENCIA_AAAA = 2026
COMPETENCIA_MM = 1

OUTPUT_DIR = Path("lookup_tables")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "cnes_meningite_lookup.parquet"


def pct(numerador: int, denominador: int) -> float:
    return round(100.0 * numerador / denominador, 2) if denominador else 0.0


def aa_mm_from_year_month(year: int, month: int) -> tuple[str, str]:
    aa = str(year)[-2:].zfill(2)
    mm = str(month).zfill(2)
    return aa, mm


def only_digits(x: object) -> str:
    if x is None:
        return ""
    return re.sub(r"\D", "", str(x).strip())


def is_valid_cep(x: object) -> bool:
    s = only_digits(x)
    return len(s) == 8 and s != "00000000"


def is_valid_municipio6(x: object) -> bool:
    s = only_digits(x)
    return len(s) == 6 and s != "000000"


def is_valid_uf2(x: object) -> bool:
    s = only_digits(x)
    return len(s) == 2 and s != "00"


def extrair_cnes_distintos_sinan() -> pd.DataFrame:
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT DISTINCT
            TRIM(CAST(ID_UNIDADE AS VARCHAR)) AS cnes_codigo
        FROM '{SINAN_PATH}'
        WHERE ID_UNIDADE IS NOT NULL
          AND TRIM(CAST(ID_UNIDADE AS VARCHAR)) <> ''
        ORDER BY 1
    """).df()
    df["cnes_codigo"] = df["cnes_codigo"].astype(str).str.strip()
    return df


def listar_arquivos_st_competencia(ftp: ftplib.FTP, year: int, month: int) -> list[str]:
    aa, mm = aa_mm_from_year_month(year, month)
    arquivos = ftp.nlst()
    arquivos = [a for a in arquivos if a.lower().endswith(".dbc")]
    arquivos = [a for a in arquivos if a.upper().startswith("ST") and a[-8:-6] == aa and a[-6:-4] == mm]
    arquivos.sort()
    return arquivos


def ler_dbc_para_dataframe(ftp: ftplib.FTP, arquivo_dbc: str, temp_dir: str) -> pd.DataFrame:
    caminho_dbc = os.path.join(temp_dir, arquivo_dbc)
    caminho_dbf = os.path.join(temp_dir, arquivo_dbc.replace(".dbc", ".dbf"))

    with open(caminho_dbc, "wb") as f:
        ftp.retrbinary(f"RETR {arquivo_dbc}", f.write)

    dbc2dbf(caminho_dbc, caminho_dbf)

    tabela = DBF(caminho_dbf, encoding="iso-8859-1", load=True)
    return pd.DataFrame(iter(tabela))


def baixar_st_nacional_competencia(year: int, month: int) -> pd.DataFrame:
    print(f"Conectando ao FTP: {FTP_HOST}")
    ftp = ftplib.FTP(FTP_HOST)
    ftp.login("anonymous", "anonymous")
    ftp.cwd(FTP_DIR_ST)

    arquivos = listar_arquivos_st_competencia(ftp, year, month)
    print(f"Arquivos ST encontrados para {year}-{str(month).zfill(2)}: {len(arquivos)}")

    if not arquivos:
        ftp.quit()
        raise RuntimeError("Nenhum arquivo ST encontrado para a competência informada.")

    frames = []
    with tempfile.TemporaryDirectory() as temp_dir:
        for i, arquivo in enumerate(arquivos, start=1):
            print(f"   [{i}/{len(arquivos)}] {arquivo}")
            frames.append(ler_dbc_para_dataframe(ftp, arquivo, temp_dir))

    ftp.quit()
    return pd.concat(frames, ignore_index=True)


def selecionar_colunas_interesse(df: pd.DataFrame) -> pd.DataFrame:
    keep_cols = [
        "CNES",
        "CODUFMUN",
        "COD_CEP",
        "TP_UNID",
        "TPGESTAO",
        "ESFERA_A",
        "NAT_JUR",
        "COMPETEN",
    ]
    cols = [c for c in keep_cols if c in df.columns]
    return df[cols].copy()


def consolidar_por_cnes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    def primeiro_nao_vazio(series: pd.Series):
        for val in series:
            if pd.notna(val) and str(val).strip() != "":
                return val
        return None

    return (
        df.groupby("CNES", as_index=False)
        .agg({col: primeiro_nao_vazio for col in df.columns if col != "CNES"})
    )


def montar_lookup(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["cnes_codigo"] = out["CNES"].astype(str).str.strip()
    out["cod_cep"] = out["COD_CEP"].astype(str).str.strip() if "COD_CEP" in out.columns else ""
    out["codufmun"] = out["CODUFMUN"].astype(str).str.strip() if "CODUFMUN" in out.columns else ""

    out["cep_limpo"] = out["cod_cep"].apply(only_digits)
    out["codufmun_limpo"] = out["codufmun"].apply(only_digits)

    out["cep_valido"] = out["cep_limpo"].apply(is_valid_cep)
    out["municipio_codigo_6"] = out["codufmun_limpo"].str[:6]
    out["municipio_valido"] = out["municipio_codigo_6"].apply(is_valid_municipio6)

    out["uf_codigo"] = out["municipio_codigo_6"].str[:2]
    out["uf_valida"] = out["uf_codigo"].apply(is_valid_uf2)

    # resolução disponível no lookup
    out["spatial_resolution_available"] = "sem_localizacao"
    out.loc[out["uf_valida"], "spatial_resolution_available"] = "uf"
    out.loc[out["municipio_valido"], "spatial_resolution_available"] = "municipio"
    out.loc[out["cep_valido"], "spatial_resolution_available"] = "cep"

    # resolução alvo da estratégia do projeto
    out["spatial_resolution_target"] = "cep"
    out["fallback_rule"] = "cep->municipio->uf"

    # renomear colunas administrativas
    rename_map = {
        "TP_UNID": "tp_unid",
        "TPGESTAO": "tpgestao",
        "ESFERA_A": "esfera_a",
        "NAT_JUR": "nat_jur",
        "COMPETEN": "competen",
    }
    out = out.rename(columns=rename_map)

    # metadados de proveniência
    out["cnes_competencia"] = f"{COMPETENCIA_AAAA}-{str(COMPETENCIA_MM).zfill(2)}"
    out["source_system"] = "CNES / DATASUS / ST"
    out["lookup_scope"] = "CNES presentes no SINAN meningite"

    keep_final = [
        "cnes_codigo",
        "cod_cep",
        "cep_limpo",
        "cep_valido",
        "codufmun",
        "municipio_codigo_6",
        "municipio_valido",
        "uf_codigo",
        "uf_valida",
        "tp_unid",
        "tpgestao",
        "esfera_a",
        "nat_jur",
        "competen",
        "spatial_resolution_available",
        "spatial_resolution_target",
        "fallback_rule",
        "cnes_competencia",
        "source_system",
        "lookup_scope",
    ]
    keep_final = [c for c in keep_final if c in out.columns]

    out = out[keep_final].copy()
    out = out.sort_values("cnes_codigo").reset_index(drop=True)
    return out


def main():
    print("1/6 - Extraindo CNES distintos do SINAN...")
    cnes_sinan = extrair_cnes_distintos_sinan()
    cnes_set = set(cnes_sinan["cnes_codigo"].tolist())
    print(f"CNES distintos no SINAN: {len(cnes_sinan):,}")

    print("2/6 - Baixando ST nacional da competência...")
    st = baixar_st_nacional_competencia(COMPETENCIA_AAAA, COMPETENCIA_MM)
    print(f"Linhas totais no ST nacional: {len(st):,}")

    if "CNES" not in st.columns:
        raise RuntimeError("Coluna CNES não encontrada no ST nacional.")

    st["CNES"] = st["CNES"].astype(str).str.strip()

    print("3/6 - Filtrando CNES da meningite...")
    st_filtrado = st[st["CNES"].isin(cnes_set)].copy()
    n_match = st_filtrado["CNES"].nunique()
    n_total = len(cnes_sinan)

    print(f"CNES com match no ST nacional: {n_match:,}")
    print(f"CNES sem match no ST nacional: {n_total - n_match:,}")
    print(f"Cobertura do ST sobre o SINAN: {pct(n_match, n_total)}%")

    print("4/6 - Selecionando colunas de interesse...")
    st_filtrado = selecionar_colunas_interesse(st_filtrado)

    print("5/6 - Consolidando uma linha por CNES...")
    st_consolidado = consolidar_por_cnes(st_filtrado)
    lookup = montar_lookup(st_consolidado)

    print("6/6 - Salvando parquet canônico...")
    lookup.to_parquet(OUTPUT_FILE, index=False)

    n_lookup = len(lookup)
    n_cep = int(lookup["cep_valido"].sum()) if "cep_valido" in lookup.columns else 0
    n_mun = int(lookup["municipio_valido"].sum()) if "municipio_valido" in lookup.columns else 0
    n_uf = int(lookup["uf_valida"].sum()) if "uf_valida" in lookup.columns else 0

    print(f"Arquivo salvo em: {OUTPUT_FILE}")
    print(f"Linhas no lookup final: {n_lookup:,}")
    print(f"CEP válido: {n_cep} ({pct(n_cep, n_lookup)}%)")
    print(f"Município válido: {n_mun} ({pct(n_mun, n_lookup)}%)")
    print(f"UF válida: {n_uf} ({pct(n_uf, n_lookup)}%)")
    print("\nAmostra:")
    print(lookup.head())


if __name__ == "__main__":
    main()

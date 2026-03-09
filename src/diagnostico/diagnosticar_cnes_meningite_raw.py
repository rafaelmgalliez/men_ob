# src/diagnostico/diagnosticar_cnes_meningite_raw.py
#
# Diagnóstico nacional do CNES para os estabelecimentos presentes no SINAN meningite.
#
# Objetivo:
# 1) extrair os CNES distintos (ID_UNIDADE) do parquet de meningite
# 2) baixar TODOS os arquivos ST de uma competência CNES
# 3) empilhar em memória os arquivos estaduais da competência
# 4) filtrar apenas os CNES presentes na meningite
# 5) medir se COD_CEP e CODUFMUN são bons o suficiente para espacialização
#
# Saídas:
# - imprime resumo no terminal
# - salva um TXT com o resumo em diagnosticos/
# - salva um CSV pequeno com amostra diagnóstica em diagnosticos/
#
# Não gera parquet canônico nesta etapa.
#
# Uso:
# uv run src/diagnostico/diagnosticar_cnes_meningite_raw.py

import ftplib
import os
import re
import tempfile
from pathlib import Path

import duckdb
import pandas as pd
from dbfread import DBF
from readdbc import dbc2dbf

# =========================
# Configuração
# =========================

SINAN_PATH = "datalake/sinan/meningite_br.parquet"

FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR_ST = "/dissemin/publicos/CNES/200508_/Dados/ST"

# Competência alvo do CNES
COMPETENCIA_AAAA = 2026
COMPETENCIA_MM = 1

OUTPUT_DIR = Path("diagnosticos")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_TXT = OUTPUT_DIR / "cnes_meningite_diagnostico.txt"
OUTPUT_CSV = OUTPUT_DIR / "cnes_meningite_diagnostico_amostra.csv"

# Se quiser limitar o número de arquivos baixados para teste rápido, altere aqui.
# Em produção diagnóstica, deixe como None.
MAX_ARQUIVOS_ST = None


# =========================
# Utilidades
# =========================

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
    if len(s) != 8:
        return False
    if s == "00000000":
        return False
    return True


def is_valid_municipio6(x: object) -> bool:
    s = only_digits(x)
    if len(s) != 6:
        return False
    if s == "000000":
        return False
    return True


def is_valid_uf2(x: object) -> bool:
    s = only_digits(x)
    return len(s) == 2 and s != "00"


# =========================
# Extração do SINAN
# =========================

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


# =========================
# CNES / FTP
# =========================

def listar_arquivos_st_competencia(ftp: ftplib.FTP, year: int, month: int) -> list[str]:
    aa, mm = aa_mm_from_year_month(year, month)
    arquivos = ftp.nlst()
    arquivos = [a for a in arquivos if a.lower().endswith(".dbc")]
    arquivos = [a for a in arquivos if a.upper().startswith("ST") and a[-8:-6] == aa and a[-6:-4] == mm]
    arquivos.sort()

    if MAX_ARQUIVOS_ST is not None:
        arquivos = arquivos[:MAX_ARQUIVOS_ST]

    return arquivos


def ler_dbc_para_dataframe(ftp: ftplib.FTP, arquivo_dbc: str, temp_dir: str) -> pd.DataFrame:
    caminho_dbc = os.path.join(temp_dir, arquivo_dbc)
    caminho_dbf = os.path.join(temp_dir, arquivo_dbc.replace(".dbc", ".dbf"))

    with open(caminho_dbc, "wb") as f:
        ftp.retrbinary(f"RETR {arquivo_dbc}", f.write)

    dbc2dbf(caminho_dbc, caminho_dbf)

    tabela = DBF(caminho_dbf, encoding="iso-8859-1", load=True)
    df = pd.DataFrame(iter(tabela))
    return df


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

    print("Primeiros 10 arquivos:")
    for a in arquivos[:10]:
        print(f" - {a}")

    frames = []
    with tempfile.TemporaryDirectory() as temp_dir:
        for i, arquivo in enumerate(arquivos, start=1):
            print(f"   [{i}/{len(arquivos)}] {arquivo}")
            df = ler_dbc_para_dataframe(ftp, arquivo, temp_dir)
            frames.append(df)

    ftp.quit()

    st = pd.concat(frames, ignore_index=True)
    return st


# =========================
# Consolidação diagnóstica
# =========================

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
    existentes = [c for c in keep_cols if c in df.columns]
    return df[existentes].copy()


def consolidar_por_cnes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consolida múltiplas linhas por CNES mantendo a primeira ocorrência não vazia por coluna.
    Isso é melhor que simplesmente drop_duplicates na primeira linha.
    """
    if df.empty:
        return df.copy()

    def primeiro_nao_vazio(series: pd.Series):
        for val in series:
            if pd.notna(val) and str(val).strip() != "":
                return val
        return None

    grouped = (
        df.groupby("CNES", as_index=False)
        .agg({col: primeiro_nao_vazio for col in df.columns if col != "CNES"})
    )

    return grouped


def montar_diagnostico(df: pd.DataFrame) -> pd.DataFrame:
    diagnostico = df.copy()

    diagnostico["cnes_codigo"] = diagnostico["CNES"].astype(str).str.strip()

    if "COD_CEP" in diagnostico.columns:
        diagnostico["cod_cep"] = diagnostico["COD_CEP"].astype(str).str.strip()
    else:
        diagnostico["cod_cep"] = ""

    if "CODUFMUN" in diagnostico.columns:
        diagnostico["codufmun"] = diagnostico["CODUFMUN"].astype(str).str.strip()
    else:
        diagnostico["codufmun"] = ""

    diagnostico["cep_limpo"] = diagnostico["cod_cep"].apply(only_digits)
    diagnostico["cep_valido"] = diagnostico["cep_limpo"].apply(is_valid_cep)

    diagnostico["codufmun_limpo"] = diagnostico["codufmun"].apply(only_digits)
    diagnostico["municipio_codigo_6"] = diagnostico["codufmun_limpo"].str[:6]
    diagnostico["municipio6_valido"] = diagnostico["municipio_codigo_6"].apply(is_valid_municipio6)

    diagnostico["uf_codigo"] = diagnostico["municipio_codigo_6"].str[:2]
    diagnostico["uf_valida"] = diagnostico["uf_codigo"].apply(is_valid_uf2)

    # resolução espacial potencial por hierarquia
    diagnostico["spatial_resolution_potencial"] = "sem_localizacao"
    diagnostico.loc[diagnostico["uf_valida"], "spatial_resolution_potencial"] = "uf"
    diagnostico.loc[diagnostico["municipio6_valido"], "spatial_resolution_potencial"] = "municipio"
    diagnostico.loc[diagnostico["cep_valido"], "spatial_resolution_potencial"] = "cep"

    return diagnostico


def gerar_resumo(diagnostico: pd.DataFrame, n_total_sinan: int, n_match_st: int, competencia_aaaa: int, competencia_mm: int) -> str:
    n_unicos = len(diagnostico)
    n_sem_match = n_total_sinan - n_match_st

    mask_cep_preenchido = diagnostico["cod_cep"].astype(str).str.strip() != ""
    mask_mun_preenchido = diagnostico["codufmun"].astype(str).str.strip() != ""

    n_cep_preenchido = int(mask_cep_preenchido.sum())
    n_cep_valido = int(diagnostico["cep_valido"].sum())

    n_mun_preenchido = int(mask_mun_preenchido.sum())
    n_mun_valido = int(diagnostico["municipio6_valido"].sum())

    n_uf_valida = int(diagnostico["uf_valida"].sum())

    n_res_cep = int((diagnostico["spatial_resolution_potencial"] == "cep").sum())
    n_res_municipio = int((diagnostico["spatial_resolution_potencial"] == "municipio").sum())
    n_res_uf = int((diagnostico["spatial_resolution_potencial"] == "uf").sum())
    n_res_none = int((diagnostico["spatial_resolution_potencial"] == "sem_localizacao").sum())

    freq_cep = diagnostico.loc[diagnostico["cep_valido"], "cep_limpo"].value_counts()
    n_ceps_validos_unicos = int((freq_cep >= 1).sum())
    n_ceps_compartilhados = int((freq_cep > 1).sum())

    linhas = []
    linhas.append("DIAGNÓSTICO CNES PARA MENINGITE")
    linhas.append("=" * 70)
    linhas.append(f"Competência CNES usada: {competencia_aaaa}-{str(competencia_mm).zfill(2)}")
    linhas.append(f"Arquivo SINAN: {SINAN_PATH}")
    linhas.append("")

    linhas.append("1. Cobertura da chave CNES")
    linhas.append("-" * 70)
    linhas.append(f"CNES distintos no SINAN: {n_total_sinan}")
    linhas.append(f"CNES com match no ST nacional: {n_match_st}")
    linhas.append(f"CNES sem match no ST nacional: {n_sem_match}")
    linhas.append(f"Cobertura ST → SINAN: {pct(n_match_st, n_total_sinan)}%")
    linhas.append("")

    linhas.append("2. Qualidade de COD_CEP")
    linhas.append("-" * 70)
    linhas.append(f"CNES únicos diagnosticados: {n_unicos}")
    linhas.append(f"COD_CEP preenchido: {n_cep_preenchido} ({pct(n_cep_preenchido, n_unicos)}%)")
    linhas.append(f"COD_CEP válido (8 dígitos): {n_cep_valido} ({pct(n_cep_valido, n_unicos)}%)")
    linhas.append(f"CEPs válidos únicos: {n_ceps_validos_unicos}")
    linhas.append(f"CEPs válidos compartilhados por >1 CNES: {n_ceps_compartilhados}")
    linhas.append("")

    linhas.append("3. Qualidade de CODUFMUN")
    linhas.append("-" * 70)
    linhas.append(f"CODUFMUN preenchido: {n_mun_preenchido} ({pct(n_mun_preenchido, n_unicos)}%)")
    linhas.append(f"Município válido (6 dígitos): {n_mun_valido} ({pct(n_mun_valido, n_unicos)}%)")
    linhas.append(f"UF derivável válida: {n_uf_valida} ({pct(n_uf_valida, n_unicos)}%)")
    linhas.append("")

    linhas.append("4. Resolução espacial potencial")
    linhas.append("-" * 70)
    linhas.append(f"CEP: {n_res_cep} ({pct(n_res_cep, n_unicos)}%)")
    linhas.append(f"Município: {n_res_municipio} ({pct(n_res_municipio, n_unicos)}%)")
    linhas.append(f"UF: {n_res_uf} ({pct(n_res_uf, n_unicos)}%)")
    linhas.append(f"Sem localização: {n_res_none} ({pct(n_res_none, n_unicos)}%)")
    linhas.append("")

    linhas.append("5. Interpretação inicial")
    linhas.append("-" * 70)
    if pct(n_cep_valido, n_unicos) >= 80:
        linhas.append("CEP parece forte como primeira resolução espacial.")
    elif pct(n_cep_valido, n_unicos) >= 50:
        linhas.append("CEP parece utilizável, mas exigirá fallback importante para município.")
    else:
        linhas.append("CEP parece fraco; município tende a ser a resolução dominante.")

    if pct(n_mun_valido, n_unicos) >= 95:
        linhas.append("Município parece excelente como fallback.")
    elif pct(n_mun_valido, n_unicos) >= 80:
        linhas.append("Município parece bom como fallback.")
    else:
        linhas.append("Município também exige cautela.")

    # top TP_UNID
    if "TP_UNID" in diagnostico.columns:
        linhas.append("")
        linhas.append("6. Top 15 TP_UNID")
        linhas.append("-" * 70)
        top_tp_unid = diagnostico["TP_UNID"].astype(str).value_counts().head(15)
        for k, v in top_tp_unid.items():
            linhas.append(f"{k}: {v}")

    return "\n".join(linhas)


# =========================
# Main
# =========================

def main():
    print("1/5 - Extraindo CNES distintos do SINAN...")
    cnes_sinan = extrair_cnes_distintos_sinan()
    cnes_set = set(cnes_sinan["cnes_codigo"].tolist())
    n_total_sinan = len(cnes_sinan)
    print(f"CNES distintos no SINAN: {n_total_sinan:,}")

    print("2/5 - Baixando ST nacional da competência...")
    st = baixar_st_nacional_competencia(COMPETENCIA_AAAA, COMPETENCIA_MM)
    print(f"Linhas totais no ST nacional empilhado: {len(st):,}")

    if "CNES" not in st.columns:
        raise RuntimeError("Coluna CNES não encontrada no ST nacional.")

    st["CNES"] = st["CNES"].astype(str).str.strip()

    print("3/5 - Filtrando apenas os CNES presentes na meningite...")
    st_filtrado = st[st["CNES"].isin(cnes_set)].copy()
    n_match_st = st_filtrado["CNES"].nunique()

    print(f"Linhas filtradas: {len(st_filtrado):,}")
    print(f"CNES com match no ST nacional: {n_match_st:,}")
    print(f"CNES sem match no ST nacional: {n_total_sinan - n_match_st:,}")
    print(f"Cobertura do ST sobre o SINAN: {pct(n_match_st, n_total_sinan)}%")

    print("4/5 - Consolidando informações por CNES...")
    st_filtrado = selecionar_colunas_interesse(st_filtrado)
    st_consolidado = consolidar_por_cnes(st_filtrado)
    diagnostico = montar_diagnostico(st_consolidado)

    print(f"CNES únicos no dataset consolidado: {len(diagnostico):,}")

    print("5/5 - Gerando resumo diagnóstico...")
    resumo = gerar_resumo(
        diagnostico=diagnostico,
        n_total_sinan=n_total_sinan,
        n_match_st=n_match_st,
        competencia_aaaa=COMPETENCIA_AAAA,
        competencia_mm=COMPETENCIA_MM,
    )

    print()
    print(resumo)

    # salva apenas artefatos leves
    with open(OUTPUT_TXT, "w", encoding="utf-8") as f:
        f.write(resumo)

    cols_amostra = [
        "cnes_codigo",
        "cod_cep",
        "cep_limpo",
        "cep_valido",
        "codufmun",
        "municipio_codigo_6",
        "municipio6_valido",
        "uf_codigo",
        "uf_valida",
        "spatial_resolution_potencial",
    ]

    # acrescenta colunas administrativas se existirem
    for col in ["TP_UNID", "TPGESTAO", "ESFERA_A", "NAT_JUR", "COMPETEN"]:
        if col in diagnostico.columns:
            cols_amostra.append(col)

    amostra = diagnostico[cols_amostra].copy()
    amostra.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")

    print()
    print(f"Resumo salvo em: {OUTPUT_TXT}")
    print(f"CSV diagnóstico salvo em: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

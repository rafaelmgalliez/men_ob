"""
Módulo de Extração do SINAN - Projeto Men_Ob (Arquitetura Raiz)
---------------------------------------------------------------
Conecta ao FTP do DATASUS, baixa arquivos proprietários (.dbc),
descompacta nativamente para (.dbf) e converte direto para Parquet (.parquet)
usando PyArrow (Zero Pandas, Zero PySUS).
"""

import ftplib
import os
import tempfile
from pathlib import Path

from readdbc import dbc2dbf
from dbfread import DBF
import pyarrow as pa
import pyarrow.parquet as pq

FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR = "/dissemin/publicos/SINAN/DADOS/FINAIS"
PREFIXO = "MENIBR"

CAMINHO_DATALAKE = Path("datalake/sinan")
CAMINHO_DATALAKE.mkdir(parents=True, exist_ok=True)
ARQUIVO_FINAL = CAMINHO_DATALAKE / "meningite_br.parquet"

def extrair_meningite_sinan():
    print(f"🔄 1/4 - Conectando ao servidor do Ministério da Saúde ({FTP_HOST})...")
    
    ftp = ftplib.FTP(FTP_HOST)
    ftp.login("anonymous", "anonymous")
    ftp.cwd(FTP_DIR)
    
    arquivos_ftp = [f for f in ftp.nlst() if f.startswith(PREFIXO) and f.endswith(".dbc")]
    arquivos_ftp.sort()
    
    print(f"📂 Encontrados {len(arquivos_ftp)} anos de dados históricos.")
    
    tabelas_arrow = [] # Lista para guardar as tabelas otimizadas na memória
    
    print("⬇️ 2/4 - Baixando e convertendo arquivos (Raiz)...")
    with tempfile.TemporaryDirectory() as temp_dir:
        
        for arquivo in arquivos_ftp:
            caminho_dbc = os.path.join(temp_dir, arquivo)
            caminho_dbf = os.path.join(temp_dir, arquivo.replace(".dbc", ".dbf"))
            
            # 1. Download nativo
            print(f"   ⬇️ Baixando {arquivo}...")
            with open(caminho_dbc, "wb") as f:
                ftp.retrbinary(f"RETR {arquivo}", f.write)
            
            # 2. Descompressão isolada em C (DBC -> DBF)
            print(f"   ⚙️ Descompactando para DBF...")
            dbc2dbf(caminho_dbc, caminho_dbf)
            
            # 3. Leitura do DBF e conversão para PyArrow Table
            print(f"   🚀 Vetorizando para PyArrow...")
            # Usamos iso-8859-1 para preservar acentos originais do DATASUS
            tabela_dbf = DBF(caminho_dbf, encoding="iso-8859-1", load=True)
            
            # Converte a lista de dicionários do DBF direto para o formato colunar do Arrow
            tabela_arrow = pa.Table.from_pylist(list(tabela_dbf))
            tabelas_arrow.append(tabela_arrow)
            
    ftp.quit()
    
    print("🥞 3/4 - Empilhando tabelas (Zero Pandas)...")
    # Concatena todos os anos de forma nativa e rápida no Arrow
    tabela_consolidada = pa.concat_tables(tabelas_arrow)
    
    print(f"💾 4/4 - Salvando Parquet em {ARQUIVO_FINAL}...")
    pq.write_table(tabela_consolidada, ARQUIVO_FINAL)
    
    tamanho_mb = ARQUIVO_FINAL.stat().st_size / (1024 * 1024)
    print("\n✅ EXTRAÇÃO CONCLUÍDA COM SUCESSO!")
    print(f"📊 Total de Registros: {tabela_consolidada.num_rows:,}".replace(",", "."))
    print(f"🪶 Tamanho do Data Lake: {tamanho_mb:.2f} MB")

if __name__ == "__main__":
    extrair_meningite_sinan()

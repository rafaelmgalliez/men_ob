import ftplib
import os
import tempfile

from dbfread import DBF
from readdbc import dbc2dbf

FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR_BASE = "/dissemin/publicos/CNES/200508_/Dados"

# Grupos que ficaram suspeitos no scan inicial
GRUPOS = ["DC", "EP", "PF", "ST"]

# Padrões mais específicos
PADROES = [
    "CEP",
    "LOGRA",
    "ENDERE",
    "BAIR",
    "NUM",
    "COMPL",
    "FANT",
    "RAZA",
    "NOME",
]


def listar_arquivos_dbc(ftp: ftplib.FTP) -> list[str]:
    arquivos = ftp.nlst()
    arquivos = [a for a in arquivos if a.lower().endswith(".dbc")]
    arquivos.sort()
    return arquivos


def escolher_arquivo_recente(arquivos: list[str]) -> str | None:
    if not arquivos:
        return None
    return arquivos[-1]


def filtrar_colunas_interesse(campos: list[str]) -> list[str]:
    cols = []
    for c in campos:
        cu = c.upper()
        if any(p in cu for p in PADROES):
            cols.append(c)
    return cols


def inspecionar_grupo(ftp: ftplib.FTP, grupo: str) -> None:
    caminho = f"{FTP_DIR_BASE}/{grupo}"
    print("\n" + "=" * 90)
    print(f"GRUPO: {grupo}")
    print("=" * 90)

    ftp.cwd(caminho)
    arquivos = listar_arquivos_dbc(ftp)

    print(f"Total de .dbc encontrados: {len(arquivos)}")

    arquivo = escolher_arquivo_recente(arquivos)
    if arquivo is None:
        print("Nenhum arquivo .dbc encontrado.")
        ftp.cwd(FTP_DIR_BASE)
        return

    print(f"Arquivo escolhido: {arquivo}")

    with tempfile.TemporaryDirectory() as temp_dir:
        caminho_dbc = os.path.join(temp_dir, arquivo)
        caminho_dbf = os.path.join(temp_dir, arquivo.replace(".dbc", ".dbf"))

        print("Baixando...")
        with open(caminho_dbc, "wb") as f:
            ftp.retrbinary(f"RETR {arquivo}", f.write)

        print("Convertendo DBC -> DBF...")
        dbc2dbf(caminho_dbc, caminho_dbf)

        print("Lendo DBF...")
        tabela = DBF(caminho_dbf, encoding="iso-8859-1", load=True)

        campos = list(tabela.field_names)
        campos_interesse = filtrar_colunas_interesse(campos)

        print(f"\nTotal de colunas: {len(campos)}")

        print("\nTodas as colunas:")
        for c in campos:
            print(" -", c)

        print("\nColunas de interesse:")
        if campos_interesse:
            for c in campos_interesse:
                print(" -", c)
        else:
            print(" - nenhuma")

        if len(tabela) > 0:
            row = next(iter(tabela))
            print("\nPrimeiro registro nas colunas de interesse:")
            if campos_interesse:
                for c in campos_interesse:
                    print(f"  {c}: {row.get(c)}")
            else:
                print("  nenhuma coluna de interesse encontrada")

    ftp.cwd(FTP_DIR_BASE)


def main():
    print(f"Conectando ao FTP: {FTP_HOST}")
    ftp = ftplib.FTP(FTP_HOST)
    ftp.login("anonymous", "anonymous")
    ftp.cwd(FTP_DIR_BASE)

    print("Diretórios disponíveis:")
    for item in ftp.nlst():
        print(" -", item)

    for grupo in GRUPOS:
        inspecionar_grupo(ftp, grupo)

    ftp.quit()
    print("\nInspeção concluída.")


if __name__ == "__main__":
    main()

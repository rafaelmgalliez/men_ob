import ftplib
import os
import tempfile

from readdbc import dbc2dbf
from dbfread import DBF

FTP_HOST = "ftp.datasus.gov.br"
FTP_DIR_BASE = "/dissemin/publicos/CNES/200508_/Dados"
GRUPO = "ST"


def listar_conteudo(ftp: ftplib.FTP) -> list[str]:
    return ftp.nlst()


def listar_arquivos_dbc(ftp: ftplib.FTP) -> list[str]:
    arquivos = ftp.nlst()
    arquivos_dbc = [a for a in arquivos if a.lower().endswith(".dbc")]
    arquivos_dbc.sort()
    return arquivos_dbc


def escolher_arquivo(arquivos: list[str]) -> str:
    if not arquivos:
        raise RuntimeError("Nenhum arquivo .dbc encontrado no diretório do grupo ST.")
    return arquivos[-1]


def main():
    print(f"Conectando ao FTP: {FTP_HOST}")
    ftp = ftplib.FTP(FTP_HOST)
    ftp.login("anonymous", "anonymous")

    print(f"Entrando em: {FTP_DIR_BASE}")
    ftp.cwd(FTP_DIR_BASE)

    print("\nConteúdo do diretório base:")
    conteudo_base = listar_conteudo(ftp)
    for item in conteudo_base[:50]:
        print(" -", item)

    if GRUPO not in [os.path.basename(x) for x in conteudo_base]:
        print(f"\nDiretório {GRUPO} não apareceu explicitamente no nlst().")
        print("Tentando entrar diretamente nele...")

    caminho_grupo = f"{FTP_DIR_BASE}/{GRUPO}"
    print(f"\nEntrando em: {caminho_grupo}")
    ftp.cwd(caminho_grupo)

    print("\nListando arquivos .dbc do grupo ST...")
    arquivos_dbc = listar_arquivos_dbc(ftp)
    print(f"Total de arquivos .dbc encontrados em {GRUPO}: {len(arquivos_dbc)}")

    print("\nÚltimos 10 arquivos encontrados:")
    for a in arquivos_dbc[-10:]:
        print(" -", a)

    arquivo_escolhido = escolher_arquivo(arquivos_dbc)
    print(f"\nArquivo escolhido para inspeção: {arquivo_escolhido}")

    with tempfile.TemporaryDirectory() as temp_dir:
        caminho_dbc = os.path.join(temp_dir, arquivo_escolhido)
        caminho_dbf = os.path.join(temp_dir, arquivo_escolhido.replace(".dbc", ".dbf"))

        print("\nBaixando arquivo...")
        with open(caminho_dbc, "wb") as f:
            ftp.retrbinary(f"RETR {arquivo_escolhido}", f.write)

        print("Convertendo DBC -> DBF...")
        dbc2dbf(caminho_dbc, caminho_dbf)

        print("Lendo DBF...")
        tabela = DBF(caminho_dbf, encoding="iso-8859-1", load=True)

        print(f"\nTotal de registros no DBF: {len(tabela):,}")

        print("\nColunas encontradas:")
        for campo in tabela.field_names:
            print(" -", campo)

        print("\nPrimeiros 3 registros:")
        for i, row in enumerate(tabela):
            if i >= 3:
                break
            print(f"\nRegistro {i + 1}:")
            for k, v in row.items():
                print(f"  {k}: {v}")

    ftp.quit()
    print("\nInspeção concluída.")


if __name__ == "__main__":
    main()

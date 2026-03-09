from pathlib import Path
import yaml
import pyarrow.parquet as pq

PARQUET_PATH = Path("datalake/sinan/meningite_br.parquet")
METADATA_PATH = Path("metadados/sinan_meningite_metadata.yaml")


def carregar_parquet():
    return pq.read_table(PARQUET_PATH)


def carregar_metadata():
    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def main():
    table = carregar_parquet()
    metadata = carregar_metadata()

    print(f"Parquet carregado com {table.num_rows} linhas e {len(table.schema.names)} colunas.")
    print(f"Metadata carregado com {len(metadata.get('variables', {}))} variáveis internas.")
    print(f"Variáveis externas declaradas: {len(metadata.get('external_lookup_variables', []))}")


if __name__ == "__main__":
    main()

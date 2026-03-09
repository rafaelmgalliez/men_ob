from pathlib import Path
import yaml
import pyarrow.parquet as pq

PARQUET_PATH = Path("datalake/sinan/meningite_br.parquet")
METADATA_PATH = Path("metadados/sinan_meningite_metadata.yaml")


def main():
    table = pq.read_table(PARQUET_PATH)

    with open(METADATA_PATH, "r", encoding="utf-8") as f:
        metadata = yaml.safe_load(f)

    parquet_cols = set(table.schema.names)
    yaml_cols = set(metadata.get("variables", {}).keys())
    external_lookup = set(metadata.get("external_lookup_variables", []))

    yaml_internal_cols = yaml_cols | external_lookup

    somente_parquet = sorted(parquet_cols - yaml_internal_cols)
    somente_yaml = sorted(yaml_internal_cols - parquet_cols)
    em_comum = sorted(parquet_cols & yaml_internal_cols)

    print(f"Colunas no parquet: {len(parquet_cols)}")
    print(f"Variáveis descritas (internas + externas declaradas): {len(yaml_internal_cols)}")
    print(f"Em comum: {len(em_comum)}")
    print(f"Só no parquet: {len(somente_parquet)}")
    print(f"Só no yaml/declaradas: {len(somente_yaml)}")

    if somente_parquet:
        print("\nVariáveis no parquet sem metadata/declaracao:")
        for c in somente_parquet:
            print(f" - {c}")

    if somente_yaml:
        print("\nVariáveis declaradas no yaml mas ausentes no parquet:")
        for c in somente_yaml:
            print(f" - {c}")


if __name__ == "__main__":
    main()

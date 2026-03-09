# src/gerar_lookup_ibge_municipios.py
from __future__ import annotations

import tempfile
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests

# ---------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------

MALHA_URL = (
    "https://geoftp.ibge.gov.br/organizacao_do_territorio/"
    "malhas_territoriais/malhas_municipais/municipio_2024/Brasil/"
    "BR_Municipios_2024.zip"
)

LOCALIDADES_URL = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

OUTPUT_DIR = Path("lookup_tables")
OUTPUT_FILE = OUTPUT_DIR / "ibge_municipios_espacial.parquet"

UF_MAP = {
    "11": ("RO", "Rondônia"),
    "12": ("AC", "Acre"),
    "13": ("AM", "Amazonas"),
    "14": ("RR", "Roraima"),
    "15": ("PA", "Pará"),
    "16": ("AP", "Amapá"),
    "17": ("TO", "Tocantins"),
    "21": ("MA", "Maranhão"),
    "22": ("PI", "Piauí"),
    "23": ("CE", "Ceará"),
    "24": ("RN", "Rio Grande do Norte"),
    "25": ("PB", "Paraíba"),
    "26": ("PE", "Pernambuco"),
    "27": ("AL", "Alagoas"),
    "28": ("SE", "Sergipe"),
    "29": ("BA", "Bahia"),
    "31": ("MG", "Minas Gerais"),
    "32": ("ES", "Espírito Santo"),
    "33": ("RJ", "Rio de Janeiro"),
    "35": ("SP", "São Paulo"),
    "41": ("PR", "Paraná"),
    "42": ("SC", "Santa Catarina"),
    "43": ("RS", "Rio Grande do Sul"),
    "50": ("MS", "Mato Grosso do Sul"),
    "51": ("MT", "Mato Grosso"),
    "52": ("GO", "Goiás"),
    "53": ("DF", "Distrito Federal"),
}


def baixar_arquivo(url: str, destino: Path) -> None:
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    destino.write_bytes(r.content)


def carregar_ids_municipios_api() -> pd.DataFrame:
    r = requests.get(LOCALIDADES_URL, timeout=120)
    r.raise_for_status()
    data = r.json()

    rows = []
    for item in data:
        rows.append(
            {
                "municipio_codigo_7": str(item["id"]),
                "municipio_nome_api": item["nome"],
            }
        )

    df = pd.DataFrame(rows)
    df["municipio_codigo_7"] = df["municipio_codigo_7"].astype(str)
    return df


def carregar_malha_municipal(zip_path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(zip_path)

    cols = {c.upper(): c for c in gdf.columns}

    required = ["CD_MUN", "NM_MUN", "SIGLA_UF"]
    missing = [c for c in required if c not in cols]
    if missing:
        raise ValueError(
            f"Colunas esperadas não encontradas na malha do IBGE: {missing}. "
            f"Colunas disponíveis: {list(gdf.columns)}"
        )

    out = gdf.rename(
        columns={
            cols["CD_MUN"]: "municipio_codigo_7",
            cols["NM_MUN"]: "municipio_nome",
            cols["SIGLA_UF"]: "uf_sigla_malha",
        }
    ).copy()

    out["municipio_codigo_7"] = out["municipio_codigo_7"].astype(str)

    # Mantém apenas geocódigos com 7 dígitos.
    out = out[out["municipio_codigo_7"].str.match(r"^\d{7}$")].copy()

    if "AREA_KM2" in cols:
        out = out.rename(columns={cols["AREA_KM2"]: "area_km2"})

    return out


def calcular_centroides(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    # Projetar para CRS métrico antes de calcular centróides.
    gdf_proj = gdf.to_crs(5880)
    cent_proj = gdf_proj.geometry.centroid
    cent_wgs84 = gpd.GeoSeries(cent_proj, crs=5880).to_crs(4326)

    return pd.DataFrame(
        {
            "municipio_codigo_7": gdf["municipio_codigo_7"].astype(str).values,
            "centroide_lon": cent_wgs84.x.values,
            "centroide_lat": cent_wgs84.y.values,
        }
    )


def enriquecer_codigos(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf["municipio_codigo_6"] = gdf["municipio_codigo_7"].str[:-1]
    gdf["uf_codigo"] = gdf["municipio_codigo_7"].str[:2]

    gdf["uf_sigla"] = gdf["uf_codigo"].map(lambda x: UF_MAP.get(x, (None, None))[0])
    gdf["uf_nome"] = gdf["uf_codigo"].map(lambda x: UF_MAP.get(x, (None, None))[1])

    # Prioriza a sigla vinda da malha, quando disponível.
    gdf["uf_sigla"] = gdf["uf_sigla_malha"].fillna(gdf["uf_sigla"])

    return gdf


def gerar_lookup_ibge_municipios() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("1/5 - Baixando lista oficial de municípios via API do IBGE...")
    municipios_api = carregar_ids_municipios_api()

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "BR_Municipios_2024.zip"

        print("2/5 - Baixando malha municipal oficial do IBGE...")
        baixar_arquivo(MALHA_URL, zip_path)

        print("3/5 - Lendo malha municipal...")
        malha = carregar_malha_municipal(zip_path)

    print("4/5 - Filtrando municípios oficiais, calculando centroides e enriquecendo códigos...")
    # Mantém apenas os municípios administrativos oficiais.
    malha = malha.merge(
        municipios_api,
        on="municipio_codigo_7",
        how="inner",
    )

    malha = enriquecer_codigos(malha)
    centroides = calcular_centroides(malha)

    final = (
        malha.drop(columns="geometry")
        .merge(centroides, on="municipio_codigo_7", how="left")
    )

    if "municipio_nome_api" in final.columns:
        final["municipio_nome"] = final["municipio_nome_api"].fillna(final["municipio_nome"])

    keep_cols = [
        "municipio_codigo_7",
        "municipio_codigo_6",
        "municipio_nome",
        "uf_codigo",
        "uf_sigla",
        "uf_nome",
        "centroide_lon",
        "centroide_lat",
    ]

    if "area_km2" in final.columns:
        keep_cols.append("area_km2")

    final = final[keep_cols].sort_values(["uf_sigla", "municipio_nome"]).reset_index(drop=True)

    print("5/5 - Salvando parquet...")
    final.to_parquet(OUTPUT_FILE, index=False)

    print(f"Arquivo salvo em: {OUTPUT_FILE}")
    print(f"Total de linhas: {len(final)}")
    print(final.head())


if __name__ == "__main__":
    gerar_lookup_ibge_municipios()

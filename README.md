# 🧠 Men_Ob: Observatório de Meningites (Brasil)

O **Men_Ob** é um projeto de construção de infraestrutura analítica para vigilância epidemiológica de meningites no Brasil, com foco inicial no banco nacional do **SINAN**.

A arquitetura do projeto prioriza:

- reprodutibilidade
- simplicidade estrutural
- separação entre dados brutos, metadados e tabelas auxiliares
- uso de formatos colunares eficientes (Parquet)

---

# ✅ Estado Atual do Projeto

## 1. Extração e consolidação do SINAN

Concluído:

- Download automatizado dos arquivos `.dbc` do DATASUS
- Conversão de `.dbc` para `.dbf`
- Leitura vetorizada com `PyArrow`
- Consolidação em arquivo único `Parquet`

Arquivo principal gerado:

datalake/sinan/meningite_br.parquet

Situação atual:

427.152 registros  
131 variáveis

---

# 2. Reconstrução do dicionário de dados

Concluído:

- Reconstrução semântica do dicionário do SINAN meningite
- Separação entre variáveis internas e variáveis com lookup externo
- Estruturação em arquivo YAML
- Validação completa contra o schema real do Parquet

Arquivo de metadados:

metadados/sinan_meningite_metadata.yaml

Validação realizada:

119 variáveis internas  
12 variáveis externas declaradas  
131 variáveis cobertas  
0 variáveis faltantes  
0 variáveis excedentes  

---

# 3. Lookup territorial municipal (IBGE)

Concluído:

- Geração de tabela espacial municipal a partir da malha do IBGE
- Cálculo de centroides municipais
- Geração de códigos compatíveis com IBGE e DATASUS
- Salvamento em Parquet

Arquivo gerado:

lookup_tables/ibge_municipios_espacial.parquet

Conteúdo principal:

municipio_codigo_7  
municipio_codigo_6  
municipio_nome  
uf_codigo  
uf_sigla  
uf_nome  
centroide_lon  
centroide_lat  
area_km2  

---

# 4. Validação da cobertura territorial do SINAN

Concluído:

Validação do join entre o banco de meningite e o lookup territorial municipal.

Campos analisados:

ID_MUNICIP — município de notificação  
ID_MN_RESI — município de residência  
ATE_MUNICI — município do hospital  

Resultado sintético:

Município de notificação  
- cobertura completa  
- nenhum código inválido  

Município de residência  
- cobertura quase completa  
- presença marginal de códigos agregados por UF  

Município do hospital  
- campo inconsistente  
- presença de códigos inválidos e truncados  
- não adequado como eixo espacial primário  

---

# 5. Investigação estrutural do CNES

Foi realizada uma análise sistemática da estrutura dos microdados do **Cadastro Nacional de Estabelecimentos de Saúde (CNES)** disponíveis no FTP do DATASUS.

Objetivo:

avaliar a possibilidade de espacialização das unidades notificadoras.

Resultado da investigação:

- não existem coordenadas geográficas das unidades
- existe o campo COD_CEP associado ao estabelecimento
- existe identificação municipal (CODUFMUN)
- não existe endereço estruturado completo

Conclusão:

os microdados públicos do CNES **não permitem geolocalização direta das unidades de saúde**.

Scripts utilizados nessa investigação encontram-se em:

src/diagnostico/

Resultados intermediários foram registrados em:

diagnosticos/

---

# 6. Construção do lookup de unidades CNES

Foi construído um lookup contendo apenas os estabelecimentos CNES presentes na base de meningite.

Processo:

1. extração dos CNES distintos presentes no SINAN  
2. download do cadastro nacional CNES (grupo ST)  
3. empilhamento nacional das bases estaduais  
4. filtragem apenas das unidades presentes no SINAN  
5. consolidação em uma linha por CNES  

Arquivo gerado:

lookup_tables/cnes_meningite_lookup.parquet

Situação atual:

10.002 CNES distintos no SINAN  
8.622 unidades encontradas no cadastro CNES  
cobertura de 86.2%

Campos principais:

cnes_codigo  
cod_cep  
municipio_codigo_6  
uf_codigo  
tp_unid  
tpgestao  
esfera_a  
nat_jur  

---

# 7. Espacialização das unidades CNES

Após avaliação da qualidade do campo COD_CEP foi implementado um processo de espacialização aproximada das unidades.

Hierarquia de resolução espacial:

1. geocodificação do CEP da unidade  
2. fallback para centroide municipal  
3. fallback para centroide da UF  

Artefato gerado:

lookup_tables/cnes_meningite_spatial.parquet

Conteúdo principal:

cnes_codigo  
latitude  
longitude  
spatial_resolution_final  
spatial_source  

Resoluções possíveis:

cep  
municipio  
uf  

---

# 8. Construção do dataset epidemiológico espacial integrado

Foi gerado o dataset analítico final do projeto integrando:

SINAN + IBGE + CNES

Arquivo gerado:

datalake/sinan/meningite_spatial.parquet

Este dataset contém:

- todas as variáveis epidemiológicas do SINAN
- enriquecimento territorial municipal
- localização aproximada das unidades notificadoras
- metadados de proveniência

Camadas espaciais presentes:

Residência do caso  
Município de notificação  
Unidade notificadora (CNES)

---

# ⚠️ Limitações metodológicas

A espacialização das unidades CNES é aproximada porque:

- CEP representa área postal e não coordenada exata
- fallback municipal utiliza centroides geométricos
- mudanças históricas da divisão territorial ainda não foram harmonizadas

Portanto as coordenadas devem ser interpretadas como **localização aproximada das unidades notificadoras**.

---

# 📁 Estrutura atual do projeto

Men_Ob/

app/  
main.py  

datalake/  
sinan/  
meningite_br.parquet  
meningite_spatial.parquet  
sim/  
sih/  

lookup_tables/  
ibge_municipios_espacial.parquet  
cnes_meningite_lookup.parquet  
cnes_meningite_spatial.parquet  

metadados/  
sinan_meningite_metadata.yaml  
dicionario.yaml  
dicionario_v5_mapeado.json  
...  

diagnosticos/  
scan_cnes_endereco_detalhe.txt  
scan_cnes_endereco_resumo.csv  
cnes_meningite_diagnostico.txt  

src/

extrator_sinan.py  
carregar_metadata.py  
validar_metadata.py  

gerar_lookup_ibge_municipios.py  
validar_cobertura_espacial_municipios.py  

gerar_lookup_cnes_meningite.py  
espacializar_cnes_meningite.py  
gerar_dataset_meningite_spatial.py  

diagnostico/  
scan_ftp_cnes_endereco.py  
inspecionar_cnes_st_dbc.py  
diagnosticar_cnes_meningite_raw.py  

legacy/  
extrator_dic_v5.py  
extrator_sih.py  
extrator_sim.py  

pyproject.toml  
uv.lock  
README.md  

---

# 🚧 Próximas etapas

## Integração epidemiológica

- integração da base de mortalidade (SIM)
- integração da base hospitalar (SIH)

## Evolução espacial

- harmonização territorial histórica
- avaliação da estabilidade histórica do CNES
- possível melhoria da geocodificação de unidades

## Observatório analítico

- construção do dataset analítico final consolidado
- criação de dashboards epidemiológicos
- análise espaço-temporal das meningites no Brasil

---

# 📌 Situação atual

O projeto já possui:

- banco nacional consolidado do SINAN meningite
- dicionário semântico validado
- lookup territorial municipal
- lookup de unidades CNES
- espacialização aproximada das unidades notificadoras
- dataset epidemiológico espacial integrado

O **Men_Ob** já dispõe de uma infraestrutura de dados reprodutível capaz de sustentar análises epidemiológicas espaciais em escala nacional.

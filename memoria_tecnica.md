# memoria_tecnica.md

# 🧠 Men_Ob — Memória Técnica do Pipeline de Construção do Observatório de Meningites

Este documento registra de forma detalhada a **memória técnica e metodológica do desenvolvimento do projeto Men_Ob**, desde a construção inicial do banco consolidado do SINAN até a geração do dataset espacial integrado.

O objetivo deste documento é:

• registrar as decisões metodológicas  
• documentar a arquitetura de dados  
• preservar o raciocínio analítico do pipeline  
• garantir reprodutibilidade científica  

Este arquivo funciona como um **log estruturado do desenvolvimento do observatório**.

---

# 1. Objetivo do projeto

O **Men_Ob (Observatório de Meningites)** é um projeto de infraestrutura analítica voltado à construção de uma base nacional integrada para análise epidemiológica das meningites no Brasil.

O projeto parte inicialmente da base nacional do **SINAN — Sistema de Informação de Agravos de Notificação**, com planos de integração futura com:

• SIM — Sistema de Informação de Mortalidade  
• SIH — Sistema de Informação Hospitalar  

O objetivo final é permitir:

• análises epidemiológicas nacionais  
• análises espaço-temporais  
• estudos da rede assistencial  
• investigação de trajetórias assistenciais  

---

# 2. Filosofia de arquitetura de dados

O projeto foi estruturado seguindo alguns princípios fundamentais:

### Reprodutibilidade

Todo o pipeline é baseado em scripts versionados.

### Simplicidade estrutural

Evitar arquiteturas excessivamente complexas.

### Separação de camadas

Os dados foram organizados em três camadas principais:

1️⃣ dados epidemiológicos brutos consolidados  
2️⃣ tabelas auxiliares (lookup tables)  
3️⃣ datasets analíticos derivados  

### Uso de formatos colunares

Os datasets são armazenados em **Parquet**, permitindo:

• leitura vetorizada  
• compressão eficiente  
• integração com Python, R e Julia  

---

# 3. Construção da base nacional do SINAN

## 3.1 Download dos microdados

Os dados do SINAN foram obtidos diretamente do FTP do DATASUS.

Arquivos disponibilizados:

• formato `.dbc`

Esses arquivos representam versões compactadas do formato DBF.

---

## 3.2 Conversão dos dados

Pipeline implementado:

DBC → DBF → DataFrame → Parquet

Script principal:

src/extrator_sinan.py

Tecnologias utilizadas:

• PyArrow  
• pandas  

---

## 3.3 Consolidação nacional

Os arquivos estaduais foram empilhados em um único dataset nacional.

Artefato gerado:

datalake/sinan/meningite_br.parquet

Características do dataset:

Registros: 427.152  
Variáveis: 131  

Esse arquivo passou a representar a **camada epidemiológica consolidada do projeto**.

---

# 4. Reconstrução do dicionário de dados

O SINAN possui documentação extensa, porém pouco estruturada para uso programático.

Foi realizado um processo de reconstrução semântica do dicionário.

Arquivos utilizados:

metadados/DIC_DADOS_Meningite_v5.pdf  
metadados/Meningite_v5_instr.pdf  

---

## 4.1 Estruturação em YAML

O dicionário foi transformado em um arquivo estruturado:

metadados/sinan_meningite_metadata.yaml

Esse arquivo contém:

• definição das variáveis  
• descrição semântica  
• classificação das variáveis  
• identificação das variáveis externas  

---

## 4.2 Validação do metadata

Script:

src/validar_metadata.py

Resultado da validação:

Variáveis internas: 119  
Variáveis externas: 12  
Total coberto: 131  

Nenhuma inconsistência detectada.

Essa etapa consolidou a camada semântica do projeto.

---

# 5. Construção do lookup territorial municipal

A espacialização inicial do projeto foi baseada em municípios.

Fonte:

malha municipal do IBGE.

---

## 5.1 Geração da tabela municipal

Script:

src/gerar_lookup_ibge_municipios.py

Processos realizados:

• leitura da malha municipal  
• cálculo do centroide geométrico  
• geração de códigos compatíveis com DATASUS  

Artefato gerado:

lookup_tables/ibge_municipios_espacial.parquet

---

## 5.2 Estrutura do lookup municipal

Campos principais:

municipio_codigo_7  
municipio_codigo_6  
municipio_nome  
uf_codigo  
uf_sigla  
uf_nome  
centroide_lon  
centroide_lat  
area_km2  

Esse objeto passou a ser a base territorial do pipeline.

---

# 6. Validação territorial do SINAN

Script:

src/validar_cobertura_espacial_municipios.py

Foram testados três campos territoriais do SINAN:

ID_MUNICIP  
ID_MN_RESI  
ATE_MUNICI  

---

## 6.1 Resultados

Município de notificação

• cobertura completa  
• nenhum código inválido  

Município de residência

• cobertura quase completa  
• poucos códigos agregados  

Município do hospital

• inconsistências frequentes  
• códigos truncados  
• baixa confiabilidade  

Conclusão metodológica:

ATE_MUNICI não deve ser utilizado como eixo territorial principal.

---

# 7. Investigação estrutural do CNES

Para investigar a espacialização das unidades notificadoras foi necessário estudar a estrutura dos microdados do CNES.

Grupos investigados:

DC  
EE  
EF  
EP  
EQ  
GM  
HB  
IN  
LT  
PF  
RC  
SR  
ST  

Scripts desenvolvidos:

src/diagnostico/scan_ftp_cnes_endereco.py  
src/diagnostico/inspecionar_cnes_st_dbc.py  

---

## 7.1 Resultado da investigação

Constatações principais:

• não existem coordenadas geográficas das unidades nos microdados públicos  
• existe o campo COD_CEP  
• existe o campo CODUFMUN  
• não existe endereço estruturado completo  

Conclusão:

O CNES público **não permite geolocalização direta das unidades**.

Essa constatação redefiniu a estratégia espacial da camada institucional.

---

# 8. Construção do lookup CNES específico da meningite

Estratégia adotada:

extrair apenas os estabelecimentos que aparecem na base do SINAN meningite.

Essa estratégia reversa reduz complexidade e mantém o foco na rede institucional relevante para a coorte.

---

## 8.1 Processo

1. extração dos CNES do SINAN  
2. download do cadastro nacional CNES (grupo ST)  
3. empilhamento nacional das bases estaduais  
4. filtragem apenas das unidades presentes no SINAN  
5. consolidação em uma linha por CNES  

Script:

src/gerar_lookup_cnes_meningite.py

---

## 8.2 Resultado

CNES distintos no SINAN:

10002

CNES encontrados no cadastro CNES:

8622

Cobertura:

86,2%

Artefato gerado:

lookup_tables/cnes_meningite_lookup.parquet

---

# 9. Diagnóstico inicial da qualidade do CEP

Foi realizada análise específica do campo COD_CEP.

Resultado:

CEP preenchido: 100%  
CEP válido (8 dígitos): 100%  

Total de CEPs únicos:

6815

CEPs compartilhados por múltiplas unidades:

1161

Conclusão inicial:

o CEP parecia ter qualidade suficiente para uma geocodificação aproximada.

Essa conclusão motivou a construção da primeira versão do lookup espacial do CNES.

---

# 10. Primeira espacialização das unidades CNES

Script:

src/espacializar_cnes_meningite.py

Estratégia implementada:

1️⃣ geocodificação por CEP  
2️⃣ fallback para município  
3️⃣ fallback para UF  

---

# 11. Resultado da primeira espacialização

ESPACIALIZAÇÃO DO CNES DA MENINGITE
======================================================================

Lookup de entrada:
lookup_tables/cnes_meningite_lookup.parquet

Lookup municipal:
lookup_tables/ibge_municipios_espacial.parquet

Lookup espacial gerado:
lookup_tables/cnes_meningite_spatial.parquet

Cobertura final:

Total de CNES: 8622

Resolvidos por CEP: 8622 (100.0%)

Resolvidos por município: 0 (0.0%)

Resolvidos por UF: 0 (0.0%)

Sem localização final: 0 (0.0%)

Artefatos produzidos:

lookup_tables/cnes_meningite_spatial.parquet  
diagnosticos/cnes_meningite_spatial_diagnostico.txt  
diagnosticos/cnes_meningite_spatial_amostra.csv  

Num primeiro momento, esse resultado parecia excelente. No entanto, ainda faltava uma etapa crítica: a validação de plausibilidade geográfica.

---

# 12. Validação da plausibilidade espacial do CNES

Foi então implementada uma etapa específica de auditoria espacial.

Script:

src/diagnostico/validar_cnes_meningite_spatial.py

Objetivo:

comparar a coordenada obtida por CEP com o centroide do município esperado da unidade.

Resultado:

Total de CNES: 8622

Distância entre coordenada final e município esperado:

> 25 km: 4239 (49,16%)  
> 50 km: 3891 (45,13%)  
> 100 km: 3668 (42,54%)  
> 250 km: 3461 (40,14%)

Também foram detectadas coordenadas muito repetidas, indicando possíveis respostas genéricas do geocoder.

Conclusão metodológica:

a primeira espacialização por CEP, apesar de formalmente completa, **não era suficientemente confiável como camada espacial final**.

Esse foi um ponto decisivo do pipeline.

---

# 13. Reconstrução conservadora do lookup espacial do CNES

Diante do diagnóstico de plausibilidade, foi desenvolvida uma nova estratégia.

Script:

src/reconstruir_cnes_meningite_spatial_conservador.py

Regra aplicada:

• manter coordenada por CEP apenas se a distância ao município esperado fosse ≤ 25 km  
• caso contrário, rebaixar para centroide municipal  
• usar UF apenas se município estivesse ausente  

---

## 13.1 Resultado da reconstrução conservadora

Arquivo de entrada:

lookup_tables/cnes_meningite_spatial.parquet

Arquivo de saída:

lookup_tables/cnes_meningite_spatial_conservador.parquet

Resultado:

Total de CNES: 8622

Resolução final = cep: 4383 (50,84%)  
Resolução final = municipio: 4239 (49,16%)  
Resolução final = uf: 0 (0,0%)  
Sem localização final: 0 (0,0%)

Regras aplicadas:

CEP mantido: 4383 (50,84%)  
Fallback para município: 4239 (49,16%)  
Fallback para UF: 0 (0,0%)

Essa etapa passou a representar a **versão espacial metodologicamente confiável da camada CNES**.

---

# 14. Construção do dataset espacial integrado

Script:

src/gerar_dataset_meningite_spatial.py

Integração realizada:

SINAN  
IBGE  
CNES  

Na versão final do pipeline, esse script passou a utilizar a versão conservadora da espacialização do CNES.

---

## 14.1 Artefato final

datalake/sinan/meningite_spatial.parquet

Validação do dataset final:

Linhas no dataset final: 427.152  
Colunas no dataset final: 177  

Casos com geografia de residência: 426.887  
Casos com geografia de notificação: 427.152  
Casos com geografia da unidade CNES: 400.754  

Esse arquivo passou a representar o **dataset analítico espacial consolidado do projeto**.

---

# 15. Arquitetura final do pipeline

Fluxo consolidado:

SINAN DBC  
↓  
extrator_sinan.py  
↓  
meningite_br.parquet  
↓  
lookup municipal IBGE  
↓  
lookup CNES meningite  
↓  
primeira espacialização por CEP  
↓  
validação de plausibilidade espacial  
↓  
reconstrução conservadora  
↓  
dataset final integrado  
↓  
meningite_spatial.parquet

---

# 16. Estrutura atual do repositório

Men_Ob/

app/

datalake/

lookup_tables/

metadados/

diagnosticos/

src/

legacy/

README.md

memoria_tecnica.md

pyproject.toml

uv.lock

.gitignore

---

# 17. Situação atual do projeto

O projeto possui atualmente:

✔ base nacional consolidada do SINAN  
✔ dicionário de dados estruturado  
✔ lookup territorial municipal  
✔ lookup CNES específico da meningite  
✔ espacialização conservadora das unidades notificadoras  
✔ dataset epidemiológico espacial integrado  

---

# 18. Próximas etapas

Integração futura:

SIM  
SIH  

Evoluções metodológicas:

• harmonização territorial histórica  
• análise espaço-temporal  
• análise de trajetórias assistenciais  
• construção do observatório analítico  

---

# 19. Status científico do pipeline

O projeto já possui uma infraestrutura de dados reprodutível capaz de sustentar análises epidemiológicas espaciais nacionais das meningites no Brasil.

Além disso, a fase de engenharia de dados pode ser considerada concluída nesta etapa, com abertura da fase seguinte de exploração epidemiológica do dataset integrado.

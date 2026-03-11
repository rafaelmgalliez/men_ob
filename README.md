# README.md

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
- Conversão dos arquivos `.dbc` para `.dbf`
- Leitura vetorizada dos dados
- Consolidação em arquivo único Parquet

Script principal:

src/extrator_sinan.py

Artefato gerado:

datalake/sinan/meningite_br.parquet

Situação atual:

- 427.152 registros
- 131 variáveis

Esse arquivo representa a **base epidemiológica nacional consolidada** do projeto.

---

## 2. Reconstrução e validação do dicionário de dados

Foi realizado um processo de reconstrução semântica do dicionário do SINAN meningite, a partir dos documentos de referência da ficha e do manual.

Objetivos dessa etapa:

- identificar o significado epidemiológico das variáveis
- separar variáveis internas e variáveis com dependência externa
- estruturar o dicionário em formato programaticamente utilizável
- validar o metadata contra o schema real do Parquet

Arquivos principais:

metadados/sinan_meningite_metadata.yaml  
metadados/dicionario_v5_mapeado.json  

Scripts principais:

src/carregar_metadata.py  
src/validar_metadata.py  

Resultado da validação:

- 119 variáveis internas
- 12 variáveis externas declaradas
- 131 variáveis cobertas
- 0 variáveis faltantes
- 0 variáveis excedentes

Essa etapa estabeleceu a **camada semântica canônica** do projeto.

---

## 3. Lookup territorial municipal

A primeira camada espacial do projeto foi construída em escala municipal, utilizando a malha territorial do IBGE.

Objetivos:

- obter uma tabela territorial estável
- gerar centroides municipais
- compatibilizar códigos IBGE e DATASUS
- permitir integração com os campos municipais do SINAN

Script principal:

src/gerar_lookup_ibge_municipios.py

Artefato gerado:

lookup_tables/ibge_municipios_espacial.parquet

Conteúdo principal:

- municipio_codigo_7
- municipio_codigo_6
- municipio_nome
- uf_codigo
- uf_sigla
- uf_nome
- centroide_lon
- centroide_lat
- area_km2

Esse objeto representa o **lookup territorial municipal canônico** do projeto.

---

## 4. Validação da cobertura territorial do SINAN

Foi realizada validação explícita da cobertura territorial dos principais campos geográficos do banco de meningite.

Script:

src/validar_cobertura_espacial_municipios.py

Campos avaliados:

- ID_MUNICIP — município de notificação
- ID_MN_RESI — município de residência
- ATE_MUNICI — município do hospital

Resultado sintético:

### Município de notificação
- cobertura completa
- nenhum código sem correspondência

### Município de residência
- cobertura quase completa
- pequena presença de códigos especiais ou agregados por UF

### Município do hospital
- campo inconsistente
- presença de códigos inválidos, truncados e agregados
- inadequado como eixo espacial principal

Conclusão metodológica:

- o eixo territorial principal do projeto é o município de residência
- o município de notificação permanece como camada complementar
- o município do hospital é tratado como campo auxiliar

---

# 🌍 Estratégia geral de espacialização

A espacialização do projeto foi organizada em duas camadas:

## Camada 1 — território epidemiológico do caso

Baseada em:

- município de residência
- município de notificação

Campos prioritários:

ID_MN_RESI  
ID_MUNICIP  

## Camada 2 — localização aproximada da unidade notificadora

Baseada na unidade CNES associada ao caso.

Essa camada exigiu investigação específica, pois os microdados públicos do CNES não fornecem coordenadas diretas das unidades.

---

# 🧪 Investigação estrutural do CNES

Foi realizada uma investigação sistemática da estrutura dos microdados públicos do **Cadastro Nacional de Estabelecimentos de Saúde (CNES)** disponíveis no FTP do DATASUS.

Objetivo:

avaliar se seria possível geolocalizar diretamente as unidades notificadoras presentes no SINAN meningite.

Grupos inspecionados:

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

Scripts utilizados:

src/diagnostico/scan_ftp_cnes_endereco.py  
src/diagnostico/inspecionar_cnes_st_dbc.py  

Resultados principais:

- não existem campos de latitude/longitude das unidades nos microdados públicos
- existe o campo COD_CEP
- existe o campo CODUFMUN
- não existe endereço estruturado completo (logradouro + número + bairro)

Conclusão:

os microdados públicos do CNES **não permitem geolocalização direta das unidades de saúde**.

Essa conclusão levou à adoção de uma estratégia de espacialização indireta.

---

# 🏥 Construção do lookup CNES específico da meningite

Em vez de trabalhar com todo o universo do CNES, foi adotada uma estratégia reversa:

1. extrair os códigos CNES presentes no banco nacional de meningite
2. buscar apenas essas unidades no cadastro CNES
3. construir um lookup específico para a coorte de meningite

Script:

src/gerar_lookup_cnes_meningite.py

Artefato gerado:

lookup_tables/cnes_meningite_lookup.parquet

Resultado:

- 10.002 CNES distintos presentes no SINAN
- 8.622 unidades encontradas no cadastro CNES
- cobertura de 86,2%

Campos principais do lookup:

- cnes_codigo
- cod_cep
- municipio_codigo_6
- uf_codigo
- tp_unid
- tpgestao
- esfera_a
- nat_jur

Esse arquivo passou a representar o **lookup institucional das unidades notificadoras da meningite**.

---

# 📍 Primeira espacialização do CNES

Uma vez construído o lookup específico da meningite, foi testada uma estratégia de espacialização hierárquica baseada em:

1. geocodificação do CEP da unidade
2. fallback para centroide municipal
3. fallback para centroide da UF

Script:

src/espacializar_cnes_meningite.py

Artefato gerado:

lookup_tables/cnes_meningite_spatial.parquet

Na primeira execução, todas as 8.622 unidades foram resolvidas formalmente por CEP.

No entanto, essa etapa ainda precisava de validação de plausibilidade geográfica.

---

# 🔎 Validação da plausibilidade espacial do CNES

Foi então realizada uma validação específica do lookup espacial do CNES.

Script:

src/diagnostico/validar_cnes_meningite_spatial.py

A lógica da validação foi comparar a coordenada obtida pela geocodificação do CEP com o centroide do município esperado da unidade.

Resultado:

- Total de CNES: 8.622
- Resolução final por CEP na versão original: 100%
- Distância > 25 km ao município esperado: 49,16%
- Distância > 50 km: 45,13%
- Distância > 100 km: 42,54%
- Distância > 250 km: 40,14%

Além disso foram observadas coordenadas altamente repetidas, indicando respostas genéricas ou colapso espacial do geocoder.

Conclusão metodológica:

a geocodificação direta por CEP, apesar de formalmente completa, **não era suficientemente confiável para ser usada como camada espacial final da unidade**.

---

# 🛡️ Reconstrução conservadora da espacialização do CNES

Diante da inconsistência geográfica da primeira versão, foi implementada uma reconstrução conservadora da espacialização das unidades.

Script:

src/reconstruir_cnes_meningite_spatial_conservador.py

Regra aplicada:

1. manter coordenada por CEP apenas quando a distância ao centroide do município esperado fosse ≤ 25 km
2. caso contrário, rebaixar para centroide do município
3. usar fallback para UF apenas quando necessário

Artefato gerado:

lookup_tables/cnes_meningite_spatial_conservador.parquet

Resultado:

- Total de CNES: 8.622
- CEP mantido: 4.383 (50,84%)
- Fallback para município: 4.239 (49,16%)
- Fallback para UF: 0
- Sem localização final: 0

Essa versão passa a ser a **camada espacial canônica das unidades notificadoras**.

---

# 🧬 Construção do dataset epidemiológico espacial integrado

Com a consolidação das camadas espaciais municipal e institucional, foi gerado o dataset epidemiológico espacial final do projeto.

Script:

src/gerar_dataset_meningite_spatial.py

Integração realizada:

SINAN + IBGE + CNES

Artefato gerado:

datalake/sinan/meningite_spatial.parquet

Resultado da validação do dataset final:

- 427.152 linhas
- 177 colunas
- 426.887 casos com geografia de residência
- 427.152 casos com geografia de notificação
- 400.754 casos com geografia da unidade CNES

Esse arquivo passa a ser o **principal objeto analítico do observatório**.

---

# ⚠️ Limitações metodológicas atuais

Apesar de o pipeline espacial estar concluído nesta fase, algumas limitações permanecem explícitas:

- a localização da unidade CNES é aproximada
- parte das unidades usa centroide municipal como fallback
- o campo ATE_MUNICI continua problemático
- a geografia utilizada é a divisão territorial municipal vigente
- a harmonização territorial histórica ainda não foi implementada

Portanto, a interpretação espacial deve ser feita em escala:

- municipal
- regional
- rede de notificação aproximada

e não como geolocalização exata de unidade em escala intraurbana fina.

---

# 🗂️ Arquitetura de dados do projeto

A arquitetura atual do Men_Ob está organizada em três camadas principais.

## 1. Dados epidemiológicos consolidados

Exemplo:

datalake/sinan/meningite_br.parquet

## 2. Lookups e tabelas auxiliares

Exemplos:

lookup_tables/ibge_municipios_espacial.parquet  
lookup_tables/cnes_meningite_lookup.parquet  
lookup_tables/cnes_meningite_spatial_conservador.parquet  

## 3. Dataset analítico integrado

Exemplo:

datalake/sinan/meningite_spatial.parquet

Essa separação permite reprodutibilidade, auditabilidade e atualização modular do pipeline.

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
   cnes_meningite_spatial_conservador.parquet  

metadados/  
   sinan_meningite_metadata.yaml  
   dicionario_v5_mapeado.json  
   Caderno_analises_Meningites.pdf  
   DIC_DADOS_Meningite_v5.pdf  
   Meningite_v5_instr.pdf  
   Meningite_v5.pdf  
   Tutorial_analises_epi_Meningites.pdf  

diagnosticos/  
   scan_cnes_endereco_detalhe.txt  
   scan_cnes_endereco_resumo.csv  
   cnes_meningite_diagnostico.txt  
   cnes_meningite_diagnostico_amostra.csv  
   cnes_meningite_spatial_diagnostico.txt  
   cnes_meningite_spatial_amostra.csv  
   cnes_meningite_spatial_validacao_resumo.txt  
   cnes_meningite_spatial_validacao_amostra.csv  
   cnes_meningite_spatial_validacao_top_coordenadas.csv  
   cnes_meningite_spatial_validacao_top_distancias.csv  
   cnes_meningite_spatial_conservador_resumo.txt  
   cnes_meningite_spatial_conservador_amostra.csv  
   cache_geocode_cep.csv  

src/  
   extrator_sinan.py  
   carregar_metadata.py  
   validar_metadata.py  
   gerar_lookup_ibge_municipios.py  
   validar_cobertura_espacial_municipios.py  
   gerar_lookup_cnes_meningite.py  
   espacializar_cnes_meningite.py  
   reconstruir_cnes_meningite_spatial_conservador.py  
   gerar_dataset_meningite_spatial.py  

   diagnostico/  
      scan_ftp_cnes_endereco.py  
      inspecionar_cnes_st_dbc.py  
      diagnosticar_cnes_meningite_raw.py  
      validar_cnes_meningite_spatial.py  
      gerar_lookup_cnes_minimo.py  
      validar_lookup_cnes_minimo.py  
      inspecionar_lookup_ibge.py  

   legacy/  
      extrator_dic_v5.py  
      extrator_sih.py  
      extrator_sim.py  

pyproject.toml  
uv.lock  
README.md  
.gitignore  

---

# 📦 Disponibilidade dos dados

Os arquivos Parquet presentes neste repositório são artefatos derivados de dados públicos do DATASUS e foram mantidos no repositório com o objetivo de:

- congelar o estado atual do pipeline
- garantir reprodutibilidade
- permitir inspeção imediata dos objetos analíticos

No futuro, especialmente com a integração de SIM e SIH, essa estratégia poderá ser revista.

---

# 🚧 Próximas etapas

## Integração epidemiológica
- integração da base de mortalidade (SIM)
- integração da base hospitalar (SIH)

## Evolução metodológica
- harmonização territorial histórica
- avaliação temporal da estabilidade do CNES
- eventual melhoria da geocodificação institucional

## Observatório analítico
- análises epidemiológicas descritivas
- análise espaço-temporal
- dashboards e visualização
- investigação de trajetórias assistenciais

---

# 📌 Situação atual

O projeto já possui:

- base nacional consolidada do SINAN meningite
- dicionário semântico validado
- lookup territorial municipal
- lookup institucional do CNES
- espacialização conservadora das unidades notificadoras
- dataset epidemiológico espacial integrado

O **Men_Ob** já dispõe de uma infraestrutura de dados reprodutível capaz de sustentar análises epidemiológicas espaciais nacionais das meningites no Brasil.

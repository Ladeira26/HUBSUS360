# HUBSUS360 — ETL do SIH/SUS

Este repositório contém o processo de ETL do projeto acadêmico **HUBSUS360**, desenvolvido para o Challenge da Oracle na FIAP.

O código processa os dados públicos do **Sistema de Informações Hospitalares do SUS (SIH/SUS)**, disponibilizados pelo DATASUS, para o estado de **São Paulo**, no período de **janeiro de 2023 a dezembro de 2025**. As internações são complementadas com referências do CNES, SIGTAP, CID-10 e regiões de saúde.

O resultado é preparado para três usos:

- carga das tabelas no Oracle Database;
- Análise Exploratória de Dados (AED);
- conferência da qualidade e consistência dos dados.

## Período e abrangência

| Item | Definição |
|---|---|
| Estado | São Paulo (SP) |
| Período | Janeiro de 2023 a dezembro de 2025 |
| Competências | 36 meses |
| Base principal | SIH/SUS — arquivos reduzidos de AIH (`RDSP*.dbc`) |
| Unidade principal de análise | Internações hospitalares agregadas por perfil |

Os arquivos públicos de origem não estão incluídos no repositório devido ao tamanho.

## Fontes de dados

- **SIH/SUS (RD):** internações hospitalares, diagnósticos, procedimentos, valores, permanência, óbitos e dados do paciente;
- **CNES (ST):** informações mensais dos estabelecimentos de saúde;
- **CNES (LT):** leitos existentes e leitos SUS por estabelecimento e competência;
- **SIGTAP:** descrições, grupos e complexidade dos procedimentos;
- **CID-10:** nomes oficiais das categorias e subcategorias dos diagnósticos;
- **Domínios do CNES:** descrições dos tipos de estabelecimento e de leito;
- **Estabelecimentos do CNES:** nome dos hospitais, com identificação especial para códigos históricos sem nome no retrato mais recente;
- **Regiões de Saúde:** município, região de saúde e macrorregião.

## Organização das referências

O caminho recomendado é `referencias_hubsus360`, com a seguinte estrutura:

```text
referencias_hubsus360/
├── dados_sih/
│   ├── 2023/
│   ├── 2024/
│   └── 2025/
├── ST/
├── LT/
├── sigtap/
├── cid10/
├── dominios_cnes/
├── regioes_saude/
└── cnes_estabelecimentos/
```

Para cada ano, são esperadas as 12 competências mensais do SIH, ST, LT e SIGTAP. O ETL relaciona as referências mensais à mesma competência da internação.

## Processamento realizado

O arquivo `HUBSUS360_ETL.py` executa, entre outras, as seguintes etapas:

1. localiza e valida todas as referências;
2. lê diretamente os arquivos DBC/DBF do DATASUS;
3. normaliza datas, códigos, valores e campos categóricos;
4. diferencia AIH inicial de AIH de continuidade;
5. classifica as Internações por Condições Sensíveis à Atenção Primária (ICSAP) nos 19 grupos da lista brasileira;
6. acrescenta descrições da CID-10 e do SIGTAP;
7. complementa hospitais, municípios, regiões de saúde e capacidade de leitos com o CNES;
8. calcula os indicadores municipais e hospitalares;
9. gera as tabelas para o Oracle, o arquivo da AED e os relatórios de validação;
10. salva um cache mensal para evitar a releitura dos DBCs nas próximas execuções.

## Tabelas geradas para o Oracle

O ETL gera 13 arquivos CSV em `tabelas_oracle`:

1. `T_GRUPO_ICSAP.csv`
2. `T_DIAGNOSTICO.csv`
3. `T_MUNICIPIO.csv`
4. `T_ESTABELECIMENTO.csv`
5. `T_PROCEDIMENTO.csv`
6. `T_FAIXA_ETARIA.csv`
7. `T_SEXO.csv`
8. `T_RACA_COR.csv`
9. `T_PERFIL_INTERNACAO.csv`
10. `T_RESUMO_AIH.csv`
11. `T_RESUMO_ASSISTENCIAL.csv`
12. `T_CATEGORIA_LEITO.csv`
13. `T_CAPACIDADE_LEITO.csv`

Também são gerados:

- `aed/DATASET_AED_HUBSUS360.csv`: arquivo único para a análise exploratória;
- `validacoes/`: resultados das verificações de qualidade, cobertura e consistência;
- `cache_hubsus360/`: competências já tratadas para reutilização;
- pacote ZIP dos resultados, quando solicitado.

## Indicadores

### Percentual de internações ICSAP

```text
PC_INTERNACOES_ICSAP =
    QT_INTERNACOES_ICSAP / QT_INTERNACOES_TOTAL * 100
```

### IEP municipal mensal

O IEP utilizado no projeto é um indicador experimental, e não um indicador oficial do SUS:

```text
IEP = 100 - (VL_TOTAL_ICSAP / VL_TOTAL_ELEGIVEL_IEP * 100)
```

As categorias CID-10 de parto `O80` a `O84` são retiradas do denominador elegível. Quanto maior o resultado, menor a participação financeira das ICSAP no valor elegível do município e mês.

### Permanência média hospitalar

```text
PERMANENCIA_MEDIA = QT_DIAS_PERMANENCIA / QT_SAIDAS_HOSPITALARES
```

### Percentual de óbitos hospitalares

```text
PC_OBITOS = QT_OBITOS / QT_SAIDAS_HOSPITALARES * 100
```

Nos meses sem saída hospitalar, permanência média e percentual de óbitos permanecem nulos, acompanhados de um marcador que informa que o indicador não pôde ser calculado.

## Instalação

É necessário ter Python instalado. No terminal do projeto, execute:

```bash
python -m pip install pandas numpy openpyxl
```

O ETL possui leitor próprio para os arquivos DBC e não exige uma biblioteca adicional de descompactação.

## Como executar

Processamento completo de 2023 a 2025:

```bash
python HUBSUS360_ETL.py --referencias referencias_hubsus360 --anos 2023 2024 2025
```

Se a pasta estiver com o nome e a localização recomendados, também é possível executar:

```bash
python HUBSUS360_ETL.py
```

Processamento de um ano ou mês específico:

```bash
python HUBSUS360_ETL.py --referencias referencias_hubsus360 --anos 2023
python HUBSUS360_ETL.py --referencias referencias_hubsus360 --anos 2025 --meses 6
```

Para ignorar o cache e reler todas as competências:

```bash
python HUBSUS360_ETL.py --referencias referencias_hubsus360 --refazer-cache
```

Para gerar também o pacote ZIP:

```bash
python HUBSUS360_ETL.py --referencias referencias_hubsus360 --gerar-zip
```

## Pontos de atenção para a AED

- Cada linha de `DATASET_AED_HUBSUS360.csv` representa um **perfil agregado**, não uma AIH nem um paciente individual.
- A quantidade de internações deve ser calculada pela soma de `QT_INTERNACOES`; a contagem de linhas não representa o total de internações.
- `NM_MUNICIPIO` representa o município de residência do paciente, não necessariamente o município do hospital.
- Indicadores municipais repetidos no arquivo devem ser considerados uma única vez por `DT_COMPETENCIA + CD_MUNICIPIO_RESIDENCIA`.
- Indicadores hospitalares e de leitos devem ser considerados uma única vez por `DT_COMPETENCIA + CD_CNES`.
- Não devem ser somados os campos `PC_INTERNACOES_ICSAP_MUNICIPIO_MES`, `IEP_MUNICIPIO_MES`, `QT_SAIDAS_HOSPITAL_MES`, `PERMANENCIA_MEDIA_HOSPITAL_MES`, `PC_OBITOS_HOSPITAL_MES`, `QT_LEITOS_EXISTENTES_TOTAL` e `QT_LEITOS_SUS_TOTAL`.
- Os leitos do CNES representam capacidade cadastrada, não disponibilidade de vagas em tempo real.
- Estabelecimentos históricos ausentes no retrato mais recente do CNES são mantidos com identificação explícita, sem excluir suas internações.

## Validações

Antes de encerrar, o ETL verifica cobertura das competências, chaves, duplicidades, relacionamentos, valores, indicadores, descrições e consistência entre as tabelas. Alertas de referência são preservados nos relatórios, sem apagar registros válidos do SIH/SUS.

## Integrantes

- Guilherme Ladeira Corrêa Santos — RM 571137
- Lucas Araújo Curci — RM 572053
- Lucas Luna Pimentel — RM 573538
- Pedro Henrique Moretti Aguiar — RM 569806
- Lucas Amaral da Silva Barros — RM 571736

# HUBSUS360 — ETL de dados do SIH/SUS

Este repositório contém o processo de ETL do projeto acadêmico **HUBSUS360**, desenvolvido para o Challenge da Oracle na FIAP.

O código lê os arquivos do **SIH/SUS (Sistema de Informações Hospitalares do SUS)**, realiza o tratamento dos registros de internação e gera tabelas menores para análise exploratória, construção do painel e futura carga no Oracle Database.

## Dados necessários

O ETL utiliza os arquivos reduzidos de AIH do SIH/SUS, disponibilizados pelo DATASUS nos formatos `.dbc` ou `.dbf`.

Os arquivos devem seguir o padrão oficial:

```text
RDSP2401.dbc  → São Paulo, janeiro de 2024
RDSP2512.dbc  → São Paulo, dezembro de 2025
```

Os datasets não estão incluídos neste repositório por causa do tamanho. Antes da execução, baixe os arquivos do período desejado e organize-os em uma pasta, podendo separar cada ano em uma subpasta:

```text
dados_sih/
├── 2024/
│   ├── RDSP2401.dbc
│   └── ...
└── 2025/
    ├── RDSP2501.dbc
    └── ...
```

## Instalação

É necessário ter o Python instalado. No terminal, execute:

```bash
python -m pip install pandas numpy dbfread
```

## Como executar

Sem informar parâmetros, o programa abre uma janela para selecionar a pasta dos dados ou arquivos individuais:

```bash
python HUBSUS360_ETL.py
```

Também é possível informar a pasta e o período pelo terminal:

```bash
python HUBSUS360_ETL.py --sih "C:\dados\dados_sih" --anos 2024 2025
```

Para processar apenas um mês:

```bash
python HUBSUS360_ETL.py --sih "C:\dados\dados_sih" --anos 2025 --meses 6
```

## Saídas

Por padrão, os resultados são salvos na pasta `resultados_hubsus360_relacional`, organizada em:

- `dimensoes`: tabelas auxiliares de municípios, tempo, hospitais e classificações;
- `fatos`: tabelas tratadas e agregadas para as análises do HUBSUS360;
- `validacoes`: arquivos que conferem se os totais foram processados corretamente.

A principal tabela para o painel e para a análise exploratória municipal é `fato_municipio_mes.csv`. Ela reúne, por município e mês, informações como internações, ICSAP, gastos, permanência, óbitos e indicadores.

O detalhe individual das internações não é gerado por padrão, pois pode conter milhões de linhas. Quando necessário, pode ser criado com:

```bash
python HUBSUS360_ETL.py --sih "C:\dados\dados_sih" --gerar-detalhe
```

## CNES histórico

O código também está preparado para receber dados históricos do CNES. Essa etapa é opcional e utiliza arquivos de estabelecimentos e leitos para complementar as análises de hospitais e capacidade cadastrada.

```bash
python HUBSUS360_ETL.py --sih "C:\dados\dados_sih" --cnes "C:\dados\cnes"
```

Os dados do CNES representam a capacidade cadastrada de leitos, e não a disponibilidade de vagas em tempo real.

# HUBSUS360

Projeto acadêmico do Challenge Oracle + FIAP (1TSCPF), voltado à inteligência de dados aplicada à saúde pública.

O HUBSUS360 integra SIH/SUS, CNES, SIGTAP, CID-10, IBGE e Regiões de Saúde para analisar internações hospitalares, ICSAP, capacidade cadastrada e padrões de demanda em São Paulo, de janeiro de 2023 a dezembro de 2025.

## Objetivos

- preparar um modelo relacional para Oracle Database;
- analisar perfil, município, estabelecimento, diagnóstico, procedimento e sazonalidade;
- calcular métricas de ICSAP, permanência, óbitos e o IEP experimental;
- demonstrar ETL, AED, governança, segurança e Machine Learning.

A base trabalha com perfis agregados de internação. Uma linha não representa necessariamente uma AIH ou um paciente individual.

## Organização

    etl/                         ETL principal e orquestração
    aed/                         Análise exploratória e metodologia
    sql/                         Materiais SQL em preparação
    docs/                        Dicionário de dados
    sprints/01-ideacao/          Ideação
    sprints/02-arquitetura/      Arquitetura
    sprints/03-implementacao/    Entregas técnicas por frente
    sprints/04-solucao-final/    Solução final

Datasets brutos, CSVs completos, caches, temporários e ZIPs não são versionados por causa do tamanho. Evidências que continham número de matrícula foram substituídas por resumos técnicos ou não foram publicadas.

## ETL e modelo

etl/HUBSUS360_ETL.py lê DBC/DBF do SIH/SUS e referências do CNES, SIGTAP, CID-10 e Regiões de Saúde. Valida competências, normaliza códigos e datas, diferencia AIH inicial e continuidade, classifica os grupos de ICSAP e gera as tabelas relacionais, a saída da AED e validações.

Tabelas: T_GRUPO_ICSAP, T_DIAGNOSTICO, T_MUNICIPIO, T_ESTABELECIMENTO, T_PROCEDIMENTO, T_FAIXA_ETARIA, T_SEXO, T_RACA_COR, T_PERFIL_INTERNACAO, T_RESUMO_AIH, T_RESUMO_ASSISTENCIAL, T_CATEGORIA_LEITO e T_CAPACIDADE_LEITO.

    python -m pip install pandas numpy openpyxl
    python etl/HUBSUS360_ETL.py --referencias referencias_hubsus360 --anos 2023 2024 2025

## SQL Developer

O DDL definitivo e a mini carga DML ainda estão em revisão. Eles não foram publicados nem devem ser executados a partir deste repositório até a validação final.

A área sql/ permanece reservada para a futura demonstração executável no Oracle SQL Developer.

## AED e sprints

A AED cobre exploração descritiva, capacidade hospitalar, padrões/clusters e explicabilidade. O IEP é uma proposta experimental e deve ser interpretado conforme a granularidade da base.

Sprint 1: ideação. Sprint 2: arquitetura, Oracle, Object Storage, APEX e Select AI. Sprint 3: ETL/Python, SQL relacional, arquitetura moderna, ética/governança/segurança e Machine Learning. Sprint 4: solução final.

## Integrantes

- Guilherme Ladeira Corrêa Santos
- Lucas Amaral da Silva Barros
- Lucas Araújo Curci
- Lucas Luna Pimentel
- Pedro Henrique Moretti Aguiar

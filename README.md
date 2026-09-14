# HUBSUS360

## Do dado ao diagnóstico da saúde pública

O HUBSUS360 transforma dados públicos e fragmentados do SUS em informações estratégicas para revelar onde a rede hospitalar está sob pressão, quais internações estão relacionadas a condições sensíveis à atenção primária e onde os gestores podem priorizar ações preventivas.

Desenvolvido pelo grupo GP3L no Challenge Oracle + FIAP, o projeto integra dados do SIH/SUS, CNES, SIGTAP, CID-10, IBGE e Regiões de Saúde para analisar as internações hospitalares do estado de São Paulo entre 2023 e 2025.

## O problema

Secretarias e gestores de saúde precisam tomar decisões importantes a partir de dados espalhados em diferentes fontes. Quando essas informações precisam ser organizadas e cruzadas manualmente, a resposta demora e a oportunidade de agir pode passar.

## A solução

O HUBSUS360 organiza o histórico hospitalar por meio de um processo de ETL em Python, estrutura os dados em um modelo relacional Oracle e apresenta indicadores em uma visão analítica. A solução combina volume de internações, perfil assistencial, permanência, óbitos, estabelecimentos, diagnósticos e capacidade hospitalar.

Além de mostrar números, o projeto busca explicar a pressão assistencial:

- ICSAP: identifica internações relacionadas a condições sensíveis à atenção primária, ajudando a investigar possíveis causas de sobrecarga hospitalar;
- IEP experimental: acrescenta uma dimensão financeira ao evidenciar a participação dos atendimentos classificados como ICSAP no valor analisado; não é um indicador oficial do SUS;
- análise exploratória: revela padrões, diferenças entre municípios e comportamentos que merecem investigação;
- painel e consultas analíticas: aproximam os resultados da linguagem de gestão e apoiam a priorização de ações.

## Pitch

[🎥 Assistir ao pitch do HUBSUS360 no YouTube](https://youtu.be/lE07c2YoyUk)

## O que entregamos

| Frente | O que foi desenvolvido |
|---|---|
| Engenharia de dados | Extração, transformação, padronização e preparação dos dados do SIH/SUS e fontes auxiliares |
| Qualidade | Validações de estrutura, registros, competências, códigos e resultados gerados pelo ETL |
| Modelo relacional | 13 tabelas Oracle com chaves, relacionamentos, índices e regras de integridade |
| Análise de dados | AED com estatísticas descritivas, distribuições, valores faltantes, outliers e correlações |
| Indicadores | ICSAP, permanência, mortalidade, capacidade hospitalar e IEP experimental |
| Visualização | Proposta de painel para comparar municípios, períodos e perfis de atendimento |

## Como o projeto funciona

1. Os dados públicos são coletados e organizados.
2. O ETL em Python realiza a limpeza, padronização e classificação dos registros.
3. Os resultados são estruturados no Oracle Database.
4. A AED e os indicadores transformam os dados em evidências.
5. O painel apoia gestores na investigação e priorização de ações.

## Escopo e transparência

- O recorte principal utiliza dados de São Paulo entre 2023 e 2025.
- A base trabalha principalmente com perfis agregados de internação; uma linha não representa necessariamente uma AIH ou um paciente individual.
- A base completa e os arquivos brutos não são versionados por tamanho e proteção dos dados utilizados no desenvolvimento.
- O IEP é uma métrica experimental do projeto e não representa, sozinho, a eficiência completa da Atenção Primária.

## Tecnologias

Python · Pandas · SQL · Oracle Autonomous Database · Apache Airflow · Jupyter Notebook · Oracle APEX · GitHub

## Organização do repositório

    etl/                         Processo de preparação e integração dos dados
    etl/airflow/                 DAG para orquestração do fluxo analítico
    aed/                         Análise exploratória e metodologia analítica
    sql/ddl/                     Scripts de criação do modelo relacional
    sql/dml/                     Scripts de carga dos dados de referência e amostra
    docs/                        Dicionário de dados e documentação do modelo

Cada pasta possui um README próprio com a explicação dos arquivos e da finalidade daquele componente.

## Equipe

- Guilherme Ladeira Corrêa Santos
- Lucas Amaral da Silva Barros
- Lucas Araújo Curci
- Lucas Luna Pimentel
- Pedro Henrique Moretti Aguiar

## Contexto acadêmico

Challenge Oracle + FIAP 2026 · Turma 1TSCPF · Grupo GP3L
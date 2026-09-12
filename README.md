# HUBSUS360

Projeto acadêmico desenvolvido pelo grupo no Challenge Oracle + FIAP, com foco no uso de dados públicos para apoiar a gestão da saúde.

O HUBSUS360 integra dados do SIH/SUS, CNES, SIGTAP, CID-10, IBGE e Regiões de Saúde para analisar internações hospitalares, Internações por Condições Sensíveis à Atenção Primária (ICSAP), capacidade hospitalar e padrões de demanda no estado de São Paulo, entre 2023 e 2025.

## Objetivos

- organizar e preparar dados públicos de saúde para análise;
- estruturar um modelo relacional para Oracle Database;
- analisar perfis de internação, municípios, estabelecimentos, diagnósticos e procedimentos;
- apoiar indicadores de ICSAP, permanência, óbitos e o IEP experimental;
- documentar o processo de dados, a arquitetura e as decisões do projeto.

A base trabalha principalmente com perfis agregados de internação. Uma linha não representa necessariamente uma AIH ou um paciente individual.

## Organização

    etl/                         Processo de preparação e integração dos dados
    aed/                         Análise exploratória e metodologia analítica
    sql/                         Modelo relacional e experimentos no Oracle SQL Developer
    docs/                        Dicionário de dados e documentação do modelo
    sprints/01-ideacao/          Definição do problema e da proposta
    sprints/02-arquitetura/      Arquitetura e tecnologias da solução
    sprints/03-implementacao/    Desenvolvimento das frentes técnicas
    sprints/04-solucao-final/    Consolidação da solução e dos indicadores

Datasets brutos, bases completas, caches e arquivos temporários não são versionados por causa do tamanho e da proteção das informações utilizadas no desenvolvimento. Os materiais públicos não incluem números de matrícula.

## ETL e modelo

O processo de ETL lê arquivos DBC/DBF do SIH/SUS e referências do CNES, SIGTAP, CID-10 e Regiões de Saúde. Em seguida, valida competências, normaliza códigos e datas, classifica os grupos de ICSAP e gera as tabelas relacionais, a saída para AED e os relatórios de validação.

O modelo relacional possui 13 tabelas: T_GRUPO_ICSAP, T_DIAGNOSTICO, T_MUNICIPIO, T_ESTABELECIMENTO, T_PROCEDIMENTO, T_FAIXA_ETARIA, T_SEXO, T_RACA_COR, T_PERFIL_INTERNACAO, T_RESUMO_AIH, T_RESUMO_ASSISTENCIAL, T_CATEGORIA_LEITO e T_CAPACIDADE_LEITO.

## SQL Developer

A pasta sql/mini contém um DDL experimental para reproduzir a estrutura relacional no Oracle SQL Developer. A mini carga DML e as consultas analíticas continuam aguardando validação final e, por isso, não acompanham esta versão pública.

## AED e evolução do projeto

A AED reúne a metodologia de exploração dos dados, os indicadores propostos e os cuidados de interpretação. As pastas de sprints mostram como a solução evoluiu desde a ideação até as frentes de implementação e a proposta final.

## Integrantes

- Guilherme Ladeira Corrêa Santos
- Lucas Amaral da Silva Barros
- Lucas Araújo Curci
- Lucas Luna Pimentel
- Pedro Henrique Moretti Aguiar

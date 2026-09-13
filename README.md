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
    aed/                         Análise exploratória, código e metodologia analítica
    sql/                         Modelo relacional e experimentos no Oracle SQL Developer
    docs/                        Dicionário de dados e documentação do modelo

Datasets brutos, bases completas, caches e arquivos temporários não são versionados por causa do tamanho e da proteção das informações utilizadas no desenvolvimento. Os materiais públicos não incluem números de matrícula.

## Pitch

A gestão da saúde pública precisa identificar onde as internações poderiam ser evitadas e como a capacidade hospitalar está sendo utilizada. O HUBSUS360 integra dados públicos de saúde, organiza essas informações em um modelo relacional e combina ETL, análise exploratória, ICSAP e um IEP experimental para apoiar a priorização de municípios e a tomada de decisão.

## ETL e modelo

O processo de ETL lê arquivos DBC/DBF do SIH/SUS e referências do CNES, SIGTAP, CID-10 e Regiões de Saúde. Em seguida, valida competências, normaliza códigos e datas, classifica os grupos de ICSAP e gera as tabelas relacionais, a saída para AED e os relatórios de validação.

O modelo relacional possui 13 tabelas: T_GRUPO_ICSAP, T_DIAGNOSTICO, T_MUNICIPIO, T_ESTABELECIMENTO, T_PROCEDIMENTO, T_FAIXA_ETARIA, T_SEXO, T_RACA_COR, T_PERFIL_INTERNACAO, T_RESUMO_AIH, T_RESUMO_ASSISTENCIAL, T_CATEGORIA_LEITO e T_CAPACIDADE_LEITO.

## SQL Developer

A pasta sql/mini contém o DDL experimental e uma carga DML de amostra para reproduzir a estrutura relacional e conferir chaves e relacionamentos no Oracle SQL Developer. A carga não representa a base completa do SIH/SUS.

## AED

A pasta aed reúne o notebook com o código da análise exploratória e a documentação metodológica. O notebook utiliza o dataset tratado gerado pelo ETL; a base completa não é versionada.

## Integrantes

- Guilherme Ladeira Corrêa Santos
- Lucas Amaral da Silva Barros
- Lucas Araújo Curci
- Lucas Luna Pimentel
- Pedro Henrique Moretti Aguiar

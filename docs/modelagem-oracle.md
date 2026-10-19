# Modelagem relacional Oracle do HUBSUS360

## Objetivo

O modelo relacional do HUBSUS360 organiza dados hospitalares e cadastrais do SUS para permitir consultas consistentes sobre perfis de internação, diagnósticos, indicadores assistenciais e capacidade hospitalar.

A estrutura utiliza 13 tabelas e separa informações de referência dos registros analíticos, com chaves primárias (PK), chaves estrangeiras (FK), restrições de unicidade e regras de validação.

## Organização do modelo

| Grupo | Tabelas | Responsabilidade |
| --- | --- | --- |
| Perfil central | `T_PERFIL_INTERNACAO` | Relaciona competência, município de residência, estabelecimento, diagnóstico, procedimento e atributos demográficos. |
| Classificações | `T_DIAGNOSTICO`, `T_GRUPO_ICSAP`, `T_PROCEDIMENTO` | Descrevem diagnósticos CID-10, grupos ICSAP e procedimentos. |
| Território e rede | `T_MUNICIPIO`, `T_ESTABELECIMENTO` | Identificam municípios e estabelecimentos de saúde. |
| Perfil demográfico | `T_FAIXA_ETARIA`, `T_SEXO`, `T_RACA_COR` | Padronizam categorias usadas nas análises. |
| Indicadores agregados | `T_RESUMO_ASSISTENCIAL`, `T_RESUMO_AIH` | Guardam quantidades, permanência e valores associados aos perfis. |
| Capacidade hospitalar | `T_CAPACIDADE_LEITO`, `T_CATEGORIA_LEITO` | Organizam leitos por estabelecimento, categoria e competência. |

## Relação entre diagnósticos e ICSAP

A tabela `T_DIAGNOSTICO` identifica diagnósticos pelo campo `CD_CID10` e pode associá-los a um grupo por `ID_GRUPO_ICSAP`.

A tabela `T_GRUPO_ICSAP` descreve as condições sensíveis à atenção primária. A associação é opcional: nem todo diagnóstico pertence a um grupo ICSAP. Essa separação ajuda a consultar e interpretar classificações sem repetir a descrição do grupo em cada perfil de internação.

## Granularidade dos dados

A tabela `T_PERFIL_INTERNACAO` representa um **perfil analítico agregado**, não necessariamente uma internação individual ou um paciente. O perfil reúne dimensões como competência, município de residência, CNES, diagnóstico e características demográficas.

Os resumos `T_RESUMO_ASSISTENCIAL` e `T_RESUMO_AIH` fazem referência ao identificador `ID_PERFIL_INTERNACAO` e utilizam restrições de unicidade para manter, no máximo, um resumo de cada tipo por perfil.

## Integridade dos dados

No DDL, as restrições ajudam a manter a consistência da base:

- **PK e FK:** identificam registros e preservam relacionamentos entre tabelas.
- **UNIQUE:** impede duplicidades em identificadores ou combinações de negócio definidas.
- **NOT NULL:** exige o preenchimento dos campos essenciais.
- **CHECK:** valida limites e regras; por exemplo, a quantidade de leitos SUS não pode exceder a quantidade total de leitos existentes em `T_CAPACIDADE_LEITO`.
- **Índices:** apoiam as consultas por campos como competência, CNES e classificações.

## Referências no próprio repositório

- [Script DDL experimental](../sql/ddl/01_ddl_experimental.sql)
- [Documentação e dicionário de dados](./README.md)
- [Visão geral do HUBSUS360](../README.md)

Este documento complementa o dicionário existente com uma visão conceitual da modelagem. Para os tipos, relacionamentos e restrições exatos, consulte o script DDL versionado.

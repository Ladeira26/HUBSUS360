# SQL Developer — modelo relacional experimental

Esta pasta reúne os scripts para reproduzir uma amostra do modelo relacional do HUBSUS360 no Oracle SQL Developer.

Arquivos disponíveis:

- `01_ddl_experimental.sql`: cria as 13 tabelas, índices, chaves e regras de integridade.
- `02_dml_amostra.sql`: insere uma carga de demonstração relacionada às 13 tabelas.

Como reproduzir:

1. Abra um schema Oracle vazio no SQL Developer.
2. Execute `01_ddl_experimental.sql`.
3. Execute `02_dml_amostra.sql` usando Executar como Script (F5).
4. Consulte as tabelas para conferir chaves e relacionamentos.

A carga é uma amostra para validação técnica e não representa a base completa do SIH/SUS. O DDL e o DML não substituem a execução do ETL completo.

Não há dados pessoais ou números de matrícula neste material.

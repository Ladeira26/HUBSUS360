# Airflow — orquestração do processamento

Esta pasta contém um exemplo de DAG para orquestrar o fluxo analítico do HUBSUS360.

A DAG organiza as etapas de ingestão, transformação pelo ETL, validação dos resultados e preparação de uma amostra analítica. A disponibilização produtiva no Oracle depende da configuração do ambiente de destino; credenciais e segredos não são versionados.

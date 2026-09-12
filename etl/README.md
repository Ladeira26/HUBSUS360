# ETL — preparação dos dados

Esta pasta contém o processo principal de extração, transformação e preparação dos dados do HUBSUS360.

O ETL processa os arquivos do SIH/SUS de São Paulo e integra referências do CNES, SIGTAP, CID-10 e Regiões de Saúde. O resultado inclui as tabelas relacionais, a base preparada para AED e relatórios de validação.

A estrutura de referências esperada está documentada no próprio processo. As fontes brutas não são incluídas no repositório.

Para executar o código:

    python -m pip install pandas numpy openpyxl
    python etl/HUBSUS360_ETL.py --referencias referencias_hubsus360 --anos 2023 2024 2025

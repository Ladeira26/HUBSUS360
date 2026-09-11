# ETL HUBSUS360

HUBSUS360_ETL.py processa SIH/SUS de São Paulo e complementa os registros com CNES, SIGTAP, CID-10 e Regiões de Saúde.

Estrutura esperada: referencias_hubsus360/dados_sih, ST, LT, sigtap, cid10, dominios_cnes, regioes_saude e cnes_estabelecimentos.

    python -m pip install pandas numpy openpyxl
    python etl/HUBSUS360_ETL.py --referencias referencias_hubsus360 --anos 2023 2024 2025

As fontes não são incluídas. A saída contém tabelas Oracle, dataset para AED, validações e cache mensal.

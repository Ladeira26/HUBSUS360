"""DAG do pipeline analítico do HUBSUS360.

Orquestra quatro etapas: ingestão, transformação pelo ETL existente,
validação dos resultados e disponibilização de uma amostra analítica.
"""

from __future__ import annotations

import csv
import logging
import subprocess
import sys
import zipfile
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from urllib.request import urlopen

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator


logger = logging.getLogger("hubsus360_pipeline")

BASE_DIR = Path("/opt/airflow/hubsus360")
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
REFERENCIAS_DIR = RAW_DIR / "referencias_hubsus360"
RESULTADOS_DIR = DATA_DIR / "resultados" / "processamento_historico"
ETL_PATH = BASE_DIR / "etl" / "ETL_HUBSUS360_ENTREGA_PATRICIA.py"

PASTAS_ESPERADAS = {
    "dados_sih",
    "ST",
    "LT",
    "sigtap",
    "cid10",
    "dominios_cnes",
    "regioes_saude",
    "cnes_estabelecimentos",
}

TABELAS_ESPERADAS = {
    "T_GRUPO_ICSAP",
    "T_DIAGNOSTICO",
    "T_MUNICIPIO",
    "T_ESTABELECIMENTO",
    "T_PROCEDIMENTO",
    "T_FAIXA_ETARIA",
    "T_SEXO",
    "T_RACA_COR",
    "T_PERFIL_INTERNACAO",
    "T_RESUMO_AIH",
    "T_RESUMO_ASSISTENCIAL",
    "T_CATEGORIA_LEITO",
    "T_CAPACIDADE_LEITO",
}


def _quantidade_linhas_csv(caminho: Path) -> int:
    """Conta registros sem carregar o arquivo inteiro na memória."""
    with caminho.open("r", encoding="utf-8-sig", errors="replace") as arquivo:
        return max(sum(1 for _ in arquivo) - 1, 0)


def executar_ingestao() -> dict[str, object]:
    """Baixa o ZIP do Object Storage, extrai e valida os dados brutos."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    arquivo_zip = RAW_DIR / "referencias_hubsus360.zip"
    arquivo_parcial = RAW_DIR / "referencias_hubsus360.zip.part"

    url_par = Variable.get("hubsus360_par_secret_url")
    logger.info("Iniciando download do Oracle Object Storage.")

    try:
        with urlopen(url_par, timeout=120) as resposta, arquivo_parcial.open("wb") as saida:
            tamanho_total = int(resposta.headers.get("Content-Length", "0"))
            baixado = 0
            proximo_log = 64 * 1024 * 1024

            while bloco := resposta.read(1024 * 1024):
                saida.write(bloco)
                baixado += len(bloco)
                if baixado >= proximo_log:
                    percentual = (baixado / tamanho_total * 100) if tamanho_total else 0
                    logger.info(
                        "Download: %.1f MiB de %.1f MiB (%.1f%%)",
                        baixado / 1024**2,
                        tamanho_total / 1024**2,
                        percentual,
                    )
                    proximo_log += 64 * 1024 * 1024

        arquivo_parcial.replace(arquivo_zip)
        logger.info("Download concluído: %.1f MiB", arquivo_zip.stat().st_size / 1024**2)
    except Exception as erro:
        arquivo_parcial.unlink(missing_ok=True)
        raise AirflowFailException(f"Falha no download dos dados brutos: {erro}") from erro

    logger.info("Iniciando extração dos dados brutos.")
    try:
        with zipfile.ZipFile(arquivo_zip) as pacote:
            pacote.extractall(RAW_DIR)
    except zipfile.BadZipFile as erro:
        raise AirflowFailException("O arquivo baixado não é um ZIP válido.") from erro

    pastas_encontradas = {pasta.name for pasta in REFERENCIAS_DIR.iterdir() if pasta.is_dir()}
    pastas_ausentes = sorted(PASTAS_ESPERADAS - pastas_encontradas)
    if pastas_ausentes:
        raise AirflowFailException(
            "Pastas obrigatórias ausentes após a extração: " + ", ".join(pastas_ausentes)
        )

    quantidade_arquivos = sum(1 for item in REFERENCIAS_DIR.rglob("*") if item.is_file())
    if quantidade_arquivos == 0:
        raise AirflowFailException("Nenhum arquivo bruto foi encontrado após a extração.")

    logger.info("Extração concluída.")
    logger.info(
        "Ingestão validada. Diretório: %s | Arquivos encontrados: %s",
        REFERENCIAS_DIR,
        quantidade_arquivos,
    )
    return {
        "diretorio_dados_brutos": str(REFERENCIAS_DIR),
        "quantidade_arquivos": quantidade_arquivos,
    }


def executar_transformacao() -> dict[str, object]:
    """Executa o ETL em Python/Pandas e confirma as 13 tabelas geradas."""
    if not ETL_PATH.exists():
        raise FileNotFoundError(f"Código do ETL não encontrado: {ETL_PATH}")
    if not REFERENCIAS_DIR.exists():
        raise FileNotFoundError(f"Dados brutos não encontrados: {REFERENCIAS_DIR}")

    RESULTADOS_DIR.mkdir(parents=True, exist_ok=True)
    comando = [
        sys.executable,
        str(ETL_PATH),
        "--referencias",
        str(REFERENCIAS_DIR),
        "--saida",
        str(RESULTADOS_DIR),
        "--anos",
        "2023",
        "2024",
        "2025",
    ]

    logger.info("Iniciando transformação pelo ETL do HUBSUS360.")
    processo = subprocess.Popen(
        comando,
        cwd=BASE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    assert processo.stdout is not None
    for linha in processo.stdout:
        logger.info("[ETL] %s", linha.rstrip())

    codigo_retorno = processo.wait()
    if codigo_retorno != 0:
        raise AirflowFailException(f"O ETL terminou com código {codigo_retorno}.")

    pasta_tabelas = RESULTADOS_DIR / "tabelas_oracle"
    arquivos_tabelas = {arquivo.stem for arquivo in pasta_tabelas.glob("T_*.csv")}
    ausentes = sorted(TABELAS_ESPERADAS - arquivos_tabelas)
    if ausentes:
        raise AirflowFailException("Tabelas não geradas pelo ETL: " + ", ".join(ausentes))

    perfil = pasta_tabelas / "T_PERFIL_INTERNACAO.csv"
    quantidade_perfis = _quantidade_linhas_csv(perfil)
    logger.info(
        "Transformação concluída: %s tabelas relacionais foram geradas.",
        len(TABELAS_ESPERADAS),
    )
    return {
        "diretorio_saida": str(RESULTADOS_DIR),
        "quantidade_tabelas": len(TABELAS_ESPERADAS),
        "quantidade_perfis": quantidade_perfis,
    }


def executar_validacao() -> dict[str, object]:
    """Valida a estrutura final e interpreta o relatório de qualidade do ETL."""
    pasta_tabelas = RESULTADOS_DIR / "tabelas_oracle"
    arquivos_tabelas = {arquivo.stem for arquivo in pasta_tabelas.glob("T_*.csv")}
    ausentes = sorted(TABELAS_ESPERADAS - arquivos_tabelas)
    if ausentes:
        raise AirflowFailException("Estrutura incompleta. Tabelas ausentes: " + ", ".join(ausentes))

    logger.info("Estrutura validada: 13 tabelas relacionais encontradas.")
    arquivo_validacao = RESULTADOS_DIR / "validacoes" / "VALIDACAO_MODELO_FINAL.csv"
    if not arquivo_validacao.exists():
        raise FileNotFoundError(f"Relatório de validação não encontrado: {arquivo_validacao}")

    with arquivo_validacao.open("r", encoding="utf-8-sig", newline="") as arquivo:
        amostra = arquivo.read(8192)
        arquivo.seek(0)
        try:
            delimitador = csv.Sniffer().sniff(amostra, delimiters=",;|").delimiter
        except csv.Error:
            delimitador = ","
        registros = list(csv.DictReader(arquivo, delimiter=delimitador))

    aprovadas = sum(1 for item in registros if item.get("RESULTADO", "").upper() == "APROVADO")
    avisos = sum(1 for item in registros if item.get("RESULTADO", "").upper() == "AVISO")
    revisar = [item for item in registros if item.get("RESULTADO", "").upper() == "REVISAR"]

    logger.info("Resultado das verificações: {'APROVADO': %s, 'AVISO': %s}", aprovadas, avisos)
    for item in registros:
        if item.get("RESULTADO", "").upper() == "AVISO":
            logger.warning(
                "Aviso de qualidade: %s | ocorrências: %s",
                item.get("VERIFICACAO", "Aviso sem descrição"),
                item.get("QUANTIDADE_PROBLEMAS", "não informada"),
            )

    if revisar:
        nomes = [item.get("VERIFICACAO", "verificação sem nome") for item in revisar]
        raise AirflowFailException("Validações reprovadas: " + "; ".join(nomes))

    logger.info("Validação concluída: %s verificações aprovadas e %s avisos aceitos.", aprovadas, avisos)
    return {
        "quantidade_verificacoes": len(registros),
        "aprovadas": aprovadas,
        "avisos": avisos,
        "tabelas_validadas": len(TABELAS_ESPERADAS),
    }


def executar_carga_analitica() -> dict[str, object]:
    """Gera e confere uma amostra final com 20 linhas tratadas."""
    quantidade_linhas = 20
    arquivo_origem = RESULTADOS_DIR / "aed" / "DATASET_AED_HUBSUS360.csv"
    diretorio_destino = DATA_DIR / "resultados" / "carga_analitica"
    arquivo_destino = diretorio_destino / "AMOSTRA_CARGA_ANALITICA_20_LINHAS.csv"

    logger.info("Iniciando a carga analítica demonstrativa.")
    logger.info("Arquivo tratado de origem: %s", arquivo_origem)
    if not arquivo_origem.exists():
        raise FileNotFoundError(f"Dataset analítico tratado não encontrado: {arquivo_origem}")

    diretorio_destino.mkdir(parents=True, exist_ok=True)
    with arquivo_origem.open("r", encoding="utf-8-sig", newline="") as entrada:
        trecho_inicial = entrada.read(8192)
        entrada.seek(0)
        try:
            delimitador = csv.Sniffer().sniff(trecho_inicial, delimiters=";,|\t").delimiter
        except csv.Error:
            delimitador = ";"
        leitor = csv.reader(entrada, delimiter=delimitador)
        cabecalho = next(leitor, None)
        if cabecalho is None:
            raise ValueError("O dataset analítico está vazio.")
        linhas = list(islice(leitor, quantidade_linhas))

    if len(linhas) != quantidade_linhas:
        raise ValueError(
            f"Eram esperadas {quantidade_linhas} linhas, mas foram encontradas {len(linhas)}."
        )

    with arquivo_destino.open("w", encoding="utf-8-sig", newline="") as saida:
        escritor = csv.writer(saida, delimiter=delimitador, lineterminator="\n")
        escritor.writerow(cabecalho)
        escritor.writerows(linhas)

    quantidade_gerada = _quantidade_linhas_csv(arquivo_destino)
    if quantidade_gerada != quantidade_linhas:
        raise ValueError(
            f"A amostra possui {quantidade_gerada} linhas, mas deveria possuir {quantidade_linhas}."
        )

    logger.info("Carga analítica demonstrativa concluída: %s linhas tratadas.", quantidade_gerada)
    logger.info("Quantidade de colunas: %s", len(cabecalho))
    logger.info("Amostra analítica gerada em: %s", arquivo_destino)
    logger.info(
        "Em produção, os dados validados seriam disponibilizados "
        "na Serving Layer do Oracle Database 26 AI."
    )
    return {
        "arquivo_carga_analitica": str(arquivo_destino),
        "quantidade_linhas": quantidade_gerada,
        "quantidade_colunas": len(cabecalho),
        "destino_conceitual": "Oracle Database 26 AI - Serving Layer",
    }


with DAG(
    dag_id="hubsus360_pipeline_lambda",
    description="Pipeline em lote do HUBSUS360 orquestrado pelo Apache Airflow",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["hubsus360", "lambda", "oracle", "sprint3"],
) as dag:
    ingestao = PythonOperator(task_id="ingestao", python_callable=executar_ingestao)
    transformacao = PythonOperator(task_id="transformacao", python_callable=executar_transformacao)
    validacao = PythonOperator(task_id="validacao", python_callable=executar_validacao)
    carga_analitica = PythonOperator(
        task_id="carga_analitica",
        python_callable=executar_carga_analitica,
    )

    ingestao >> transformacao >> validacao >> carga_analitica

import logging
import pendulum

from datetime import timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


# Logger utilizado para registrar informações na interface do Airflow
logger = logging.getLogger(__name__)


# ============================================================
# FUNÇÕES EXECUTADAS PELAS TAREFAS
# Neste primeiro teste, elas apenas registram mensagens.
# Posteriormente, receberão a lógica real do HUBSUS360.
# ============================================================

def executar_ingestao():
    import logging
    import zipfile
    from pathlib import Path
    from urllib.request import Request, urlopen

    from airflow.sdk import Variable

    logger = logging.getLogger(__name__)

    # O link secreto é obtido da variável cadastrada no Airflow.
    par_url = Variable.get("hubsus360_par_secret_url")

    base_dir = Path("/opt/airflow/hubsus360/data")
    downloads_dir = base_dir / "downloads"
    raw_dir = base_dir / "raw"

    zip_path = downloads_dir / "referencias_hubsus360.zip"
    temp_path = downloads_dir / "referencias_hubsus360.zip.part"

    downloads_dir.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)

    diretorios_esperados = {
        "dados_sih",
        "ST",
        "LT",
        "sigtap",
        "cid10",
        "dominios_cnes",
        "regioes_saude",
        "cnes_estabelecimentos",
    }

    def localizar_raiz_referencias():
        candidatos = [raw_dir]

        candidatos.extend(
            pasta for pasta in raw_dir.iterdir()
            if pasta.is_dir()
        )

        for candidato in candidatos:
            if all(
                (candidato / nome).is_dir()
                for nome in diretorios_esperados
            ):
                return candidato

        return None

    # Reutiliza o arquivo caso ele já tenha sido baixado corretamente.
    if zip_path.exists() and zipfile.is_zipfile(zip_path):
        logger.info(
            "ZIP já existente e válido: %.1f MiB",
            zip_path.stat().st_size / (1024 * 1024),
        )
    else:
        if temp_path.exists():
            temp_path.unlink()

        logger.info("Iniciando download do Oracle Object Storage.")

        requisicao = Request(
            par_url,
            headers={"User-Agent": "HUBSUS360-Airflow"},
        )

        with urlopen(requisicao, timeout=300) as resposta:
            tamanho_total = int(
                resposta.headers.get("Content-Length", "0")
            )

            baixado = 0
            proximo_log = 64 * 1024 * 1024

            with temp_path.open("wb") as arquivo_saida:
                while True:
                    bloco = resposta.read(8 * 1024 * 1024)

                    if not bloco:
                        break

                    arquivo_saida.write(bloco)
                    baixado += len(bloco)

                    if baixado >= proximo_log:
                        if tamanho_total:
                            percentual = baixado / tamanho_total * 100
                            logger.info(
                                "Download: %.1f MiB de %.1f MiB (%.1f%%)",
                                baixado / (1024 * 1024),
                                tamanho_total / (1024 * 1024),
                                percentual,
                            )
                        else:
                            logger.info(
                                "Download: %.1f MiB",
                                baixado / (1024 * 1024),
                            )

                        proximo_log += 64 * 1024 * 1024

        temp_path.replace(zip_path)

        logger.info(
            "Download concluído: %.1f MiB",
            zip_path.stat().st_size / (1024 * 1024),
        )

    if zip_path.stat().st_size < 1024 * 1024:
        raise ValueError("O arquivo baixado possui tamanho inválido.")

    if not zipfile.is_zipfile(zip_path):
        raise ValueError("O arquivo baixado não é um ZIP válido.")

    raiz_referencias = localizar_raiz_referencias()

    if raiz_referencias is None:
        logger.info("Iniciando extração dos dados brutos.")

        raw_resolvido = raw_dir.resolve()

        with zipfile.ZipFile(zip_path, "r") as arquivo_zip:
            # Evita que um caminho interno do ZIP saia da pasta raw.
            for membro in arquivo_zip.infolist():
                destino = (raw_dir / membro.filename).resolve()

                if (
                    destino != raw_resolvido
                    and raw_resolvido not in destino.parents
                ):
                    raise ValueError(
                        f"Caminho inválido encontrado no ZIP: "
                        f"{membro.filename}"
                    )

            arquivo_zip.extractall(raw_dir)

        logger.info("Extração concluída.")
        raiz_referencias = localizar_raiz_referencias()

    if raiz_referencias is None:
        pastas_encontradas = [
            pasta.name
            for pasta in raw_dir.iterdir()
            if pasta.is_dir()
        ]

        raise ValueError(
            "Os oito diretórios esperados não foram encontrados. "
            f"Pastas encontradas: {pastas_encontradas}"
        )

    total_arquivos = sum(
        1 for arquivo in raiz_referencias.rglob("*")
        if arquivo.is_file()
    )

    logger.info(
        "Ingestão validada. Diretório: %s | Arquivos encontrados: %s",
        raiz_referencias,
        total_arquivos,
    )

    return str(raiz_referencias)


def executar_transformacao():
    import logging
    import subprocess
    import sys
    from pathlib import Path

    logger = logging.getLogger(__name__)

    script_etl = Path(
        "/opt/airflow/hubsus360/etl/"
        "HUBSUS360_ETL.py"
    )

    referencias_dir = Path(
        "/opt/airflow/hubsus360/data/raw/"
        "referencias_hubsus360"
    )

    saida_dir = Path(
        "/opt/airflow/hubsus360/data/resultados/"
        "processamento_historico"
    )

    if not script_etl.is_file():
        raise FileNotFoundError(
            f"Script do ETL não encontrado: {script_etl}"
        )

    if not referencias_dir.is_dir():
        raise FileNotFoundError(
            f"Diretório de referências não encontrado: "
            f"{referencias_dir}"
        )

    saida_dir.mkdir(parents=True, exist_ok=True)

    comando = [
        sys.executable,
        "-u",
        str(script_etl),
        "--referencias",
        str(referencias_dir),
        "--saida",
        str(saida_dir),
        "--anos",
        "2023",
        "2024",
        "2025",
    ]

    logger.info("Iniciando o ETL histórico do HUBSUS360.")
    logger.info("Período selecionado: 2023 a 2025.")
    logger.info("Diretório de saída: %s", saida_dir)

    processo = subprocess.Popen(
        comando,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
        cwd=str(script_etl.parent),
    )

    if processo.stdout is None:
        processo.kill()
        raise RuntimeError(
            "Não foi possível capturar a saída do ETL."
        )

    for linha in processo.stdout:
        linha = linha.rstrip()

        if linha:
            logger.info("[ETL] %s", linha)

    codigo_retorno = processo.wait()

    if codigo_retorno != 0:
        raise RuntimeError(
            "O ETL do HUBSUS360 terminou com erro. "
            f"Código de retorno: {codigo_retorno}"
        )

    tabelas_dir = saida_dir / "tabelas_oracle"

    tabelas_esperadas = {
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

    tabelas_geradas = {
        arquivo.stem
        for arquivo in tabelas_dir.glob("T_*.csv")
    }

    tabelas_faltantes = (
        tabelas_esperadas - tabelas_geradas
    )

    if tabelas_faltantes:
        raise ValueError(
            "O ETL terminou, mas algumas tabelas não foram "
            f"geradas: {sorted(tabelas_faltantes)}"
        )

    logger.info(
        "Transformação concluída com sucesso. "
        "%s tabelas relacionais foram geradas.",
        len(tabelas_geradas),
    )

    return {
        "diretorio_saida": str(saida_dir),
        "quantidade_tabelas": len(tabelas_geradas),
    }


def executar_validacao():
    import csv
    import logging
    from collections import Counter
    from pathlib import Path

    logger = logging.getLogger(__name__)

    saida_dir = Path(
        "/opt/airflow/hubsus360/data/resultados/"
        "processamento_historico"
    )

    tabelas_dir = saida_dir / "tabelas_oracle"
    validacoes_dir = saida_dir / "validacoes"

    relatorio_validacao = (
        validacoes_dir / "VALIDACAO_MODELO_FINAL.csv"
    )

    manifesto_arquivos = (
        validacoes_dir / "MANIFESTO_ARQUIVOS.csv"
    )

    dataset_aed = (
        saida_dir / "aed" / "DATASET_AED_HUBSUS360.csv"
    )

    tabelas_esperadas = {
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

    logger.info("Iniciando validação dos resultados do ETL.")

    # Verifica se os principais arquivos foram gerados.
    arquivos_obrigatorios = {
        relatorio_validacao,
        manifesto_arquivos,
        dataset_aed,
    }

    arquivos_ausentes = [
        str(arquivo)
        for arquivo in arquivos_obrigatorios
        if not arquivo.is_file()
    ]

    if arquivos_ausentes:
        raise FileNotFoundError(
            "Arquivos obrigatórios não encontrados: "
            f"{arquivos_ausentes}"
        )

    # Confere a geração das 13 tabelas relacionais.
    tabelas_geradas = {
        arquivo.stem
        for arquivo in tabelas_dir.glob("T_*.csv")
        if arquivo.stat().st_size > 0
    }

    tabelas_faltantes = (
        tabelas_esperadas - tabelas_geradas
    )

    if tabelas_faltantes:
        raise ValueError(
            "Tabelas ausentes ou vazias: "
            f"{sorted(tabelas_faltantes)}"
        )

    logger.info(
        "Estrutura validada: %s tabelas relacionais encontradas.",
        len(tabelas_geradas),
    )

    # Lê o relatório de qualidade produzido pelo próprio ETL.
    with relatorio_validacao.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as arquivo:
        verificacoes = list(csv.DictReader(arquivo))

    if not verificacoes:
        raise ValueError(
            "O relatório de validação está vazio."
        )

    colunas_obrigatorias = {
        "VERIFICACAO",
        "QUANTIDADE_PROBLEMAS",
        "RESULTADO",
    }

    if not colunas_obrigatorias.issubset(
        verificacoes[0].keys()
    ):
        raise ValueError(
            "O relatório de validação não possui "
            "as colunas esperadas."
        )

    contagem_resultados = Counter(
        linha["RESULTADO"].strip().upper()
        for linha in verificacoes
    )

    logger.info(
        "Resultado das verificações: %s",
        dict(contagem_resultados),
    )

    resultados_permitidos = {
        "APROVADO",
        "AVISO",
        "REVISAR",
    }

    resultados_desconhecidos = (
        set(contagem_resultados)
        - resultados_permitidos
    )

    if resultados_desconhecidos:
        raise ValueError(
            "Foram encontrados resultados desconhecidos: "
            f"{sorted(resultados_desconhecidos)}"
        )

    avisos = [
        linha
        for linha in verificacoes
        if linha["RESULTADO"].strip().upper() == "AVISO"
    ]

    for aviso in avisos:
        logger.warning(
            "Aviso de qualidade: %s | ocorrências: %s",
            aviso["VERIFICACAO"],
            aviso["QUANTIDADE_PROBLEMAS"],
        )

    pendencias = [
        linha
        for linha in verificacoes
        if linha["RESULTADO"].strip().upper() == "REVISAR"
    ]

    if pendencias:
        detalhes = [
            (
                linha["VERIFICACAO"],
                linha["QUANTIDADE_PROBLEMAS"],
            )
            for linha in pendencias
        ]

        raise ValueError(
            "A validação encontrou verificações que "
            f"precisam de revisão: {detalhes}"
        )

    logger.info(
        "Validação concluída: %s verificações aprovadas "
        "e %s avisos aceitos.",
        contagem_resultados.get("APROVADO", 0),
        contagem_resultados.get("AVISO", 0),
    )

    return {
        "quantidade_verificacoes": len(verificacoes),
        "aprovadas": contagem_resultados.get(
            "APROVADO", 0
        ),
        "avisos": contagem_resultados.get("AVISO", 0),
        "tabelas_validadas": len(tabelas_geradas),
    }


def executar_carga_analitica():
    import csv
    from itertools import islice
    from pathlib import Path

    quantidade_linhas = 20

    arquivo_origem = Path(
        "/opt/airflow/hubsus360/data/resultados/"
        "processamento_historico/aed/DATASET_AED_HUBSUS360.csv"
    )

    diretorio_destino = Path(
        "/opt/airflow/hubsus360/data/resultados/carga_analitica"
    )

    arquivo_destino = (
        diretorio_destino / "AMOSTRA_CARGA_ANALITICA_20_LINHAS.csv"
    )

    logger.info("Iniciando a carga analítica demonstrativa.")
    logger.info("Arquivo tratado de origem: %s", arquivo_origem)

    if not arquivo_origem.exists():
        raise FileNotFoundError(
            f"Dataset analítico tratado não encontrado: {arquivo_origem}"
        )

    diretorio_destino.mkdir(parents=True, exist_ok=True)

    with arquivo_origem.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as entrada:
        trecho_inicial = entrada.read(8192)
        entrada.seek(0)

        try:
            delimitador = csv.Sniffer().sniff(
                trecho_inicial,
                delimiters=";,|\t"
            ).delimiter
        except csv.Error:
            delimitador = ";"

        leitor = csv.reader(entrada, delimiter=delimitador)
        cabecalho = next(leitor, None)

        if cabecalho is None:
            raise ValueError("O dataset analítico está vazio.")

        linhas = list(islice(leitor, quantidade_linhas))

    if len(linhas) != quantidade_linhas:
        raise ValueError(
            f"Eram esperadas {quantidade_linhas} linhas, "
            f"mas foram encontradas somente {len(linhas)}."
        )

    with arquivo_destino.open(
        "w",
        encoding="utf-8-sig",
        newline=""
    ) as saida:
        escritor = csv.writer(
            saida,
            delimiter=delimitador,
            lineterminator="\n"
        )
        escritor.writerow(cabecalho)
        escritor.writerows(linhas)

    with arquivo_destino.open(
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as verificacao:
        leitor_validacao = csv.reader(
            verificacao,
            delimiter=delimitador
        )
        next(leitor_validacao, None)
        quantidade_gerada = sum(1 for _ in leitor_validacao)

    if quantidade_gerada != quantidade_linhas:
        raise ValueError(
            f"A amostra gerada possui {quantidade_gerada} linhas, "
            f"mas deveria possuir {quantidade_linhas}."
        )

    logger.info(
        "Carga analítica demonstrativa concluída: %s linhas tratadas.",
        quantidade_gerada
    )
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


# Configuração comum de tentativas em caso de falha
default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


# ============================================================
# DEFINIÇÃO DA DAG
# ============================================================

with DAG(
    dag_id="hubsus360_pipeline_lambda",
    description="Pipeline da Arquitetura Lambda do HUBSUS360",
    start_date=pendulum.datetime(
        2026,
        1,
        1,
        tz="America/Sao_Paulo",
    ),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["hubsus360", "lambda", "challenge"],
) as dag:

    ingestao = PythonOperator(
        task_id="ingestao",
        python_callable=executar_ingestao,
    )

    transformacao = PythonOperator(
        task_id="transformacao",
        python_callable=executar_transformacao,
    )

    validacao = PythonOperator(
        task_id="validacao",
        python_callable=executar_validacao,
    )

    carga_analitica = PythonOperator(
        task_id="carga_analitica",
        python_callable=executar_carga_analitica,
    )

    # Ordem obrigatória de execução
    ingestao >> transformacao >> validacao >> carga_analitica
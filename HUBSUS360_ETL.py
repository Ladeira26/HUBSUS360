"""ETL relacional e autocontido do HUBSUS360.

Por padrao, cria dimensoes e fatos agregados para AED e Oracle sem exportar a
base mestra detalhada. Cada competencia e processada isoladamente para reduzir
o uso de memoria. O detalhe SIH enxuto e opcional e, quando solicitado, fica
particionado por ano e mes.

Sem --sih, abre uma janela que permite escolher uma pasta (incluindo
subpastas) ou arquivos RD individuais. O periodo nunca e fixo: pode ser
filtrado por --anos e --meses e e validado contra ANO_CMPT e MES_CMPT.

Instalacao:
    python -m pip install pandas numpy dbfread

Exemplos:
    python HUBSUS360_ETL.py
    python HUBSUS360_ETL.py --sih dados --anos 2023 --meses 6
    python HUBSUS360_ETL.py --sih dados/2025 --gerar-detalhe
"""

from __future__ import annotations

import argparse
import calendar
import gzip
import json
import logging
import re
import shutil
import struct
import sys
import tempfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import numpy as np
import pandas as pd


LOG = logging.getLogger("hubsus360")
UF_ANALISE = "SP"
CODIGO_UF_ANALISE = "35"


# Tabelas utilizadas para descompactar o formato DBC do DATASUS. A rotina foi
# incluída no próprio programa para evitar compiladores externos no Windows.
MAX_BITS_DBC = 13
LITLEN_DBC = [
    11, 124, 8, 7, 28, 7, 188, 13, 76, 4, 10, 8, 12, 10, 12, 10,
    8, 23, 8, 9, 7, 6, 7, 8, 7, 6, 55, 8, 23, 24, 12, 11, 7, 9,
    11, 12, 6, 7, 22, 5, 7, 24, 6, 11, 9, 6, 7, 22, 7, 11, 38, 7,
    9, 8, 25, 11, 8, 11, 9, 12, 8, 12, 5, 38, 5, 38, 5, 11, 7, 5,
    6, 21, 6, 10, 53, 8, 7, 24, 10, 27, 44, 253, 253, 253, 252,
    252, 252, 13, 12, 45, 12, 45, 12, 61, 12, 45, 44, 173,
]
LENLEN_DBC = [2, 35, 36, 53, 38, 23]
DISTLEN_DBC = [2, 20, 53, 230, 247, 151, 248]
BASE_DBC = [3, 2, 4, 5, 6, 7, 8, 9, 10, 12, 16, 24, 40, 72, 136, 264]
EXTRA_DBC = [0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8]


class LeitorBitsDBC:
    """Lê a sequência de bits usada na compactação dos arquivos DBC."""

    def __init__(self, dados: bytes, inicio: int = 0) -> None:
        self.dados = dados
        self.posicao = inicio
        self.buffer = 0
        self.quantidade_bits = 0

    def ler(self, quantidade: int) -> int:
        valor = self.buffer
        while self.quantidade_bits < quantidade:
            if self.posicao >= len(self.dados):
                raise EOFError("Fim inesperado durante a descompactação do DBC.")
            valor |= self.dados[self.posicao] << self.quantidade_bits
            self.posicao += 1
            self.quantidade_bits += 8
        self.buffer = valor >> quantidade
        self.quantidade_bits -= quantidade
        return valor & ((1 << quantidade) - 1)


def construir_tabela_dbc(repeticoes: list[int]) -> tuple[list[int], list[int]]:
    """Monta uma tabela canônica de códigos usada pelo descompactador."""
    comprimentos = []
    for valor in repeticoes:
        quantidade = (valor >> 4) + 1
        comprimento = valor & 15
        comprimentos.extend([comprimento] * quantidade)

    contagens = [0] * (MAX_BITS_DBC + 1)
    for comprimento in comprimentos:
        contagens[comprimento] += 1

    simbolos = [
        simbolo
        for comprimento in range(1, MAX_BITS_DBC + 1)
        for simbolo, tamanho in enumerate(comprimentos)
        if tamanho == comprimento
    ]
    return contagens, simbolos


def decodificar_simbolo_dbc(
    leitor: LeitorBitsDBC,
    tabela: tuple[list[int], list[int]],
) -> int:
    """Decodifica um símbolo da tabela de compactação."""
    contagens, simbolos = tabela
    codigo = primeiro = indice = 0
    for comprimento in range(1, MAX_BITS_DBC + 1):
        codigo |= leitor.ler(1) ^ 1
        quantidade = contagens[comprimento]
        if codigo < primeiro + quantidade:
            return simbolos[indice + codigo - primeiro]
        indice += quantidade
        primeiro = (primeiro + quantidade) << 1
        codigo <<= 1
    raise ValueError("Foi encontrado um código inválido no arquivo DBC.")


def descompactar_dbc(arquivo_entrada: Path, arquivo_saida: Path) -> None:
    """Converte um DBC do DATASUS em DBF usando somente a biblioteca padrão."""
    dados_dbc = arquivo_entrada.read_bytes()
    if len(dados_dbc) < 32:
        raise ValueError("O arquivo DBC é menor que o cabeçalho esperado.")

    tamanho_cabecalho = struct.unpack_from("<H", dados_dbc, 8)[0]
    cabecalho = bytearray(dados_dbc[:tamanho_cabecalho])
    cabecalho[-1] = 0x0D

    leitor = LeitorBitsDBC(dados_dbc, tamanho_cabecalho + 4)
    literais_codificados = leitor.ler(8)
    tamanho_dicionario = leitor.ler(8)
    if literais_codificados not in (0, 1) or tamanho_dicionario not in (4, 5, 6):
        raise ValueError("O cabeçalho de compactação do DBC é inválido.")

    tabela_literais = construir_tabela_dbc(LITLEN_DBC)
    tabela_comprimentos = construir_tabela_dbc(LENLEN_DBC)
    tabela_distancias = construir_tabela_dbc(DISTLEN_DBC)
    saida = cabecalho

    while True:
        if leitor.ler(1):
            simbolo = decodificar_simbolo_dbc(leitor, tabela_comprimentos)
            comprimento = BASE_DBC[simbolo] + leitor.ler(EXTRA_DBC[simbolo])
            if comprimento == 519:
                break

            bits_distancia = 2 if comprimento == 2 else tamanho_dicionario
            distancia = (
                (decodificar_simbolo_dbc(leitor, tabela_distancias) << bits_distancia)
                + leitor.ler(bits_distancia)
                + 1
            )
            if distancia > len(saida) - tamanho_cabecalho:
                raise ValueError("Foi encontrada uma distância inválida no DBC.")

            padrao = saida[-distancia:]
            repeticoes = (comprimento + distancia - 1) // distancia
            saida.extend((padrao * repeticoes)[:comprimento])
        else:
            valor = (
                decodificar_simbolo_dbc(leitor, tabela_literais)
                if literais_codificados
                else leitor.ler(8)
            )
            saida.append(valor)

    arquivo_saida.write_bytes(saida)


COLUNAS_ORIGINAIS = [
    "UF_ZI", "ANO_CMPT", "MES_CMPT", "N_AIH", "IDENT", "MUNIC_RES",
    "MUNIC_MOV", "CNES", "DT_INTER", "DT_SAIDA", "DIAG_PRINC",
    "DIAG_SECUN", "PROC_REA", "DIAS_PERM", "VAL_TOT", "MORTE",
    "CAR_INT", "COD_IDADE", "IDADE", "SEXO", "RACA_COR", "COMPLEX",
    "NACIONAL",
]

RENOMEAR_COLUNAS = {
    "UF_ZI": "uf_zi",
    "ANO_CMPT": "ano_competencia",
    "MES_CMPT": "mes_competencia",
    "N_AIH": "numero_aih",
    "IDENT": "tipo_aih",
    "MUNIC_RES": "municipio_residencia_datasus",
    "MUNIC_MOV": "municipio_internacao_datasus",
    "CNES": "codigo_cnes",
    "DT_INTER": "data_internacao",
    "DT_SAIDA": "data_saida",
    "DIAG_PRINC": "cid_principal",
    "DIAG_SECUN": "cid_secundario",
    "PROC_REA": "procedimento_realizado",
    "DIAS_PERM": "dias_permanencia",
    "VAL_TOT": "valor_total",
    "MORTE": "obito",
    "CAR_INT": "carater_internacao",
    "COD_IDADE": "unidade_idade",
    "IDADE": "idade",
    "SEXO": "sexo",
    "RACA_COR": "raca_cor",
    "COMPLEX": "complexidade",
    "NACIONAL": "nacionalidade",
}

COLUNAS_IDENTIFICADORAS = [
    "uf_zi", "numero_aih", "tipo_aih", "municipio_residencia_datasus",
    "municipio_internacao_datasus", "codigo_cnes", "cid_principal",
    "cid_secundario", "procedimento_realizado", "carater_internacao",
    "unidade_idade", "sexo", "raca_cor", "complexidade", "nacionalidade",
]

GRUPOS_ICSAP = [
    (1, "Doenças preveníveis por imunização e condições sensíveis",
     ["A37", "A36", "A33", "A34", "A35", "B26", "B06", "B05",
      "A95", "B16", "G000", "A17", "A19", "A15", "A16", "A18",
      "I00", "I01", "I02", "A51", "A52", "A53", "B50", "B51",
      "B52", "B53", "B54", "B77"]),
    (2, "Gastroenterites infecciosas e complicações",
     ["E86", "A00", "A01", "A02", "A03", "A04", "A05", "A06",
      "A07", "A08", "A09"]),
    (3, "Anemia", ["D50"]),
    (4, "Deficiências nutricionais",
     ["E40", "E41", "E42", "E43", "E44", "E45", "E46", "E50",
      "E51", "E52", "E53", "E54", "E55", "E56", "E57", "E58",
      "E59", "E60", "E61", "E62", "E63", "E64"]),
    (5, "Infecções de ouvido, nariz e garganta",
     ["H66", "J00", "J01", "J02", "J03", "J06", "J31"]),
    (6, "Pneumonias bacterianas",
     ["J13", "J14", "J153", "J154", "J158", "J159", "J181"]),
    (7, "Asma", ["J45", "J46"]),
    (8, "Doenças pulmonares",
     ["J20", "J21", "J40", "J41", "J42", "J43", "J44", "J47"]),
    (9, "Hipertensão", ["I10", "I11"]),
    (10, "Angina", ["I20"]),
    (11, "Insuficiência cardíaca", ["I50", "J81"]),
    (12, "Doenças cerebrovasculares",
     ["I63", "I64", "I65", "I66", "I67", "I69", "G45", "G46"]),
    (13, "Diabetes mellitus", ["E10", "E11", "E12", "E13", "E14"]),
    (14, "Epilepsias", ["G40", "G41"]),
    (15, "Infecção no rim e trato urinário",
     ["N10", "N11", "N12", "N30", "N34", "N390"]),
    (16, "Infecção da pele e tecido subcutâneo",
     ["A46", "L01", "L02", "L03", "L04", "L08"]),
    (17, "Doença inflamatória dos órgãos pélvicos femininos",
     ["N70", "N71", "N72", "N73", "N75", "N76"]),
    (18, "Úlcera gastrointestinal",
     ["K25", "K26", "K27", "K28", "K920", "K921", "K922"]),
    (19, "Doenças relacionadas ao pré-natal e parto",
     ["O23", "A50", "P350"]),
]

MAPA_UF = {
    "11": "RO", "12": "AC", "13": "AM", "14": "RR", "15": "PA",
    "16": "AP", "17": "TO", "21": "MA", "22": "PI", "23": "CE",
    "24": "RN", "25": "PB", "26": "PE", "27": "AL", "28": "SE",
    "29": "BA", "31": "MG", "32": "ES", "33": "RJ", "35": "SP",
    "41": "PR", "42": "SC", "43": "RS", "50": "MS", "51": "MT",
    "52": "GO", "53": "DF",
}

COLUNAS_BASE_FINAL = [
    "id_registro_sih", "fonte_dados", "arquivo_origem", "data_competencia",
    "ano_competencia", "mes_competencia", "numero_aih",
    "sequencia_registro_aih", "tipo_aih", "categoria_aih",
    "conta_como_internacao", "numero_aih_repetido", "uf_zi",
    "codigo_uf_residencia", "uf_residencia", "municipio_residencia_datasus",
    "codigo_uf_internacao", "uf_internacao", "municipio_internacao_datasus",
    "internacao_fora_uf_residencia", "internacao_fora_municipio_residencia",
    "codigo_cnes", "data_internacao", "data_saida", "procedimento_realizado",
    "dias_permanencia", "dias_permanencia_icsap", "valor_total",
    "valor_icsap", "valor_internacao_inicial", "valor_icsap_inicial", "obito",
    "obito_descricao", "obito_icsap", "carater_internacao",
    "carater_internacao_descricao", "complexidade", "complexidade_descricao",
    "cid_principal_original", "cid_principal", "cid_secundario", "icsap",
    "internacao_icsap", "grupo_icsap", "nome_grupo_icsap",
    "regra_cid_icsap", "unidade_idade", "unidade_idade_descricao", "idade",
    "idade_anos", "faixa_etaria", "sexo", "sexo_descricao", "raca_cor",
    "raca_cor_descricao", "nacionalidade",
]

COLUNAS_PAINEL_FINAL = [
    "data_competencia", "ano_competencia", "mes_competencia", "uf_residencia",
    "municipio_residencia_datasus", "codigo_municipio_ibge7",
    "nome_municipio", "quantidade_registros", "total_internacoes",
    "internacoes_icsap", "percentual_internacoes_icsap", "valor_total",
    "valor_icsap", "percentual_financeiro_icsap", "iep", "dias_permanencia",
    "dias_permanencia_icsap", "permanencia_media", "obitos", "obitos_icsap",
    "percentual_obitos",
]


def criar_argumentos() -> argparse.Namespace:
    """Lê as opções de execução do terminal ou da configuração da IDE."""
    parser = argparse.ArgumentParser(
        description="Gera a Base Mestra e o painel municipal do HUBSUS360."
    )
    parser.add_argument(
        "--entrada",
        type=Path,
        help="Caminho do arquivo RDSP em formato .dbc ou .dbf.",
    )
    parser.add_argument(
        "--saida",
        type=Path,
        default=Path("resultados_hubsus360"),
        help="Pasta de saída (padrão: resultados_hubsus360).",
    )
    parser.add_argument(
        "--municipios-ibge",
        type=Path,
        help="CSV opcional de municípios. Sem ele, será consultada a API do IBGE.",
    )
    parser.add_argument(
        "--sem-parquet",
        action="store_true",
        help="Gera apenas CSVs; útil quando o pyarrow não estiver instalado.",
    )
    return parser.parse_args()


def localizar_entrada(caminho_informado: Path | None) -> Path:
    """Usa o caminho informado ou abre uma janela nativa para selecionar o arquivo."""
    if caminho_informado:
        candidatos = [caminho_informado]
    else:
        arquivo_selecionado = selecionar_arquivo_sih()
        if arquivo_selecionado:
            candidatos = [arquivo_selecionado]
        else:
            LOG.warning(
                "Nenhum arquivo foi escolhido. Procurando RDSP2401.dbc no projeto..."
            )
            pasta_script = Path(__file__).resolve().parent
            pasta_atual = Path.cwd()
            candidatos = [
                pasta_atual / "RDSP2401.dbc",
                pasta_atual / "RDSP2401.dbf",
                pasta_atual / "dados" / "RDSP2401.dbc",
                pasta_atual / "dados" / "RDSP2401.dbf",
                pasta_script / "RDSP2401.dbc",
                pasta_script / "RDSP2401.dbf",
                pasta_script / "dados" / "RDSP2401.dbc",
                pasta_script / "dados" / "RDSP2401.dbf",
            ]

    for candidato in candidatos:
        caminho = candidato.expanduser().resolve()
        if caminho.is_file():
            if caminho.suffix.lower() not in {".dbc", ".dbf"}:
                raise ValueError("A entrada deve ter extensão .dbc ou .dbf.")
            return caminho

    procurados = "\n - ".join(str(item) for item in candidatos)
    raise FileNotFoundError(
        "Arquivo de entrada não encontrado. Selecione um DBC/DBF na janela ou "
        "execute com --entrada. Caminhos verificados:\n - " + procurados
    )


def selecionar_arquivo_sih() -> Path | None:
    """Abre o seletor de arquivos do sistema ao executar o programa pela IDE."""
    try:
        import tkinter as tk
        from tkinter import filedialog
    except ImportError:
        LOG.warning("O tkinter não está disponível; o seletor não pôde ser aberto.")
        return None

    janela = None
    try:
        janela = tk.Tk()
        janela.withdraw()
        janela.attributes("-topmost", True)
        caminho = filedialog.askopenfilename(
            parent=janela,
            title="Selecione o arquivo do SIH/SUS",
            filetypes=[
                ("Arquivos DATASUS", "*.dbc *.dbf"),
                ("Arquivo compactado DBC", "*.dbc"),
                ("Arquivo DBF", "*.dbf"),
                ("Todos os arquivos", "*.*"),
            ],
        )
        return Path(caminho) if caminho else None
    except (RuntimeError, tk.TclError):
        LOG.warning("A janela de seleção não está disponível neste ambiente.")
        return None
    finally:
        if janela is not None:
            janela.destroy()


def preparar_dbf(entrada: Path, pasta_temporaria: Path) -> Path:
    """Devolve um DBF pronto para leitura, descompactando o DBC quando necessário."""
    if entrada.suffix.lower() == ".dbf":
        return entrada

    arquivo_dbf = pasta_temporaria / f"{entrada.stem}.dbf"
    LOG.info("Descompactando %s...", entrada.name)
    descompactar_dbc(entrada, arquivo_dbf)
    if not arquivo_dbf.is_file():
        raise RuntimeError("A descompactação terminou sem criar o arquivo DBF.")
    return arquivo_dbf


def ler_dados_sih(arquivo_dbf: Path) -> pd.DataFrame:
    """Lê o DBF do SIH/SUS e confirma a presença dos campos utilizados."""
    try:
        from dbfread import DBF
    except ImportError as erro:
        raise RuntimeError(
            "A biblioteca dbfread não está instalada. Execute: "
            "python -m pip install dbfread"
        ) from erro

    tabela = DBF(
        str(arquivo_dbf),
        encoding="latin-1",
        load=True,
        char_decode_errors="ignore",
    )
    dados = pd.DataFrame(iter(tabela))
    if dados.empty:
        raise ValueError("O arquivo DBF não possui registros.")

    faltantes = [coluna for coluna in COLUNAS_ORIGINAIS if coluna not in dados]
    if faltantes:
        raise ValueError(f"Campos obrigatórios ausentes no DBF: {faltantes}")

    LOG.info("Arquivo lido: %s registros e %s colunas.", len(dados), len(dados.columns))
    return dados


def padronizar_base(dados_brutos: pd.DataFrame) -> pd.DataFrame:
    """Seleciona os campos do projeto e corrige tipos, datas e códigos CID."""
    base = (
        dados_brutos[COLUNAS_ORIGINAIS]
        .copy()
        .rename(columns=RENOMEAR_COLUNAS)
    )

    for coluna in COLUNAS_IDENTIFICADORAS:
        base[coluna] = (
            base[coluna].astype("string").str.strip().replace("", pd.NA)
        )

    for coluna in [
        "ano_competencia", "mes_competencia", "dias_permanencia", "obito", "idade"
    ]:
        base[coluna] = pd.to_numeric(base[coluna], errors="coerce").astype("Int64")

    base["valor_total"] = pd.to_numeric(base["valor_total"], errors="coerce")
    base["data_internacao"] = pd.to_datetime(
        base["data_internacao"], format="%Y%m%d", errors="coerce"
    )
    base["data_saida"] = pd.to_datetime(
        base["data_saida"], format="%Y%m%d", errors="coerce"
    )
    base["cid_principal_original"] = base["cid_principal"]

    for coluna in ["cid_principal", "cid_secundario"]:
        base[coluna] = (
            base[coluna]
            .astype("string")
            .str.upper()
            .str.replace(".", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.strip()
        )

    return base


def classificar_aihs(base: pd.DataFrame) -> pd.DataFrame:
    """Distingue AIHs iniciais das continuidades sem excluir registros."""
    base = base.copy()
    base["categoria_aih"] = np.select(
        [base["tipo_aih"].eq("1"), base["tipo_aih"].eq("5")],
        ["INICIAL", "CONTINUIDADE"],
        default="OUTRO",
    )
    base["conta_como_internacao"] = base["tipo_aih"].eq("1").astype(int)
    base["numero_aih_repetido"] = (
        base.duplicated(subset=["numero_aih"], keep=False).astype(int)
    )
    return base


def criar_referencia_icsap() -> pd.DataFrame:
    """Transforma a Lista Brasileira de ICSAP em uma tabela de regras CID-10."""
    registros = []
    for numero, nome, prefixos in GRUPOS_ICSAP:
        for prefixo in prefixos:
            registros.append(
                {
                    "grupo_icsap": numero,
                    "nome_grupo_icsap": nome,
                    "prefixo_cid": prefixo,
                }
            )
    return pd.DataFrame(registros)


def classificar_icsap(
    base: pd.DataFrame, referencia: pd.DataFrame
) -> pd.DataFrame:
    """Compara o CID principal com os prefixos oficiais das ICSAP."""
    base = base.copy()
    base["icsap"] = 0
    base["grupo_icsap"] = pd.NA
    base["nome_grupo_icsap"] = pd.NA
    base["regra_cid_icsap"] = pd.NA

    regras = (
        referencia
        .assign(tamanho_prefixo=lambda df: df["prefixo_cid"].str.len())
        .sort_values("tamanho_prefixo", ascending=False)
    )
    for regra in regras.itertuples(index=False):
        corresponde = (
            base["cid_principal"].str.startswith(regra.prefixo_cid, na=False)
            & base["grupo_icsap"].isna()
        )
        base.loc[corresponde, "icsap"] = 1
        base.loc[corresponde, "grupo_icsap"] = regra.grupo_icsap
        base.loc[corresponde, "nome_grupo_icsap"] = regra.nome_grupo_icsap
        base.loc[corresponde, "regra_cid_icsap"] = regra.prefixo_cid

    base["grupo_icsap"] = base["grupo_icsap"].astype("Int64")
    return base


def construir_base_mestra(
    base: pd.DataFrame, nome_arquivo_origem: str
) -> pd.DataFrame:
    """Acrescenta rastreabilidade, perfil do paciente e medidas analíticas."""
    base = base.copy()
    base["fonte_dados"] = "SIH/SUS - DATASUS"
    base["arquivo_origem"] = nome_arquivo_origem
    base["data_competencia"] = pd.to_datetime(
        base["ano_competencia"].astype("string")
        + "-"
        + base["mes_competencia"].astype("string").str.zfill(2)
        + "-01",
        errors="coerce",
    )

    base["sequencia_registro_aih"] = (
        base.groupby("numero_aih", dropna=False).cumcount().add(1)
    )
    base["id_registro_sih"] = (
        "SIH-"
        + base["ano_competencia"].astype("string")
        + base["mes_competencia"].astype("string").str.zfill(2)
        + "-"
        + base["numero_aih"].astype("string")
        + "-"
        + base["sequencia_registro_aih"].astype("string").str.zfill(2)
    )

    base = base.sort_values(
        ["data_competencia", "numero_aih", "sequencia_registro_aih"]
    ).reset_index(drop=True)

    # COD_IDADE informa se o valor original está em dias, meses ou anos.
    base["unidade_idade"] = pd.to_numeric(
        base["unidade_idade"], errors="coerce"
    ).astype("Int64")
    base["idade"] = pd.to_numeric(base["idade"], errors="coerce").astype("Int64")
    mapa_unidade = {
        0: "IGNORADA", 2: "DIAS", 3: "MESES", 4: "ANOS", 5: "100 ANOS + IDADE"
    }
    base["unidade_idade_descricao"] = (
        base["unidade_idade"].map(mapa_unidade).fillna("CÓDIGO NÃO DOCUMENTADO")
    )
    idade_anos = pd.Series(pd.NA, index=base.index, dtype="Int64")
    idade_anos.loc[base["unidade_idade"].isin([2, 3])] = 0
    idade_anos.loc[base["unidade_idade"].eq(4)] = base.loc[
        base["unidade_idade"].eq(4), "idade"
    ]
    idade_anos.loc[base["unidade_idade"].eq(5)] = (
        base.loc[base["unidade_idade"].eq(5), "idade"] + 100
    )
    base["idade_anos"] = idade_anos
    base["faixa_etaria"] = pd.cut(
        base["idade_anos"].astype("float"),
        bins=[-1, 0, 4, 9, 14, 19, 29, 39, 49, 59, 69, 79, np.inf],
        labels=[
            "Menor de 1 ano", "1 a 4 anos", "5 a 9 anos", "10 a 14 anos",
            "15 a 19 anos", "20 a 29 anos", "30 a 39 anos", "40 a 49 anos",
            "50 a 59 anos", "60 a 69 anos", "70 a 79 anos", "80 anos ou mais",
        ],
    ).astype("string")

    codigo_sexo = base["sexo"].astype("string").str.strip()
    codigo_raca = base["raca_cor"].astype("string").str.strip().str.zfill(2)
    codigo_carater = (
        base["carater_internacao"].astype("string").str.strip().str.zfill(2)
    )
    codigo_complexidade = (
        base["complexidade"].astype("string").str.strip().str.zfill(2)
    )
    base["sexo_descricao"] = codigo_sexo.map(
        {"0": "IGNORADO", "1": "MASCULINO", "2": "FEMININO",
         "3": "FEMININO", "9": "IGNORADO"}
    ).fillna("CÓDIGO NÃO DOCUMENTADO")
    base["raca_cor_descricao"] = codigo_raca.map(
        {"01": "BRANCA", "02": "PRETA", "03": "PARDA", "04": "AMARELA",
         "05": "INDÍGENA", "99": "SEM INFORMAÇÃO"}
    ).fillna("CÓDIGO NÃO DOCUMENTADO")
    base["carater_internacao_descricao"] = codigo_carater.map(
        {"01": "ELETIVA", "02": "URGÊNCIA",
         "03": "ACIDENTE NO LOCAL DE TRABALHO",
         "04": "ACIDENTE NO TRAJETO PARA O TRABALHO",
         "05": "OUTRO ACIDENTE DE TRÂNSITO",
         "06": "OUTRAS LESÕES OU ENVENENAMENTOS"}
    ).fillna("CÓDIGO NÃO DOCUMENTADO")
    base["complexidade_descricao"] = codigo_complexidade.map(
        {"01": "ATENÇÃO BÁSICA", "02": "MÉDIA COMPLEXIDADE",
         "03": "ALTA COMPLEXIDADE"}
    ).fillna("CÓDIGO NÃO DOCUMENTADO")
    base["obito_descricao"] = base["obito"].map(
        {0: "NÃO", 1: "SIM"}
    ).fillna("CÓDIGO NÃO DOCUMENTADO")

    # As colunas auxiliares evitam refazer condições durante as agregações.
    base["internacao_icsap"] = (
        base["conta_como_internacao"].eq(1) & base["icsap"].eq(1)
    ).astype("int8")
    base["valor_icsap"] = np.where(base["icsap"].eq(1), base["valor_total"], 0.0)
    base["valor_internacao_inicial"] = np.where(
        base["conta_como_internacao"].eq(1), base["valor_total"], 0.0
    )
    base["valor_icsap_inicial"] = np.where(
        base["internacao_icsap"].eq(1), base["valor_total"], 0.0
    )
    base["dias_permanencia_icsap"] = np.where(
        base["icsap"].eq(1), base["dias_permanencia"], 0
    )
    base["obito_icsap"] = np.where(
        base["icsap"].eq(1), base["obito"], 0
    ).astype("int8")

    for coluna in ["municipio_residencia_datasus", "municipio_internacao_datasus"]:
        base[coluna] = (
            base[coluna]
            .astype("string")
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
            .str.zfill(6)
        )
    base["codigo_uf_residencia"] = base["municipio_residencia_datasus"].str[:2]
    base["codigo_uf_internacao"] = base["municipio_internacao_datasus"].str[:2]
    base["uf_residencia"] = base["codigo_uf_residencia"].map(MAPA_UF)
    base["uf_internacao"] = base["codigo_uf_internacao"].map(MAPA_UF)
    base["internacao_fora_uf_residencia"] = (
        base["uf_residencia"] != base["uf_internacao"]
    ).astype("int8")
    base["internacao_fora_municipio_residencia"] = (
        base["municipio_residencia_datasus"]
        != base["municipio_internacao_datasus"]
    ).astype("int8")

    return base[COLUNAS_BASE_FINAL].copy().reset_index(drop=True)


def validar_base_mestra(
    base_final: pd.DataFrame,
    base_tratada: pd.DataFrame,
    referencia_icsap: pd.DataFrame,
) -> pd.DataFrame:
    """Executa as verificações que impedem a exportação de uma base inconsistente."""
    essenciais = [
        "id_registro_sih", "numero_aih", "data_competencia",
        "municipio_residencia_datasus", "municipio_internacao_datasus",
        "cid_principal", "valor_total",
    ]
    problemas = [
        abs(len(base_final) - len(base_tratada)),
        base_final["id_registro_sih"].duplicated().sum(),
        base_final.columns.duplicated().sum(),
        base_final[essenciais].isna().sum().sum(),
        base_final.duplicated().sum(),
        (base_final["data_saida"] < base_final["data_internacao"]).sum(),
        (base_final["valor_total"] < 0).sum(),
        (base_final["dias_permanencia"] < 0).sum(),
        abs(
            base_final["conta_como_internacao"].sum()
            - base_tratada["conta_como_internacao"].sum()
        ),
        abs(
            base_final["internacao_icsap"].sum()
            - (
                base_tratada["conta_como_internacao"].eq(1)
                & base_tratada["icsap"].eq(1)
            ).sum()
        ),
        (base_final["icsap"].eq(1) & base_final["grupo_icsap"].isna()).sum(),
        abs(
            base_final.loc[base_final["icsap"].eq(1), "grupo_icsap"].nunique()
            - referencia_icsap["grupo_icsap"].nunique()
        ),
        ((base_final["idade_anos"] < 0) | (base_final["idade_anos"] > 130)).sum(),
        base_final["uf_internacao"].ne(UF_ANALISE).sum(),
        abs(base_final["valor_total"].sum() - base_tratada["valor_total"].sum()),
    ]
    nomes = [
        "Diferença na quantidade de registros", "Identificadores únicos duplicados",
        "Colunas duplicadas", "Campos essenciais nulos",
        "Linhas completamente duplicadas", "Saída anterior à internação",
        "Valores financeiros negativos", "Permanências negativas",
        "Diferença nas internações iniciais", "Diferença nas internações ICSAP",
        "ICSAP sem grupo", "Diferença na quantidade de grupos ICSAP",
        "Idades fora do intervalo", "UF de internação diferente de SP",
        "Diferença no valor financeiro total",
    ]
    validacao = pd.DataFrame({"verificacao": nomes, "quantidade_problemas": problemas})
    # Contagens e regras estruturais precisam ser exatamente zero. A unica
    # excecao e a reconciliacao financeira: somas de valores em ponto flutuante
    # podem produzir residuos microscopicos, mesmo sem perda de nenhum centavo.
    validacao["tolerancia"] = 0.0
    validacao.loc[
        validacao["verificacao"].eq("Diferença no valor financeiro total"),
        "tolerancia",
    ] = 0.01
    validacao["resultado"] = np.where(
        validacao["quantidade_problemas"].astype(float)
        <= validacao["tolerancia"],
        "APROVADO",
        "REVISAR",
    )
    return validacao


def criar_dataset_painel(
    base_mestra: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Agrega os registros por município e mês e calcula os indicadores."""
    base_sp = base_mestra[base_mestra["uf_residencia"].eq(UF_ANALISE)].copy()
    painel = (
        base_sp
        .groupby(
            ["data_competencia", "ano_competencia", "mes_competencia",
             "uf_residencia", "municipio_residencia_datasus"],
            dropna=False,
        )
        .agg(
            quantidade_registros=("id_registro_sih", "size"),
            total_internacoes=("conta_como_internacao", "sum"),
            internacoes_icsap=("internacao_icsap", "sum"),
            valor_total=("valor_total", "sum"),
            valor_icsap=("valor_icsap", "sum"),
            dias_permanencia=("dias_permanencia", "sum"),
            dias_permanencia_icsap=("dias_permanencia_icsap", "sum"),
            obitos=("obito", "sum"),
            obitos_icsap=("obito_icsap", "sum"),
        )
        .reset_index()
    )

    denominador_internacoes = painel["total_internacoes"].replace(0, np.nan)
    denominador_valor = painel["valor_total"].replace(0, np.nan)
    painel["percentual_internacoes_icsap"] = (
        painel["internacoes_icsap"] / denominador_internacoes * 100
    )
    painel["percentual_financeiro_icsap"] = (
        painel["valor_icsap"] / denominador_valor * 100
    )
    painel["iep"] = 100 - painel["percentual_financeiro_icsap"]
    painel["permanencia_media"] = (
        painel["dias_permanencia"] / denominador_internacoes
    )
    painel["percentual_obitos"] = painel["obitos"] / denominador_internacoes * 100

    decimais = [
        "valor_total", "valor_icsap", "percentual_internacoes_icsap",
        "percentual_financeiro_icsap", "iep", "permanencia_media",
        "percentual_obitos",
    ]
    painel[decimais] = painel[decimais].round(2)
    painel = painel.sort_values(
        ["ano_competencia", "mes_competencia", "municipio_residencia_datasus"]
    ).reset_index(drop=True)

    medidas = [
        ("Quantidade de registros", len(base_sp), painel["quantidade_registros"].sum()),
        ("Total de internações", base_sp["conta_como_internacao"].sum(),
         painel["total_internacoes"].sum()),
        ("Internações ICSAP", base_sp["internacao_icsap"].sum(),
         painel["internacoes_icsap"].sum()),
        ("Valor total", base_sp["valor_total"].sum(), painel["valor_total"].sum()),
        ("Valor ICSAP", base_sp["valor_icsap"].sum(), painel["valor_icsap"].sum()),
        ("Dias de permanência", base_sp["dias_permanencia"].sum(),
         painel["dias_permanencia"].sum()),
        ("Dias de permanência ICSAP", base_sp["dias_permanencia_icsap"].sum(),
         painel["dias_permanencia_icsap"].sum()),
        ("Óbitos", base_sp["obito"].sum(), painel["obitos"].sum()),
        ("Óbitos ICSAP", base_sp["obito_icsap"].sum(), painel["obitos_icsap"].sum()),
    ]
    reconciliacao = pd.DataFrame(
        medidas, columns=["medida", "base_mestra_sp", "dataset_painel"]
    )
    reconciliacao["diferenca"] = (
        reconciliacao["base_mestra_sp"] - reconciliacao["dataset_painel"]
    ).abs()
    reconciliacao["resultado"] = np.where(
        reconciliacao["diferenca"] <= 0.01, "APROVADO", "REVISAR"
    )

    chave = ["data_competencia", "municipio_residencia_datasus"]
    problemas = [
        painel.duplicated(subset=chave).sum(),
        (painel["internacoes_icsap"] > painel["total_internacoes"]).sum(),
        (painel["valor_icsap"] > painel["valor_total"]).sum(),
        (painel["obitos"] > painel["total_internacoes"]).sum(),
        (~painel["percentual_internacoes_icsap"].between(0, 100)).sum(),
        (~painel["percentual_financeiro_icsap"].between(0, 100)).sum(),
        (~painel["iep"].between(0, 100)).sum(),
        (
            (painel["iep"] - (100 - painel["percentual_financeiro_icsap"])).abs()
            > 0.02
        ).sum(),
        painel[["total_internacoes", "internacoes_icsap", "valor_total",
                "valor_icsap", "percentual_internacoes_icsap",
                "percentual_financeiro_icsap", "iep"]].isna().any(axis=1).sum(),
    ]
    nomes = [
        "Municípios e meses duplicados", "ICSAP maior que o total",
        "Valor ICSAP maior que o total", "Óbitos maiores que internações",
        "Percentual ICSAP fora de 0 a 100",
        "Percentual financeiro fora de 0 a 100", "IEP fora de 0 a 100",
        "IEP diferente da fórmula", "Indicadores essenciais nulos",
    ]
    validacao = pd.DataFrame({"verificacao": nomes, "quantidade_problemas": problemas})
    validacao["resultado"] = np.where(
        validacao["quantidade_problemas"].eq(0), "APROVADO", "REVISAR"
    )
    return painel, reconciliacao, validacao


def carregar_dimensao_municipios(caminho_csv: Path | None) -> pd.DataFrame:
    """Lê um CSV local ou consulta os municípios de São Paulo na API do IBGE."""
    if caminho_csv:
        caminho_csv = caminho_csv.expanduser().resolve()
        if not caminho_csv.is_file():
            raise FileNotFoundError(f"CSV de municípios não encontrado: {caminho_csv}")
        municipios = pd.read_csv(caminho_csv, dtype="string")

        if set(["municipio_residencia_datasus", "codigo_municipio_ibge7",
                "nome_municipio", "uf"]).issubset(municipios.columns):
            dimensao = municipios[["municipio_residencia_datasus",
                                   "codigo_municipio_ibge7",
                                   "nome_municipio", "uf"]].copy()
            dimensao = dimensao[dimensao["uf"].str.upper().eq(UF_ANALISE)]
        elif set(["codigo_municipio", "nome_municipio", "uf_sigla"]).issubset(
            municipios.columns
        ):
            municipios = municipios[municipios["uf_sigla"].str.upper().eq(UF_ANALISE)]
            codigo_ibge = municipios["codigo_municipio"].str.replace(r"\.0$", "", regex=True)
            codigo_datasus = (
                municipios["codigo_municipio_datasus"]
                if "codigo_municipio_datasus" in municipios
                else codigo_ibge.str[:6]
            )
            dimensao = pd.DataFrame({
                "municipio_residencia_datasus": codigo_datasus,
                "codigo_municipio_ibge7": codigo_ibge,
                "nome_municipio": municipios["nome_municipio"],
                "uf": UF_ANALISE,
            })
        else:
            raise ValueError(
                "O CSV de municípios não possui as colunas reconhecidas. Consulte "
                "o cabeçalho do script para os formatos aceitos."
            )
    else:
        url = (
            "https://servicodados.ibge.gov.br/api/v1/localidades/"
            f"estados/{UF_ANALISE}/municipios"
        )
        LOG.info("Consultando nomes e códigos municipais na API do IBGE...")
        try:
            with urlopen(url, timeout=60) as resposta:
                conteudo = resposta.read()

                # A API pode enviar JSON compactado mesmo quando a biblioteca
                # padrão não faz a descompressão automaticamente. O byte 0x8b
                # que aparecia no erro pertence ao cabeçalho do formato GZIP.
                resposta_gzip = (
                    resposta.headers.get("Content-Encoding", "").lower() == "gzip"
                    or conteudo.startswith(b"\x1f\x8b")
                )
                if resposta_gzip:
                    conteudo = gzip.decompress(conteudo)

                registros = json.loads(conteudo.decode("utf-8-sig"))
        except (HTTPError, URLError, TimeoutError) as erro:
            raise RuntimeError(
                "Não foi possível consultar a API do IBGE. Baixe o CSV de municípios "
                "e informe-o com --municipios-ibge caminho/do/arquivo.csv."
            ) from erro
        except (UnicodeDecodeError, json.JSONDecodeError, gzip.BadGzipFile) as erro:
            raise RuntimeError(
                "A API do IBGE respondeu em um formato inesperado. Tente novamente "
                "ou informe um CSV local com --municipios-ibge."
            ) from erro
        dimensao = pd.DataFrame([
            {
                "municipio_residencia_datasus": str(item["id"])[:6],
                "codigo_municipio_ibge7": str(item["id"]),
                "nome_municipio": item["nome"],
                "uf": UF_ANALISE,
            }
            for item in registros
        ])

    dimensao["municipio_residencia_datasus"] = (
        dimensao["municipio_residencia_datasus"]
        .astype("string").str.replace(r"\.0$", "", regex=True).str.zfill(6)
    )
    dimensao["codigo_municipio_ibge7"] = (
        dimensao["codigo_municipio_ibge7"]
        .astype("string").str.replace(r"\.0$", "", regex=True).str.zfill(7)
    )
    return dimensao.sort_values("codigo_municipio_ibge7").reset_index(drop=True)


def incluir_municipios(
    painel: pd.DataFrame, dimensao: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Acrescenta nome oficial e código IBGE de sete dígitos ao painel."""
    quantidade_antes = len(painel)
    painel = painel.merge(
        dimensao[["municipio_residencia_datasus", "codigo_municipio_ibge7",
                  "nome_municipio"]],
        on="municipio_residencia_datasus",
        how="left",
        validate="many_to_one",
    )
    problemas = [
        abs(len(dimensao) - 645),
        dimensao["municipio_residencia_datasus"].duplicated().sum(),
        dimensao["codigo_municipio_ibge7"].duplicated().sum(),
        painel["codigo_municipio_ibge7"].isna().sum(),
        painel["nome_municipio"].isna().sum(),
        abs(len(painel) - quantidade_antes),
    ]
    nomes = [
        "Quantidade diferente de 645 municípios", "Código DATASUS duplicado",
        "Código IBGE duplicado", "Painel sem código IBGE",
        "Painel sem nome oficial", "Diferença nas linhas após integração",
    ]
    validacao = pd.DataFrame({"verificacao": nomes, "quantidade_problemas": problemas})
    validacao["resultado"] = np.where(
        validacao["quantidade_problemas"].eq(0), "APROVADO", "REVISAR"
    )
    painel_final = (
        painel[COLUNAS_PAINEL_FINAL]
        .sort_values(["ano_competencia", "mes_competencia", "nome_municipio"])
        .reset_index(drop=True)
    )
    return painel_final, validacao


def exigir_aprovacao(nome: str, validacao: pd.DataFrame) -> None:
    """Interrompe o ETL e mostra exatamente qual regra precisa ser revisada."""
    pendencias = validacao[validacao["resultado"].ne("APROVADO")]
    if not pendencias.empty:
        raise ValueError(
            f"{nome} possui validações pendentes:\n"
            + pendencias.to_string(index=False)
        )


def sufixo_competencia(base: pd.DataFrame) -> str:
    """Cria um identificador de período para os nomes dos arquivos exportados."""
    competencias = base[["ano_competencia", "mes_competencia"]].drop_duplicates()
    if len(competencias) == 1:
        linha = competencias.iloc[0]
        return f"{int(linha['ano_competencia'])}_{int(linha['mes_competencia']):02d}"
    return "multiperiodo"


def exportar_resultados(
    pasta_saida: Path,
    base_mestra: pd.DataFrame,
    painel: pd.DataFrame,
    dimensao: pd.DataFrame,
    reconciliacao: pd.DataFrame,
    validacao_indicadores: pd.DataFrame,
    validacao_municipios: pd.DataFrame,
    gerar_parquet: bool,
) -> Path:
    """Grava CSVs, Parquets opcionais e um ZIP pronto para compartilhamento."""
    pasta_saida = pasta_saida.expanduser().resolve()
    pasta_saida.mkdir(parents=True, exist_ok=True)
    periodo = sufixo_competencia(base_mestra)

    nome_base = f"01_base_mestra_sih_sp_{periodo}"
    nome_painel = f"02_dataset_painel_municipio_mes_sp_{periodo}"
    base_mestra.to_csv(
        pasta_saida / f"{nome_base}.csv", index=False, encoding="utf-8-sig",
        date_format="%Y-%m-%d",
    )
    painel.to_csv(
        pasta_saida / f"{nome_painel}.csv", index=False, encoding="utf-8-sig",
        date_format="%Y-%m-%d",
    )
    dimensao.to_csv(
        pasta_saida / "03_dim_municipio_sp.csv", index=False, encoding="utf-8-sig"
    )
    reconciliacao.to_csv(
        pasta_saida / "04_reconciliacao_painel.csv", index=False,
        encoding="utf-8-sig",
    )
    validacao_indicadores.to_csv(
        pasta_saida / "05_validacao_indicadores.csv", index=False,
        encoding="utf-8-sig",
    )
    validacao_municipios.to_csv(
        pasta_saida / "06_validacao_municipios.csv", index=False,
        encoding="utf-8-sig",
    )

    if gerar_parquet:
        try:
            base_mestra.to_parquet(
                pasta_saida / f"{nome_base}.parquet", index=False, compression="snappy"
            )
            painel.to_parquet(
                pasta_saida / f"{nome_painel}.parquet", index=False,
                compression="snappy",
            )
        except ImportError:
            LOG.warning(
                "Parquets não foram gerados porque o pyarrow não está instalado. "
                "Os CSVs foram preservados normalmente."
            )

    caminho_zip = Path(
        shutil.make_archive(str(pasta_saida), "zip", root_dir=pasta_saida)
    )
    return caminho_zip


def executar_etl(argumentos: argparse.Namespace) -> None:
    """Coordena as etapas do ETL na ordem correta."""
    entrada = localizar_entrada(argumentos.entrada)
    LOG.info("Entrada: %s", entrada)

    with tempfile.TemporaryDirectory(prefix="hubsus360_") as temporario:
        dbf = preparar_dbf(entrada, Path(temporario))
        dados_brutos = ler_dados_sih(dbf)

    LOG.info("1/7 - Padronizando os campos do SIH/SUS...")
    base = padronizar_base(dados_brutos)
    base = classificar_aihs(base)

    LOG.info("2/7 - Aplicando a classificação ICSAP...")
    referencia = criar_referencia_icsap()
    base = classificar_icsap(base, referencia)

    LOG.info("3/7 - Construindo a Base Mestra detalhada...")
    base_mestra = construir_base_mestra(base, entrada.name)
    validacao_base = validar_base_mestra(base_mestra, base, referencia)
    exigir_aprovacao("Base Mestra", validacao_base)

    LOG.info("4/7 - Criando os indicadores municipais...")
    painel, reconciliacao, validacao_indicadores = criar_dataset_painel(base_mestra)
    exigir_aprovacao("Reconciliação do painel", reconciliacao)
    exigir_aprovacao("Indicadores", validacao_indicadores)

    LOG.info("5/7 - Incluindo os nomes oficiais dos municípios...")
    dimensao = carregar_dimensao_municipios(argumentos.municipios_ibge)
    painel_final, validacao_municipios = incluir_municipios(painel, dimensao)
    exigir_aprovacao("Dimensão municipal", validacao_municipios)

    LOG.info("6/7 - Conferindo os resultados finais...")
    if not painel_final["iep"].between(0, 100).all():
        raise ValueError("Foram encontrados valores de IEP fora de 0 a 100.")
    if painel_final.duplicated(
        subset=["data_competencia", "municipio_residencia_datasus"]
    ).any():
        raise ValueError("Foram encontradas linhas municipais duplicadas no painel.")

    LOG.info("7/7 - Exportando os arquivos...")
    caminho_zip = exportar_resultados(
        argumentos.saida,
        base_mestra,
        painel_final,
        dimensao,
        reconciliacao,
        validacao_indicadores,
        validacao_municipios,
        gerar_parquet=not argumentos.sem_parquet,
    )

    LOG.info("ETL concluído e validado.")
    LOG.info("Base Mestra: %s linhas e %s colunas.", *base_mestra.shape)
    LOG.info("Registros ICSAP: %s.", int(base_mestra["icsap"].sum()))
    LOG.info("Internações iniciais ICSAP: %s.", int(base_mestra["internacao_icsap"].sum()))
    LOG.info("Painel: %s linhas e %s colunas.", *painel_final.shape)
    LOG.info("Arquivos: %s", argumentos.saida.expanduser().resolve())
    LOG.info("Pacote ZIP: %s", caminho_zip)


LOG = logging.getLogger("hubsus360.historico")
PADRAO_ARQUIVO_RD = re.compile(
    r"^RDSP(?P<ano>\d{2})(?P<mes>\d{2})(?:\.[^.]+)?$",
    flags=re.IGNORECASE,
)


def criar_argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Gera a Base Mestra historica e o painel municipio-mes do HUBSUS360."
        )
    )
    parser.add_argument(
        "--entrada",
        type=Path,
        nargs="+",
        help="Um ou varios DBC/DBF; sem esta opcao, abre o seletor de arquivos.",
    )
    parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        help=(
            "Anos desejados. Se omitido, detecta pelos nomes dos arquivos."
        ),
    )
    parser.add_argument(
        "--meses",
        type=int,
        nargs="+",
        help=(
            "Meses desejados. Se omitido junto com --anos, usa os 12 meses; "
            "sem filtros, detecta as competencias existentes."
        ),
    )
    parser.add_argument(
        "--saida",
        type=Path,
        default=Path("resultados_hubsus360"),
        help="Pasta de saida dos CSVs.",
    )
    parser.add_argument(
        "--municipios-ibge",
        type=Path,
        help="CSV opcional de municipios; sem ele, consulta a API do IBGE.",
    )
    parser.add_argument(
        "--gerar-zip",
        action="store_true",
        help="Tambem compacta os resultados. Desativado por padrao pelo tamanho.",
    )
    argumentos = parser.parse_args()

    meses_invalidos = sorted(
        {mes for mes in (argumentos.meses or []) if mes not in range(1, 13)}
    )
    if meses_invalidos:
        parser.error(f"Meses invalidos: {meses_invalidos}. Use valores de 1 a 12.")
    if argumentos.meses:
        argumentos.meses = sorted(set(argumentos.meses))
    if argumentos.anos:
        argumentos.anos = sorted(set(argumentos.anos))
    anos_invalidos = [
        ano for ano in (argumentos.anos or []) if ano < 2008 or ano > 2100
    ]
    if anos_invalidos:
        parser.error(f"Anos invalidos: {anos_invalidos}.")
    return argumentos


def selecionar_entrada_sih() -> list[Path]:
    """Permite escolher uma pasta inteira ou arquivos DBC/DBF individuais."""
    try:
        import tkinter as tk
        from tkinter import filedialog, messagebox
    except ImportError:
        LOG.warning("O tkinter nao esta disponivel para selecionar a pasta.")
        return None

    janela = None
    try:
        janela = tk.Tk()
        janela.withdraw()
        janela.attributes("-topmost", True)
        escolher_pasta = messagebox.askyesnocancel(
            title="HUBSUS360 - Forma de selecao",
            message=(
                "Deseja selecionar uma pasta com os arquivos do SIH/SUS?\n\n"
                "Sim: selecionar uma pasta (inclui todas as subpastas).\n"
                "Nao: selecionar um ou varios arquivos DBC/DBF.\n"
                "Cancelar: encerrar a selecao."
            ),
            parent=janela,
        )
        if escolher_pasta is None:
            return []
        if escolher_pasta:
            caminho = filedialog.askdirectory(
                parent=janela,
                title="Selecione a pasta principal dos arquivos RD do SIH/SUS",
            )
            return [Path(caminho)] if caminho else []

        caminhos = filedialog.askopenfilenames(
            parent=janela,
            title="Selecione um ou varios arquivos RD do SIH/SUS",
            filetypes=[
                ("Arquivos DATASUS", "*.dbc *.dbf"),
                ("Arquivos DBC", "*.dbc"),
                ("Arquivos DBF", "*.dbf"),
                ("Todos os arquivos", "*.*"),
            ],
        )
        return [Path(caminho) for caminho in caminhos]
    except (RuntimeError, tk.TclError):
        LOG.warning("A janela de selecao nao esta disponivel neste ambiente.")
        return []
    finally:
        if janela is not None:
            janela.destroy()


def extrair_competencia_nome(caminho: Path) -> tuple[int, int] | None:
    """Extrai ano e mes de nomes como RDSP2401.dbc ou RDSP2401.dbf."""
    correspondencia = PADRAO_ARQUIVO_RD.match(caminho.name)
    if not correspondencia:
        return None
    ano = 2000 + int(correspondencia.group("ano"))
    mes = int(correspondencia.group("mes"))
    if mes not in range(1, 13):
        return None
    return ano, mes


def localizar_arquivos_locais(
    entrada: list[Path] | None,
    anos: list[int] | None,
    meses: list[int] | None,
) -> dict[tuple[int, int], Path]:
    """Detecta as competencias locais e aplica filtros opcionais de periodo."""
    entradas = entrada or selecionar_entrada_sih()
    if not entradas:
        raise FileNotFoundError("Nenhum arquivo DBC/DBF foi selecionado.")

    candidatos = []
    for item in entradas:
        item = item.expanduser().resolve()
        if not item.exists():
            raise FileNotFoundError(f"Entrada local nao encontrada: {item}")
        if item.is_file():
            candidatos.append(item)
        else:
            candidatos.extend(
                caminho
                for caminho in item.rglob("*")
                if caminho.is_file()
                and caminho.suffix.lower() in {".dbc", ".dbf"}
            )
    candidatos = sorted(set(candidatos))

    encontrados: dict[tuple[int, int], list[Path]] = {}
    for caminho in candidatos:
        competencia = extrair_competencia_nome(caminho)
        if competencia is None:
            continue
        ano_arquivo, mes_arquivo = competencia
        if anos and ano_arquivo not in anos:
            continue
        if meses and mes_arquivo not in meses:
            continue
        encontrados.setdefault(competencia, []).append(caminho)

    if not encontrados:
        raise FileNotFoundError(
            "Nenhum arquivo RDSP no formato DBC/DBF foi encontrado para os "
            "filtros informados."
        )

    # Ao informar anos sem meses, a intencao e obter anos completos. Quando
    # anos e meses sao informados, o produto dos filtros tambem e obrigatorio.
    esperadas = None
    if anos:
        meses_exigidos = meses or list(range(1, 13))
        esperadas = {(ano, mes) for ano in anos for mes in meses_exigidos}
    faltantes = sorted((esperadas or set()) - set(encontrados))
    if faltantes:
        nomes = ", ".join(
            f"RDSP{ano % 100:02d}{mes:02d}" for ano, mes in faltantes
        )
        raise FileNotFoundError(
            "Faltam arquivos das competencias solicitadas: " + nomes
        )

    selecionados: dict[tuple[int, int], Path] = {}
    for competencia, arquivos in encontrados.items():
        ano, mes = competencia
        if len(arquivos) > 1:
            # Quando DBC e DBF coexistem, o DBF evita nova descompactacao. Duas
            # copias com a mesma extensao continuam sendo tratadas como ambiguidade.
            dbfs = [arquivo for arquivo in arquivos if arquivo.suffix.lower() == ".dbf"]
            if len(dbfs) == 1:
                selecionados[competencia] = dbfs[0]
                continue
            raise ValueError(
                f"Mais de um arquivo foi encontrado para {ano}-{mes:02d}: "
                + ", ".join(arquivo.name for arquivo in arquivos)
            )
        selecionados[competencia] = arquivos[0]
    return selecionados


def carregar_mes_local(caminho: Path) -> pd.DataFrame:
    """Descompacta, quando necessario, e le um arquivo mensal local."""
    with tempfile.TemporaryDirectory(prefix="hubsus360_historico_") as temporario:
        dbf = preparar_dbf(caminho, Path(temporario))
        return ler_dados_sih(dbf)


def validar_competencia(
    dados: pd.DataFrame,
    ano_esperado: int,
    mes_esperado: int,
) -> None:
    """Impede que um arquivo rotulado incorretamente contamine a serie historica."""
    for coluna in ["ANO_CMPT", "MES_CMPT"]:
        if coluna not in dados.columns:
            raise ValueError(f"Campo obrigatorio ausente: {coluna}")
    anos = set(pd.to_numeric(dados["ANO_CMPT"], errors="coerce").dropna().astype(int))
    meses = set(pd.to_numeric(dados["MES_CMPT"], errors="coerce").dropna().astype(int))
    if anos != {ano_esperado} or meses != {mes_esperado}:
        raise ValueError(
            f"A entrada esperada para {ano_esperado}-{mes_esperado:02d} contem "
            f"ANO_CMPT={sorted(anos)} e MES_CMPT={sorted(meses)}."
        )


def processar_mes(
    dados_brutos: pd.DataFrame,
    nome_origem: str,
    referencia: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Aplica a mesma logica analitica validada no ETL """
    base = padronizar_base(dados_brutos)
    base = classificar_aihs(base)
    base = classificar_icsap(base, referencia)

    base_mestra = construir_base_mestra(base, nome_origem)
    validacao_base = validar_base_mestra(base_mestra, base, referencia)
    exigir_aprovacao("Base Mestra mensal", validacao_base)

    painel, reconciliacao, validacao_indicadores = criar_dataset_painel(
        base_mestra
    )
    exigir_aprovacao("Reconciliacao mensal", reconciliacao)
    exigir_aprovacao("Indicadores mensais", validacao_indicadores)
    return (
        base_mestra,
        painel,
        validacao_base,
        reconciliacao,
        validacao_indicadores,
    )


def adicionar_competencia(
    tabela: pd.DataFrame,
    ano: int,
    mes: int,
) -> pd.DataFrame:
    tabela = tabela.copy()
    tabela.insert(0, "data_competencia", f"{ano}-{mes:02d}-01")
    tabela.insert(1, "ano_competencia", ano)
    tabela.insert(2, "mes_competencia", mes)
    return tabela


def consolidar_reconciliacao(reconciliacoes: list[pd.DataFrame]) -> pd.DataFrame:
    """Acrescenta ao controle mensal uma reconciliacao total do periodo."""
    mensal_consolidada = pd.concat(reconciliacoes, ignore_index=True)
    total = (
        mensal_consolidada.groupby("medida", as_index=False)[
            ["base_mestra_sp", "dataset_painel"]
        ]
        .sum()
    )
    total["diferenca"] = (
        total["base_mestra_sp"] - total["dataset_painel"]
    ).abs()
    total["resultado"] = np.where(
        total["diferenca"] <= 0.01, "APROVADO", "REVISAR"
    )
    total.insert(0, "data_competencia", "TOTAL_DO_PERIODO")
    total.insert(1, "ano_competencia", pd.NA)
    total.insert(2, "mes_competencia", pd.NA)
    return pd.concat([mensal_consolidada, total], ignore_index=True)


def exportar_controles(
    pasta_saida: Path,
    painel: pd.DataFrame,
    dimensao: pd.DataFrame,
    validacao_competencias: pd.DataFrame,
    validacoes_base: list[pd.DataFrame],
    reconciliacoes: list[pd.DataFrame],
    validacoes_indicadores: list[pd.DataFrame],
    validacao_municipios: pd.DataFrame,
    competencias: list[tuple[int, int]],
) -> list[Path]:
    """Exporta painel, dimensao e evidencias de qualidade em CSV."""
    primeira = min(competencias)
    ultima = max(competencias)
    periodo = f"{primeira[0]}_{primeira[1]:02d}_a_{ultima[0]}_{ultima[1]:02d}"
    arquivos = []

    destinos = [
        (
            painel,
            pasta_saida / f"02_dataset_painel_municipio_mes_sp_{periodo}.csv",
        ),
        (dimensao, pasta_saida / "03_dim_municipio_sp.csv"),
        (validacao_competencias, pasta_saida / "04_validacao_competencias.csv"),
        (
            pd.concat(validacoes_base, ignore_index=True),
            pasta_saida / "05_validacao_base_mestra_por_competencia.csv",
        ),
        (
            consolidar_reconciliacao(reconciliacoes),
            pasta_saida / "06_reconciliacao_painel.csv",
        ),
        (
            pd.concat(validacoes_indicadores, ignore_index=True),
            pasta_saida / "07_validacao_indicadores_por_competencia.csv",
        ),
        (validacao_municipios, pasta_saida / "08_validacao_municipios.csv"),
    ]
    for tabela, caminho in destinos:
        tabela.to_csv(caminho, index=False, encoding="utf-8-sig")
        arquivos.append(caminho)
    return arquivos


def executar_etl_historico(argumentos: argparse.Namespace) -> None:
    pasta_saida = argumentos.saida.expanduser().resolve()
    pasta_saida.mkdir(parents=True, exist_ok=True)
    referencia = criar_referencia_icsap()

    arquivos_locais = localizar_arquivos_locais(
        argumentos.entrada, argumentos.anos, argumentos.meses
    )
    competencias_esperadas = sorted(arquivos_locais)

    primeira = min(competencias_esperadas)
    ultima = max(competencias_esperadas)
    periodo = f"{primeira[0]}_{primeira[1]:02d}_a_{ultima[0]}_{ultima[1]:02d}"
    caminho_base_final = (
        pasta_saida / f"01_base_mestra_sih_sp_{periodo}.csv"
    )
    arquivo_temporario = tempfile.NamedTemporaryFile(
        prefix=".base_mestra_em_construcao_",
        suffix=".tmp",
        dir=pasta_saida,
        delete=False,
    )
    caminho_base_temporaria = Path(arquivo_temporario.name)
    arquivo_temporario.close()

    paineis = []
    validacoes_base = []
    reconciliacoes = []
    validacoes_indicadores = []
    controles_competencia = []
    total_linhas = total_icsap = total_internacoes_icsap = 0

    try:
        for indice, (ano, mes) in enumerate(competencias_esperadas, start=1):
            LOG.info(
                "Competencia %s/%s: %s-%02d",
                indice,
                len(competencias_esperadas),
                ano,
                mes,
            )
            caminho = arquivos_locais[(ano, mes)]
            LOG.info("Entrada: %s", caminho)
            dados_brutos = carregar_mes_local(caminho)
            nome_origem = caminho.name

            validar_competencia(dados_brutos, ano, mes)
            (
                base_mestra,
                painel,
                validacao_base,
                reconciliacao,
                validacao_indicadores,
            ) = processar_mes(dados_brutos, nome_origem, referencia)

            base_mestra.to_csv(
                caminho_base_temporaria,
                mode="a",
                header=indice == 1,
                index=False,
                encoding="utf-8-sig" if indice == 1 else "utf-8",
                date_format="%Y-%m-%d",
            )
            paineis.append(painel)
            validacoes_base.append(
                adicionar_competencia(validacao_base, ano, mes)
            )
            reconciliacoes.append(
                adicionar_competencia(reconciliacao, ano, mes)
            )
            validacoes_indicadores.append(
                adicionar_competencia(validacao_indicadores, ano, mes)
            )
            controles_competencia.append(
                {
                    "data_competencia": f"{ano}-{mes:02d}-01",
                    "ano_competencia": ano,
                    "mes_competencia": mes,
                    "arquivo_origem": nome_origem,
                    "quantidade_registros": len(base_mestra),
                    "internacoes_iniciais": int(
                        base_mestra["conta_como_internacao"].sum()
                    ),
                    "internacoes_icsap": int(base_mestra["internacao_icsap"].sum()),
                    "resultado": "APROVADO",
                }
            )
            total_linhas += len(base_mestra)
            total_icsap += int(base_mestra["icsap"].sum())
            total_internacoes_icsap += int(base_mestra["internacao_icsap"].sum())
            del dados_brutos, base_mestra

        validacao_competencias = pd.DataFrame(controles_competencia)
        encontrados = set(
            zip(
                validacao_competencias["ano_competencia"].astype(int),
                validacao_competencias["mes_competencia"].astype(int),
            )
        )
        esperados = set(competencias_esperadas)
        if encontrados != esperados:
            raise ValueError(
                "A carga nao possui exatamente as competencias esperadas. "
                f"Faltantes: {sorted(esperados - encontrados)}; "
                f"extras: {sorted(encontrados - esperados)}."
            )

        painel_historico = pd.concat(paineis, ignore_index=True)
        chave = ["data_competencia", "municipio_residencia_datasus"]
        if painel_historico.duplicated(subset=chave).any():
            raise ValueError("O painel historico possui municipio-mes duplicado.")

        dimensao = carregar_dimensao_municipios(argumentos.municipios_ibge)
        painel_final, validacao_municipios = incluir_municipios(
            painel_historico, dimensao
        )
        exigir_aprovacao("Dimensao municipal", validacao_municipios)

        reconciliacao_final = consolidar_reconciliacao(reconciliacoes)
        exigir_aprovacao("Reconciliacao historica", reconciliacao_final)

        caminho_base_temporaria.replace(caminho_base_final)
        arquivos = [caminho_base_final]
        arquivos.extend(
            exportar_controles(
                pasta_saida,
                painel_final,
                dimensao,
                validacao_competencias,
                validacoes_base,
                reconciliacoes,
                validacoes_indicadores,
                validacao_municipios,
                competencias_esperadas,
            )
        )

        LOG.info("ETL historico concluido e validado.")
        LOG.info("Competencias processadas: %s.", len(competencias_esperadas))
        LOG.info("Base Mestra historica: %s linhas.", total_linhas)
        LOG.info("Registros ICSAP: %s.", total_icsap)
        LOG.info("Internacoes iniciais ICSAP: %s.", total_internacoes_icsap)
        LOG.info("Painel municipio-mes: %s linhas.", len(painel_final))
        LOG.info("Arquivos CSV: %s", pasta_saida)

        if argumentos.gerar_zip:
            caminho_zip = Path(
                shutil.make_archive(str(pasta_saida), "zip", root_dir=pasta_saida)
            )
            LOG.info("Pacote ZIP: %s", caminho_zip)
    except Exception:
        caminho_base_temporaria.unlink(missing_ok=True)
        raise


LOG_REL = logging.getLogger("hubsus360.relacional")

MAPA_FAIXA_ID = {
    "Menor de 1 ano": 1,
    "1 a 4 anos": 2,
    "5 a 9 anos": 3,
    "10 a 14 anos": 4,
    "15 a 19 anos": 5,
    "20 a 29 anos": 6,
    "30 a 39 anos": 7,
    "40 a 49 anos": 8,
    "50 a 59 anos": 9,
    "60 a 69 anos": 10,
    "70 a 79 anos": 11,
    "80 anos ou mais": 12,
}

MAPA_TIPO_LEITO = {
    "01": "CIRURGICO",
    "02": "CLINICO",
    "03": "COMPLEMENTAR",
    "04": "OBSTETRICO",
    "05": "PEDIATRICO",
    "06": "OUTRAS ESPECIALIDADES",
    "07": "HOSPITAL-DIA",
}

COLUNAS_MEDIDAS = [
    "quantidade_registros",
    "total_internacoes",
    "internacoes_icsap",
    "valor_total",
    "valor_icsap",
    "dias_permanencia",
    "dias_permanencia_icsap",
    "obitos",
    "obitos_icsap",
]

COLUNAS_FATO_DETALHE = [
    "id_registro_sih",
    "competencia_id",
    "numero_aih",
    "sequencia_registro_aih",
    "tipo_aih",
    "municipio_residencia_id",
    "municipio_internacao_id",
    "codigo_cnes",
    "data_internacao",
    "data_saida",
    "procedimento_realizado",
    "cid_principal",
    "cid_secundario",
    "grupo_icsap_id",
    "faixa_etaria_id",
    "unidade_idade",
    "idade",
    "idade_anos",
    "sexo_id",
    "raca_cor_id",
    "carater_internacao_id",
    "complexidade_id",
    "conta_como_internacao",
    "internacao_icsap",
    "dias_permanencia",
    "valor_total",
    "obito",
    "internacao_fora_uf",
    "internacao_fora_municipio",
]

PADRAO_CNES = re.compile(
    r"^(?P<grupo>LT|ST)SP(?P<ano>\d{2})(?P<mes>\d{2})(?:\.[^.]+)?$",
    flags=re.IGNORECASE,
)


def criar_argumentos_relacionais() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Transforma SIH/SUS em dimensoes e fatos menores para AED e Oracle."
        )
    )
    parser.add_argument(
        "--sih",
        type=Path,
        nargs="+",
        help=(
            "Pasta(s) ou arquivo(s) RD do SIH/SUS. Sem esta opcao, abre o "
            "seletor grafico."
        ),
    )
    parser.add_argument(
        "--saida",
        type=Path,
        default=Path("resultados_hubsus360_relacional"),
        help="Pasta de saida do modelo relacional.",
    )
    parser.add_argument(
        "--anos",
        type=int,
        nargs="+",
        help=(
            "Filtra um ou mais anos encontrados na pasta, por exemplo: "
            "--anos 2023 ou --anos 2024 2025."
        ),
    )
    parser.add_argument(
        "--meses",
        type=int,
        nargs="+",
        help=(
            "Filtra um ou mais meses (1 a 12), por exemplo: --meses 6 para "
            "junho de qualquer ano selecionado."
        ),
    )
    parser.add_argument(
        "--municipios-ibge",
        type=Path,
        help="CSV opcional da dimensao municipal de Sao Paulo.",
    )
    parser.add_argument(
        "--cnes",
        type=Path,
        help=(
            "Pasta opcional com arquivos historicos LTSP e STSP do CNES. "
            "A integracao e ignorada quando a pasta nao e informada."
        ),
    )
    parser.add_argument(
        "--cnes-estabelecimentos",
        type=Path,
        help=(
            "CSV opcional de estabelecimentos para enriquecer nomes, tipologia "
            "e localizacao. Nao substitui os arquivos historicos de leitos LT."
        ),
    )
    parser.add_argument(
        "--gerar-detalhe",
        action="store_true",
        help=(
            "Gera a fato_internacao_sih particionada por mes. Por padrao, "
            "exporta apenas dimensoes e fatos agregados."
        ),
    )
    parser.add_argument(
        "--sem-fato-cid",
        action="store_true",
        help="Nao gera a fato_cid3_municipio_mes, reduzindo ainda mais o volume.",
    )
    parser.add_argument(
        "--gerar-zip",
        action="store_true",
        help="Compacta a pasta ao final. Desativado por padrao.",
    )
    argumentos = parser.parse_args()
    meses_invalidos = sorted(
        {mes for mes in (argumentos.meses or []) if mes not in range(1, 13)}
    )
    if meses_invalidos:
        parser.error(f"Meses invalidos: {meses_invalidos}. Use valores de 1 a 12.")
    anos_invalidos = [
        ano for ano in (argumentos.anos or []) if ano < 2008 or ano > 2100
    ]
    if anos_invalidos:
        parser.error(f"Anos invalidos: {anos_invalidos}.")
    if argumentos.meses:
        argumentos.meses = sorted(set(argumentos.meses))
    if argumentos.anos:
        argumentos.anos = sorted(set(argumentos.anos))
    return argumentos


def competencia_id(base: pd.DataFrame) -> pd.Series:
    return (
        pd.to_numeric(base["ano_competencia"], errors="raise").astype(int) * 100
        + pd.to_numeric(base["mes_competencia"], errors="raise").astype(int)
    )


def preparar_chaves_dimensionais(base: pd.DataFrame) -> pd.DataFrame:
    preparada = base.copy()
    preparada["competencia_id"] = competencia_id(preparada)
    preparada["municipio_residencia_id"] = preparada[
        "municipio_residencia_datasus"
    ]
    preparada["municipio_internacao_id"] = preparada[
        "municipio_internacao_datasus"
    ]
    preparada["grupo_icsap_id"] = preparada["grupo_icsap"].fillna(0).astype(int)
    preparada["faixa_etaria_id"] = (
        preparada["faixa_etaria"].map(MAPA_FAIXA_ID).fillna(0).astype(int)
    )
    preparada["sexo_id"] = preparada["sexo"].astype("string").fillna("NA")
    preparada["raca_cor_id"] = preparada["raca_cor"].astype("string").fillna("NA")
    preparada["carater_internacao_id"] = (
        preparada["carater_internacao"].astype("string").fillna("NA")
    )
    preparada["complexidade_id"] = (
        preparada["complexidade"].astype("string").fillna("NA")
    )
    preparada["cid3"] = preparada["cid_principal"].astype("string").str[:3]
    return preparada


def criar_fato_detalhe(base: pd.DataFrame) -> pd.DataFrame:
    fato = preparar_chaves_dimensionais(base)
    fato["internacao_fora_uf"] = fato["internacao_fora_uf_residencia"]
    fato["internacao_fora_municipio"] = fato[
        "internacao_fora_municipio_residencia"
    ]
    return fato[COLUNAS_FATO_DETALHE].copy()


def agregar_medidas(base: pd.DataFrame, chaves: list[str]) -> pd.DataFrame:
    return (
        base.groupby(chaves, dropna=False)
        .agg(
            quantidade_registros=("id_registro_sih", "size"),
            total_internacoes=("conta_como_internacao", "sum"),
            internacoes_icsap=("internacao_icsap", "sum"),
            valor_total=("valor_total", "sum"),
            valor_icsap=("valor_icsap", "sum"),
            dias_permanencia=("dias_permanencia", "sum"),
            dias_permanencia_icsap=("dias_permanencia_icsap", "sum"),
            obitos=("obito", "sum"),
            obitos_icsap=("obito_icsap", "sum"),
        )
        .reset_index()
    )


def criar_fato_municipio_mes(base: pd.DataFrame) -> pd.DataFrame:
    residentes_sp = base[base["uf_residencia"].eq("SP")].copy()
    fato = agregar_medidas(
        residentes_sp, ["competencia_id", "municipio_residencia_id"]
    )
    denom_internacoes = fato["total_internacoes"].replace(0, np.nan)
    denom_valor = fato["valor_total"].replace(0, np.nan)
    fato["percentual_internacoes_icsap"] = (
        fato["internacoes_icsap"] / denom_internacoes * 100
    ).fillna(0)
    fato["percentual_financeiro_icsap"] = (
        fato["valor_icsap"] / denom_valor * 100
    ).fillna(0)
    fato["iep"] = 100 - fato["percentual_financeiro_icsap"]
    fato["permanencia_media"] = (
        fato["dias_permanencia"] / denom_internacoes
    ).fillna(0)
    fato["percentual_obitos"] = (
        fato["obitos"] / denom_internacoes * 100
    ).fillna(0)
    decimais = [
        "valor_total", "valor_icsap", "percentual_internacoes_icsap",
        "percentual_financeiro_icsap", "iep", "permanencia_media",
        "percentual_obitos",
    ]
    fato[decimais] = fato[decimais].round(2)
    return fato


def criar_fato_perfil_mes(base: pd.DataFrame) -> pd.DataFrame:
    residentes_sp = base[base["uf_residencia"].eq("SP")].copy()
    chaves = [
        "competencia_id",
        "municipio_residencia_id",
        "faixa_etaria_id",
        "sexo_id",
        "raca_cor_id",
        "grupo_icsap_id",
    ]
    return agregar_medidas(residentes_sp, chaves)


def criar_fato_hospital_mes(base: pd.DataFrame) -> pd.DataFrame:
    chaves = ["competencia_id", "codigo_cnes", "municipio_internacao_id"]
    fato = agregar_medidas(base, chaves)
    base_movimentos = base.assign(
        internacao_inicial_fora_municipio=(
            base["conta_como_internacao"]
            * base["internacao_fora_municipio_residencia"]
        ),
        internacao_inicial_fora_uf=(
            base["conta_como_internacao"]
            * base["internacao_fora_uf_residencia"]
        ),
    )
    movimentos = (
        base_movimentos.groupby(chaves, dropna=False)
        .agg(
            internacoes_residentes_fora_municipio=(
                "internacao_inicial_fora_municipio", "sum"
            ),
            internacoes_residentes_fora_uf=(
                "internacao_inicial_fora_uf", "sum"
            ),
        )
        .reset_index()
    )
    return fato.merge(movimentos, on=chaves, how="left", validate="one_to_one")


def criar_fato_fluxo_mes(base: pd.DataFrame) -> pd.DataFrame:
    residentes_sp = base[base["uf_residencia"].eq("SP")].copy()
    chaves = [
        "competencia_id",
        "municipio_residencia_id",
        "municipio_internacao_id",
    ]
    return agregar_medidas(residentes_sp, chaves)


def criar_fato_cid3_mes(base: pd.DataFrame) -> pd.DataFrame:
    residentes_sp = base[base["uf_residencia"].eq("SP")].copy()
    chaves = [
        "competencia_id",
        "municipio_residencia_id",
        "cid3",
        "grupo_icsap_id",
    ]
    return agregar_medidas(residentes_sp, chaves)


def criar_dim_tempo(competencias: list[tuple[int, int]]) -> pd.DataFrame:
    registros = []
    nomes = [
        "Janeiro", "Fevereiro", "Marco", "Abril", "Maio", "Junho",
        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
    ]
    for ano, mes in sorted(set(competencias)):
        registros.append(
            {
                "competencia_id": ano * 100 + mes,
                "data_competencia": f"{ano}-{mes:02d}-01",
                "ano": ano,
                "mes": mes,
                "nome_mes": nomes[mes - 1],
                "trimestre": (mes - 1) // 3 + 1,
                "semestre": 1 if mes <= 6 else 2,
                "dias_no_mes": calendar.monthrange(ano, mes)[1],
            }
        )
    return pd.DataFrame(registros)


def criar_dim_icsap(referencia: pd.DataFrame) -> pd.DataFrame:
    grupos = (
        referencia[["grupo_icsap", "nome_grupo_icsap"]]
        .drop_duplicates()
        .rename(columns={"grupo_icsap": "grupo_icsap_id"})
    )
    grupos["icsap"] = 1
    nao_icsap = pd.DataFrame(
        [{"grupo_icsap_id": 0, "nome_grupo_icsap": "NAO ICSAP", "icsap": 0}]
    )
    return pd.concat([nao_icsap, grupos], ignore_index=True).sort_values(
        "grupo_icsap_id"
    )


def criar_dim_faixa_etaria() -> pd.DataFrame:
    registros = [
        {"faixa_etaria_id": 0, "faixa_etaria": "NAO INFORMADA", "ordem": 0,
         "idade_minima_anos": pd.NA, "idade_maxima_anos": pd.NA}
    ]
    limites = {
        "Menor de 1 ano": (0, 0), "1 a 4 anos": (1, 4),
        "5 a 9 anos": (5, 9), "10 a 14 anos": (10, 14),
        "15 a 19 anos": (15, 19), "20 a 29 anos": (20, 29),
        "30 a 39 anos": (30, 39), "40 a 49 anos": (40, 49),
        "50 a 59 anos": (50, 59), "60 a 69 anos": (60, 69),
        "70 a 79 anos": (70, 79), "80 anos ou mais": (80, pd.NA),
    }
    for nome, identificador in MAPA_FAIXA_ID.items():
        minimo, maximo = limites[nome]
        registros.append(
            {
                "faixa_etaria_id": identificador,
                "faixa_etaria": nome,
                "ordem": identificador,
                "idade_minima_anos": minimo,
                "idade_maxima_anos": maximo,
            }
        )
    return pd.DataFrame(registros)


def criar_dimensoes_dominios() -> dict[str, pd.DataFrame]:
    return {
        "dim_sexo": pd.DataFrame(
            [
                ("0", "IGNORADO"), ("1", "MASCULINO"),
                ("2", "FEMININO"), ("3", "FEMININO"),
                ("9", "IGNORADO"), ("NA", "NAO INFORMADO"),
            ], columns=["sexo_id", "sexo_descricao"]
        ),
        "dim_raca_cor": pd.DataFrame(
            [
                ("01", "BRANCA"), ("02", "PRETA"), ("03", "PARDA"),
                ("04", "AMARELA"), ("05", "INDIGENA"),
                ("99", "SEM INFORMACAO"), ("NA", "NAO INFORMADO"),
            ], columns=["raca_cor_id", "raca_cor_descricao"]
        ),
        "dim_carater_internacao": pd.DataFrame(
            [
                ("01", "ELETIVA"), ("02", "URGENCIA"),
                ("03", "ACIDENTE NO LOCAL DE TRABALHO"),
                ("04", "ACIDENTE NO TRAJETO PARA O TRABALHO"),
                ("05", "OUTRO ACIDENTE DE TRANSITO"),
                ("06", "OUTRAS LESOES OU ENVENENAMENTOS"),
                ("NA", "NAO INFORMADO"),
            ], columns=["carater_internacao_id", "carater_internacao_descricao"]
        ),
        "dim_complexidade": pd.DataFrame(
            [
                ("01", "ATENCAO BASICA"),
                ("02", "MEDIA COMPLEXIDADE"),
                ("03", "ALTA COMPLEXIDADE"),
                ("NA", "NAO INFORMADO"),
            ], columns=["complexidade_id", "complexidade_descricao"]
        ),
    }


def criar_dimensoes_leitos(
    fato_leitos: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    if fato_leitos.empty:
        return {}
    tipos_observados = sorted(
        fato_leitos["tipo_leito_id"].dropna().astype("string").unique()
    )
    dim_tipo = pd.DataFrame(
        {
            "tipo_leito_id": tipos_observados,
            "tipo_leito_descricao": [
                MAPA_TIPO_LEITO.get(codigo, "CODIGO NAO DOCUMENTADO")
                for codigo in tipos_observados
            ],
        }
    )
    dim_especialidade = (
        fato_leitos[["tipo_leito_id", "especialidade_leito_id"]]
        .drop_duplicates()
        .sort_values(["tipo_leito_id", "especialidade_leito_id"])
        .reset_index(drop=True)
    )
    dim_especialidade["especialidade_leito_descricao"] = pd.NA
    return {
        "dim_tipo_leito": dim_tipo,
        "dim_especialidade_leito": dim_especialidade,
    }


def completar_municipio_mes(
    fato: pd.DataFrame,
    dim_tempo: pd.DataFrame,
    dim_municipio: pd.DataFrame,
) -> pd.DataFrame:
    grade = (
        dim_tempo[["competencia_id"]]
        .assign(chave=1)
        .merge(
            dim_municipio[["municipio_residencia_datasus"]].assign(chave=1),
            on="chave",
        )
        .drop(columns="chave")
        .rename(columns={"municipio_residencia_datasus": "municipio_residencia_id"})
    )
    completa = grade.merge(
        fato,
        on=["competencia_id", "municipio_residencia_id"],
        how="left",
        validate="one_to_one",
    )
    numericas = [coluna for coluna in completa.columns if coluna not in {
        "competencia_id", "municipio_residencia_id"
    }]
    completa[numericas] = completa[numericas].fillna(0)
    return completa.sort_values(
        ["competencia_id", "municipio_residencia_id"]
    ).reset_index(drop=True)


def validar_reconciliacao_fato(
    nome_fato: str,
    base_referencia: pd.DataFrame,
    fato: pd.DataFrame,
    competencia: int,
) -> pd.DataFrame:
    esperado = {
        "quantidade_registros": len(base_referencia),
        "total_internacoes": base_referencia["conta_como_internacao"].sum(),
        "internacoes_icsap": base_referencia["internacao_icsap"].sum(),
        "valor_total": base_referencia["valor_total"].sum(),
        "valor_icsap": base_referencia["valor_icsap"].sum(),
        "dias_permanencia": base_referencia["dias_permanencia"].sum(),
        "dias_permanencia_icsap": base_referencia[
            "dias_permanencia_icsap"
        ].sum(),
        "obitos": base_referencia["obito"].sum(),
        "obitos_icsap": base_referencia["obito_icsap"].sum(),
    }
    linhas = []
    for medida, valor_esperado in esperado.items():
        valor_fato = fato[medida].sum()
        diferenca = abs(float(valor_esperado) - float(valor_fato))
        tolerancia = 0.01 if medida in {"valor_total", "valor_icsap"} else 0.0
        linhas.append(
            {
                "competencia_id": competencia,
                "fato": nome_fato,
                "medida": medida,
                "valor_base": valor_esperado,
                "valor_fato": valor_fato,
                "diferenca": diferenca,
                "tolerancia": tolerancia,
                "resultado": "APROVADO" if diferenca <= tolerancia else "REVISAR",
            }
        )
    return pd.DataFrame(linhas)


def extrair_competencia_cnes(caminho: Path) -> tuple[str, int, int] | None:
    correspondencia = PADRAO_CNES.match(caminho.name)
    if not correspondencia:
        return None
    mes = int(correspondencia.group("mes"))
    if mes not in range(1, 13):
        return None
    return (
        correspondencia.group("grupo").upper(),
        2000 + int(correspondencia.group("ano")),
        mes,
    )


def localizar_arquivos_cnes(pasta: Path) -> dict[tuple[str, int, int], Path]:
    pasta = pasta.expanduser().resolve()
    if not pasta.is_dir():
        raise FileNotFoundError(f"Pasta CNES nao encontrada: {pasta}")
    encontrados = {}
    for caminho in pasta.rglob("*"):
        if not caminho.is_file() or caminho.suffix.lower() not in {".dbc", ".dbf"}:
            continue
        chave = extrair_competencia_cnes(caminho)
        if chave is None:
            continue
        if chave in encontrados:
            raise ValueError(
                f"Mais de um arquivo CNES para {chave}: "
                f"{encontrados[chave].name} e {caminho.name}"
            )
        encontrados[chave] = caminho
    return encontrados


def escolher_coluna(
    dados: pd.DataFrame,
    aliases: list[str],
    descricao: str,
    obrigatoria: bool = True,
) -> str | None:
    mapa = {str(coluna).upper(): coluna for coluna in dados.columns}
    for alias in aliases:
        if alias.upper() in mapa:
            return mapa[alias.upper()]
    if obrigatoria:
        raise ValueError(
            f"Campo CNES de {descricao} nao encontrado. Aliases aceitos: "
            f"{aliases}. Colunas recebidas: {list(dados.columns)}"
        )
    return None


def ler_dbf_generico(caminho: Path) -> pd.DataFrame:
    try:
        from dbfread import DBF
    except ImportError as erro:
        raise RuntimeError(
            "A biblioteca dbfread nao esta instalada. Execute: "
            "python -m pip install dbfread"
        ) from erro
    tabela = DBF(
        str(caminho), encoding="latin-1", load=True, char_decode_errors="ignore"
    )
    dados = pd.DataFrame(iter(tabela))
    if dados.empty:
        raise ValueError(f"O DBF CNES esta vazio: {caminho.name}")
    return dados


def carregar_cnes_generico(caminho: Path) -> pd.DataFrame:
    import tempfile

    with tempfile.TemporaryDirectory(prefix="hubsus360_cnes_") as temporario:
        dbf = preparar_dbf(caminho, Path(temporario))
        return ler_dbf_generico(dbf)


def tratar_leitos_cnes(dados: pd.DataFrame, ano: int, mes: int) -> pd.DataFrame:
    cnes = escolher_coluna(dados, ["CNES", "CO_CNES", "COD_CNES"], "CNES")
    municipio = escolher_coluna(
        dados,
        ["CODUFMUN", "CODUFMN", "CODMUN", "CO_MUNICIPIO_GESTOR"],
        "municipio",
        obrigatoria=False,
    )
    tipo = escolher_coluna(
        dados, ["TP_LEITO", "TIPO_LEITO", "CO_TIPO_LEITO"], "tipo de leito"
    )
    especialidade = escolher_coluna(
        dados,
        ["ESPEC", "ESPECIALID", "CODLEITO", "CO_LEITO"],
        "especialidade de leito",
    )
    existentes = escolher_coluna(
        dados,
        ["LEITOS", "QT_EXIST", "QT_LEITOS", "QT_EXISTENTE"],
        "leitos existentes",
    )
    sus = escolher_coluna(
        dados, ["LEIT_SUS", "LEITOS_SUS", "QT_SUS"], "leitos SUS"
    )
    fato = pd.DataFrame(
        {
            "competencia_id": ano * 100 + mes,
            "codigo_cnes": dados[cnes],
            "municipio_internacao_id": dados[municipio] if municipio else pd.NA,
            "tipo_leito_id": dados[tipo],
            "especialidade_leito_id": dados[especialidade],
            "leitos_existentes": dados[existentes],
            "leitos_sus": dados[sus],
        }
    )
    for coluna, tamanho in [
        ("codigo_cnes", 7), ("municipio_internacao_id", 6),
        ("tipo_leito_id", 2), ("especialidade_leito_id", 2),
    ]:
        fato[coluna] = (
            fato[coluna].astype("string").str.strip().str.replace(r"\.0$", "", regex=True)
        )
        if coluna != "municipio_internacao_id" or fato[coluna].notna().any():
            fato[coluna] = fato[coluna].str.zfill(tamanho)
    for coluna in ["leitos_existentes", "leitos_sus"]:
        fato[coluna] = pd.to_numeric(fato[coluna], errors="coerce").fillna(0).astype(int)
    chaves = [
        "competencia_id", "codigo_cnes", "municipio_internacao_id",
        "tipo_leito_id", "especialidade_leito_id",
    ]
    return (
        fato.groupby(chaves, dropna=False)[["leitos_existentes", "leitos_sus"]]
        .sum()
        .reset_index()
    )


def tratar_estabelecimentos_cnes(
    dados: pd.DataFrame, ano: int, mes: int
) -> pd.DataFrame:
    cnes = escolher_coluna(dados, ["CNES", "CO_CNES", "COD_CNES"], "CNES")
    municipio = escolher_coluna(
        dados,
        ["CODUFMUN", "CODUFMN", "CODMUN", "CO_MUNICIPIO_GESTOR"],
        "municipio",
    )
    aliases_opcionais = {
        "nome_estabelecimento": ["NOME_FANT", "NO_FANTASIA", "NOME_FANTASIA"],
        "tipo_unidade_id": ["TP_UNID", "CO_TIPO_UNIDADE", "COD_TIPO_UNIDADE"],
        "vinculo_sus_id": ["VINC_SUS", "VINCULO_SUS"],
        "tipo_gestao_id": ["TPGESTAO", "TP_GESTAO", "TIPO_GESTAO"],
        "esfera_administrativa_id": ["ESFERA_A", "ESFERA_ADMIN"],
        "natureza_juridica_id": ["NATUREZA", "NAT_JUR", "CO_NATUREZA_JUR"],
    }
    saida = pd.DataFrame(
        {
            "competencia_id": ano * 100 + mes,
            "codigo_cnes": dados[cnes],
            "municipio_internacao_id": dados[municipio],
        }
    )
    for destino, aliases in aliases_opcionais.items():
        origem = escolher_coluna(
            dados, aliases, destino, obrigatoria=False
        )
        saida[destino] = dados[origem] if origem else pd.NA
    saida["codigo_cnes"] = (
        saida["codigo_cnes"].astype("string").str.strip()
        .str.replace(r"\.0$", "", regex=True).str.zfill(7)
    )
    saida["municipio_internacao_id"] = (
        saida["municipio_internacao_id"].astype("string").str.strip()
        .str.replace(r"\.0$", "", regex=True).str.zfill(6)
    )
    return saida.drop_duplicates(
        subset=["competencia_id", "codigo_cnes"], keep="last"
    )


def carregar_snapshot_estabelecimentos(caminho: Path) -> pd.DataFrame:
    caminho = caminho.expanduser().resolve()
    if not caminho.is_file():
        raise FileNotFoundError(f"CSV CNES nao encontrado: {caminho}")
    dados = pd.read_csv(caminho, dtype="string", low_memory=False)
    obrigatorias = {"codigo_cnes", "codigo_municipio"}
    if not obrigatorias.issubset(dados.columns):
        raise ValueError(
            f"Snapshot CNES precisa das colunas {sorted(obrigatorias)}."
        )
    colunas = {
        "codigo_cnes": "codigo_cnes",
        "codigo_municipio": "municipio_internacao_id",
        "nome_fantasia": "nome_estabelecimento",
        "codigo_tipo_unidade": "tipo_unidade_id",
        "tipo_gestao": "tipo_gestao_id",
        "descricao_esfera_administrativa": "esfera_administrativa_descricao",
        "latitude_estabelecimento_decimo_grau": "latitude",
        "longitude_estabelecimento_decimo_grau": "longitude",
        "endereco_estabelecimento": "endereco",
        "numero_estabelecimento": "numero_endereco",
        "bairro_estabelecimento": "bairro",
    }
    presentes = [coluna for coluna in colunas if coluna in dados.columns]
    snapshot = dados[presentes].rename(columns=colunas).copy()
    snapshot["codigo_cnes"] = (
        snapshot["codigo_cnes"].str.replace(r"\.0$", "", regex=True).str.zfill(7)
    )
    snapshot["municipio_internacao_id"] = (
        snapshot["municipio_internacao_id"].str.replace(r"\.0$", "", regex=True)
        .str.zfill(6)
    )
    return snapshot.drop_duplicates("codigo_cnes", keep="last")


def processar_cnes_historico(
    pasta_cnes: Path,
    competencias_sih: list[tuple[int, int]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    arquivos = localizar_arquivos_cnes(pasta_cnes)
    fatos_leito = []
    historico_estabelecimentos = []
    validacoes = []
    for ano, mes in competencias_sih:
        for grupo in ["LT", "ST"]:
            chave = (grupo, ano, mes)
            if chave not in arquivos:
                validacoes.append(
                    {
                        "competencia_id": ano * 100 + mes,
                        "grupo_cnes": grupo,
                        "arquivo": pd.NA,
                        "linhas": 0,
                        "resultado": "AUSENTE",
                    }
                )
                continue
            caminho = arquivos[chave]
            LOG_REL.info("Processando CNES %s...", caminho.name)
            dados = carregar_cnes_generico(caminho)
            if grupo == "LT":
                tratada = tratar_leitos_cnes(dados, ano, mes)
                fatos_leito.append(tratada)
            else:
                tratada = tratar_estabelecimentos_cnes(dados, ano, mes)
                historico_estabelecimentos.append(tratada)
            validacoes.append(
                {
                    "competencia_id": ano * 100 + mes,
                    "grupo_cnes": grupo,
                    "arquivo": caminho.name,
                    "linhas": len(tratada),
                    "resultado": "APROVADO",
                }
            )
    leitos = pd.concat(fatos_leito, ignore_index=True) if fatos_leito else pd.DataFrame()
    estabelecimentos = (
        pd.concat(historico_estabelecimentos, ignore_index=True)
        if historico_estabelecimentos else pd.DataFrame()
    )
    return leitos, estabelecimentos, pd.DataFrame(validacoes)


def criar_fato_capacidade(
    fato_hospital: pd.DataFrame,
    fato_leitos: pd.DataFrame,
    dim_tempo: pd.DataFrame,
) -> pd.DataFrame:
    if fato_leitos.empty:
        return pd.DataFrame()
    totais_leito = (
        fato_leitos.groupby(["competencia_id", "codigo_cnes"], dropna=False)
        [["leitos_existentes", "leitos_sus"]]
        .sum()
        .reset_index()
    )
    capacidade = fato_hospital.merge(
        totais_leito,
        on=["competencia_id", "codigo_cnes"],
        how="left",
        validate="many_to_one",
    ).merge(
        dim_tempo[["competencia_id", "dias_no_mes"]],
        on="competencia_id",
        how="left",
        validate="many_to_one",
    )
    capacidade["cobertura_cnes_leitos"] = capacidade["leitos_sus"].notna().astype(int)
    capacidade[["leitos_existentes", "leitos_sus"]] = capacidade[
        ["leitos_existentes", "leitos_sus"]
    ].fillna(0)
    capacidade["dias_capacidade_sus"] = (
        capacidade["leitos_sus"] * capacidade["dias_no_mes"]
    )
    denominador = capacidade["dias_capacidade_sus"].replace(0, np.nan)
    capacidade["pressao_estimada_pct"] = (
        capacidade["dias_permanencia"] / denominador * 100
    ).round(2)
    return capacidade


def criar_dim_estabelecimento(
    pares_sih: pd.DataFrame,
    historico_cnes: pd.DataFrame,
    snapshot: pd.DataFrame,
) -> pd.DataFrame:
    dim = (
        pares_sih.sort_values("competencia_id")
        .drop_duplicates("codigo_cnes", keep="last")
        [["codigo_cnes", "municipio_internacao_id"]]
    )
    if not historico_cnes.empty:
        ultimo = (
            historico_cnes.sort_values("competencia_id")
            .drop_duplicates("codigo_cnes", keep="last")
            .drop(columns="competencia_id")
        )
        dim = dim.merge(
            ultimo,
            on="codigo_cnes",
            how="outer",
            suffixes=("_sih", "_cnes"),
            validate="one_to_one",
        )
        if "municipio_internacao_id_sih" in dim.columns:
            dim["municipio_internacao_id"] = dim[
                "municipio_internacao_id_cnes"
            ].fillna(dim["municipio_internacao_id_sih"])
            dim = dim.drop(
                columns=["municipio_internacao_id_sih", "municipio_internacao_id_cnes"]
            )
    if not snapshot.empty:
        dim = dim.merge(
            snapshot,
            on="codigo_cnes",
            how="left",
            suffixes=("", "_snapshot"),
            validate="one_to_one",
        )
        for coluna in snapshot.columns:
            if coluna == "codigo_cnes":
                continue
            coluna_snapshot = f"{coluna}_snapshot"
            if coluna_snapshot not in dim.columns:
                continue
            if coluna in dim.columns:
                dim[coluna] = dim[coluna].fillna(dim[coluna_snapshot])
                dim = dim.drop(columns=coluna_snapshot)
            else:
                dim = dim.rename(columns={coluna_snapshot: coluna})
    return dim.sort_values("codigo_cnes").reset_index(drop=True)


def exportar_csv(tabela: pd.DataFrame, caminho: Path) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    tabela.to_csv(caminho, index=False, encoding="utf-8-sig", date_format="%Y-%m-%d")


def executar_etl_relacional(argumentos: argparse.Namespace) -> None:
    entradas = localizar_arquivos_locais(
        argumentos.sih, argumentos.anos, argumentos.meses
    )
    competencias = sorted(entradas)
    pasta_saida = argumentos.saida.expanduser().resolve()
    pasta_dim = pasta_saida / "dimensoes"
    pasta_fatos = pasta_saida / "fatos"
    pasta_detalhe = pasta_saida / "detalhe_sih"
    pasta_validacoes = pasta_saida / "validacoes"
    for pasta in [pasta_dim, pasta_fatos, pasta_validacoes]:
        pasta.mkdir(parents=True, exist_ok=True)

    referencia = criar_referencia_icsap()
    dim_municipio = carregar_dimensao_municipios(argumentos.municipios_ibge)
    dim_tempo = criar_dim_tempo(competencias)

    fatos_municipio = []
    fatos_perfil = []
    fatos_hospital = []
    fatos_fluxo = []
    fatos_cid = []
    pares_estabelecimento = []
    validacoes_base = []
    reconciliacoes = []
    resumo_competencias = []

    for indice, (ano, mes) in enumerate(competencias, start=1):
        caminho = entradas[(ano, mes)]
        LOG_REL.info(
            "SIH %s/%s - processando %s...", indice, len(competencias), caminho.name
        )
        dados_brutos = carregar_mes_local(caminho)
        validar_competencia(dados_brutos, ano, mes)
        base = padronizar_base(dados_brutos)
        base = classificar_aihs(base)
        base = classificar_icsap(base, referencia)
        mestra = construir_base_mestra(base, caminho.name)
        validacao = validar_base_mestra(mestra, base, referencia)
        exigir_aprovacao("Base SIH mensal", validacao)
        preparada = preparar_chaves_dimensionais(mestra)
        comp_id = ano * 100 + mes

        fato_municipio = criar_fato_municipio_mes(preparada)
        fato_perfil = criar_fato_perfil_mes(preparada)
        fato_hospital = criar_fato_hospital_mes(preparada)
        fato_fluxo = criar_fato_fluxo_mes(preparada)
        fato_cid = (
            criar_fato_cid3_mes(preparada) if not argumentos.sem_fato_cid else None
        )

        base_sp = preparada[preparada["uf_residencia"].eq("SP")]
        reconciliacoes.extend(
            [
                validar_reconciliacao_fato(
                    "fato_municipio_mes", base_sp, fato_municipio, comp_id
                ),
                validar_reconciliacao_fato(
                    "fato_perfil_municipio_mes", base_sp, fato_perfil, comp_id
                ),
                validar_reconciliacao_fato(
                    "fato_hospital_mes", preparada, fato_hospital, comp_id
                ),
                validar_reconciliacao_fato(
                    "fato_fluxo_municipio_mes", base_sp, fato_fluxo, comp_id
                ),
            ]
        )
        if fato_cid is not None:
            reconciliacoes.append(
                validar_reconciliacao_fato(
                    "fato_cid3_municipio_mes", base_sp, fato_cid, comp_id
                )
            )

        if argumentos.gerar_detalhe:
            detalhe = criar_fato_detalhe(preparada)
            destino = (
                pasta_detalhe
                / f"ano={ano}"
                / f"mes={mes:02d}"
                / f"fato_internacao_sih_{ano}_{mes:02d}.csv"
            )
            exportar_csv(detalhe, destino)

        fatos_municipio.append(fato_municipio)
        fatos_perfil.append(fato_perfil)
        fatos_hospital.append(fato_hospital)
        fatos_fluxo.append(fato_fluxo)
        if fato_cid is not None:
            fatos_cid.append(fato_cid)
        pares_estabelecimento.append(
            preparada[
                ["competencia_id", "codigo_cnes", "municipio_internacao_id"]
            ].drop_duplicates()
        )
        validacao.insert(0, "competencia_id", comp_id)
        validacoes_base.append(validacao)
        resumo_competencias.append(
            {
                "competencia_id": comp_id,
                "arquivo_sih": caminho.name,
                "registros_detalhe": len(preparada),
                "linhas_fato_municipio": len(fato_municipio),
                "linhas_fato_perfil": len(fato_perfil),
                "linhas_fato_hospital": len(fato_hospital),
                "linhas_fato_fluxo": len(fato_fluxo),
                "linhas_fato_cid3": len(fato_cid) if fato_cid is not None else 0,
                "resultado": "APROVADO",
            }
        )
        del dados_brutos, base, mestra, preparada

    fato_municipio = pd.concat(fatos_municipio, ignore_index=True)
    fato_municipio = completar_municipio_mes(
        fato_municipio, dim_tempo, dim_municipio
    )
    fato_perfil = pd.concat(fatos_perfil, ignore_index=True)
    fato_hospital = pd.concat(fatos_hospital, ignore_index=True)
    fato_fluxo = pd.concat(fatos_fluxo, ignore_index=True)
    fato_cid = pd.concat(fatos_cid, ignore_index=True) if fatos_cid else pd.DataFrame()
    pares_sih = pd.concat(pares_estabelecimento, ignore_index=True)

    validacao_reconciliacao = pd.concat(reconciliacoes, ignore_index=True)
    exigir_aprovacao("Reconciliacao dos fatos", validacao_reconciliacao)

    fato_leitos = pd.DataFrame()
    historico_estabelecimentos = pd.DataFrame()
    validacao_cnes = pd.DataFrame()
    if argumentos.cnes:
        (
            fato_leitos,
            historico_estabelecimentos,
            validacao_cnes,
        ) = processar_cnes_historico(argumentos.cnes, competencias)
    if not fato_leitos.empty:
        pares_leitos = (
            fato_leitos[
                ["competencia_id", "codigo_cnes", "municipio_internacao_id"]
            ]
            .drop_duplicates()
        )
        pares_sih = pd.concat([pares_sih, pares_leitos], ignore_index=True)

    snapshot = pd.DataFrame()
    if argumentos.cnes_estabelecimentos:
        snapshot = carregar_snapshot_estabelecimentos(
            argumentos.cnes_estabelecimentos
        )
    dim_estabelecimento = criar_dim_estabelecimento(
        pares_sih, historico_estabelecimentos, snapshot
    )
    fato_capacidade = criar_fato_capacidade(
        fato_hospital, fato_leitos, dim_tempo
    )

    dimensoes = {
        "dim_tempo": dim_tempo,
        "dim_municipio": dim_municipio.rename(
            columns={"municipio_residencia_datasus": "municipio_id"}
        ),
        "dim_icsap": criar_dim_icsap(referencia),
        "dim_faixa_etaria": criar_dim_faixa_etaria(),
        "dim_estabelecimento_cnes": dim_estabelecimento,
        **criar_dimensoes_dominios(),
        **criar_dimensoes_leitos(fato_leitos),
    }
    if not fato_cid.empty:
        dimensoes["dim_cid3"] = (
            fato_cid[["cid3"]].drop_duplicates().sort_values("cid3").reset_index(drop=True)
        )

    fatos = {
        "fato_municipio_mes": fato_municipio,
        "fato_perfil_municipio_mes": fato_perfil,
        "fato_hospital_mes": fato_hospital,
        "fato_fluxo_municipio_mes": fato_fluxo,
    }
    if not fato_cid.empty:
        fatos["fato_cid3_municipio_mes"] = fato_cid
    if not fato_leitos.empty:
        fatos["fato_leito_cnes_mes"] = fato_leitos
    if not fato_capacidade.empty:
        fatos["fato_capacidade_hospital_mes"] = fato_capacidade

    for nome, tabela in dimensoes.items():
        exportar_csv(tabela, pasta_dim / f"{nome}.csv")
    for nome, tabela in fatos.items():
        exportar_csv(tabela, pasta_fatos / f"{nome}.csv")

    exportar_csv(
        pd.DataFrame(resumo_competencias),
        pasta_validacoes / "validacao_competencias_sih.csv",
    )
    exportar_csv(
        pd.concat(validacoes_base, ignore_index=True),
        pasta_validacoes / "validacao_base_sih_por_competencia.csv",
    )
    exportar_csv(
        validacao_reconciliacao,
        pasta_validacoes / "reconciliacao_fatos.csv",
    )
    if not validacao_cnes.empty:
        exportar_csv(
            validacao_cnes, pasta_validacoes / "validacao_competencias_cnes.csv"
        )

    manifesto = []
    for caminho in sorted(pasta_saida.rglob("*.csv")):
        with caminho.open("r", encoding="utf-8-sig", errors="replace") as arquivo:
            linhas = max(sum(1 for _ in arquivo) - 1, 0)
        manifesto.append(
            {
                "arquivo": str(caminho.relative_to(pasta_saida)),
                "linhas": linhas,
                "tamanho_bytes": caminho.stat().st_size,
            }
        )
    exportar_csv(
        pd.DataFrame(manifesto), pasta_validacoes / "manifesto_arquivos.csv"
    )

    LOG_REL.info("ETL relacional concluido e validado.")
    LOG_REL.info("Competencias SIH: %s.", len(competencias))
    LOG_REL.info("Fato municipio-mes: %s linhas.", len(fato_municipio))
    LOG_REL.info("Fato perfil: %s linhas.", len(fato_perfil))
    LOG_REL.info("Fato hospital: %s linhas.", len(fato_hospital))
    LOG_REL.info("Fato fluxo: %s linhas.", len(fato_fluxo))
    if not fato_cid.empty:
        LOG_REL.info("Fato CID3: %s linhas.", len(fato_cid))
    if argumentos.gerar_detalhe:
        LOG_REL.info("Detalhe SIH particionado: %s", pasta_detalhe)
    else:
        LOG_REL.info(
            "Detalhe SIH nao exportado. Use --gerar-detalhe apenas se necessario."
        )
    if argumentos.gerar_zip:
        caminho_zip = Path(
            shutil.make_archive(str(pasta_saida), "zip", root_dir=pasta_saida)
        )
        LOG_REL.info("Pacote ZIP: %s", caminho_zip)


def main_relacional() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    try:
        executar_etl_relacional(criar_argumentos_relacionais())
    except (FileNotFoundError, ValueError, RuntimeError) as erro:
        LOG_REL.error("%s", erro)
        return 1
    except KeyboardInterrupt:
        LOG_REL.error("Execucao cancelada pelo usuario.")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main_relacional())

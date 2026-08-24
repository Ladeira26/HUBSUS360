"""ETL do projeto HUBSUS360.

ETL significa Extracao, Transformacao e Carga. Neste codigo, isso corresponde a:
1. extrair os registros dos arquivos DBC/DBF do SIH/SUS;
2. tratar datas, codigos, valores, AIHs e informacoes dos pacientes;
3. identificar as Internacoes por Condicoes Sensiveis a Atencao Primaria
   (ICSAP) e seus 19 grupos;
4. acrescentar as descricoes oficiais dos diagnosticos da CID-10;
5. gerar os CSVs que serao carregados no Oracle e um arquivo unico para a
   Analise Exploratoria de Dados (AED).

Os indicadores hospitalares mensais usam as saidas hospitalares como
denominador. AIHs de continuidade continuam participando dos valores, dias e
obitos, mas nao sao confundidas com novas internacoes. Meses sem saida recebem
um marcador de indicador nao calculavel, sem preenchimento artificial com zero.

Os dados detalhados de cada competencia sao usados apenas durante o
processamento. O codigo exporta somente as tabelas finais que definimos para o
HUBSUS360. Cada mes do SIH e tratado separadamente para consumir menos memoria.
Depois da primeira leitura, o resultado tratado de cada mes fica salvo na pasta
cache_hubsus360. Nas proximas execucoes, esses arquivos prontos sao reutilizados.

As referencias ficam reunidas em uma pasta referencias_hubsus360, com as
subpastas dados_sih, ST, LT, sigtap, cid10, dominios_cnes, regioes_saude e
cnes_estabelecimentos. O programa relaciona cada competencia do SIH com ST,
LT e SIGTAP do mesmo mes.

Instalacao:
    python -m pip install pandas numpy openpyxl

Exemplos:
    python HUBSUS360_ETL.py --referencias referencias_hubsus360
    python HUBSUS360_ETL.py --referencias referencias_hubsus360 --anos 2023
"""

from __future__ import annotations

# ============================================================
# ETAPA 1 — BIBLIOTECAS E CONFIGURACOES GERAIS
# ============================================================

import argparse
import calendar
import csv
import gzip
import json
import logging
import re
import shutil
import struct
import sys
import tempfile
import unicodedata
import zipfile
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

import numpy as np
import pandas as pd


LOG = logging.getLogger("hubsus360")
UF_ANALISE = "SP"
CODIGO_UF_ANALISE = "35"


# Valores usados na abertura dos arquivos DBC do DATASUS. A rotina foi
# mantida dentro deste codigo para facilitar a execucao no Windows.
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


# ============================================================
# ETAPA 2 — ABERTURA DOS ARQUIVOS DBC E DBF DO DATASUS
# ============================================================

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


# ============================================================
# ETAPA 3 — CAMPOS DO SIH E REGRAS DOS 19 GRUPOS ICSAP
# ============================================================

# Campos do SIH que realmente serao usados pelo HUBSUS360.
COLUNAS_ORIGINAIS = [
    "UF_ZI", "ANO_CMPT", "MES_CMPT", "N_AIH", "IDENT", "MUNIC_RES",
    "MUNIC_MOV", "CNES", "DT_INTER", "DT_SAIDA", "DIAG_PRINC",
    "DIAG_SECUN", "PROC_REA", "DIAS_PERM", "VAL_TOT", "MORTE",
    "COBRANCA", "CAR_INT", "COD_IDADE", "IDADE", "SEXO", "RACA_COR",
    "COMPLEX", "NACIONAL",
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
    "COBRANCA": "motivo_saida_permanencia",
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
    "motivo_saida_permanencia", "unidade_idade", "sexo", "raca_cor",
    "complexidade", "nacionalidade",
]

# Lista Brasileira de ICSAP organizada nos 19 grupos definidos para o projeto.
# Os codigos sem ponto permitem comparar tanto categorias quanto subcategorias.
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
    "obito_descricao", "obito_icsap", "motivo_saida_permanencia",
    "conta_como_saida_hospitalar", "carater_internacao",
    "carater_internacao_descricao", "complexidade", "complexidade_descricao",
    "cid_principal_original", "cid_principal", "cid_secundario", "icsap",
    "internacao_icsap", "grupo_icsap", "nome_grupo_icsap",
    "regra_cid_icsap", "unidade_idade", "unidade_idade_descricao", "idade",
    "idade_anos", "faixa_etaria", "sexo", "sexo_descricao", "raca_cor",
    "raca_cor_descricao", "nacionalidade",
]


# ============================================================
# ETAPA 4 — LEITURA E TRATAMENTO DOS REGISTROS DO SIH
# ============================================================

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
    """Monta a base temporaria usada para criar as tabelas finais."""
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
    ).astype("string").fillna("Idade não informada")

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

    # O primeiro digito do motivo de saida/permanencia identifica a familia
    # do encerramento no SIH. Codigos iniciados por 2 representam permanencia;
    # os demais representam uma saida hospitalar (alta, transferencia, obito
    # ou outro encerramento). Essa separacao permite usar o denominador correto
    # nos indicadores hospitalares sem descartar as AIHs de continuidade.
    motivo_saida = (
        base["motivo_saida_permanencia"]
        .astype("string").str.replace(r"\.0$", "", regex=True)
        .str.strip().str.zfill(2)
    )
    base["motivo_saida_permanencia"] = motivo_saida
    base["conta_como_saida_hospitalar"] = (
        motivo_saida.notna()
        & ~motivo_saida.str.startswith("2", na=False)
    ).astype("int8")

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


# ============================================================
# ETAPA 5 — VERIFICACOES DOS DADOS TRATADOS
# ============================================================

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


# ============================================================
# ETAPA 6 — REFERENCIA DOS MUNICIPIOS DE SAO PAULO
# ============================================================

def carregar_referencia_municipios(caminho_csv: Path | None) -> pd.DataFrame:
    """Lê um CSV local ou consulta os municípios de São Paulo na API do IBGE."""
    if caminho_csv:
        caminho_csv = caminho_csv.expanduser().resolve()
        if not caminho_csv.is_file():
            raise FileNotFoundError(f"CSV de municípios não encontrado: {caminho_csv}")
        municipios = pd.read_csv(caminho_csv, dtype="string")

        if set(["municipio_residencia_datasus", "codigo_municipio_ibge7",
                "nome_municipio", "uf"]).issubset(municipios.columns):
            referencia_municipios = municipios[["municipio_residencia_datasus",
                                   "codigo_municipio_ibge7",
                                   "nome_municipio", "uf"]].copy()
            referencia_municipios = referencia_municipios[referencia_municipios["uf"].str.upper().eq(UF_ANALISE)]
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
            referencia_municipios = pd.DataFrame({
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
        referencia_municipios = pd.DataFrame([
            {
                "municipio_residencia_datasus": str(item["id"])[:6],
                "codigo_municipio_ibge7": str(item["id"]),
                "nome_municipio": item["nome"],
                "uf": UF_ANALISE,
            }
            for item in registros
        ])

    referencia_municipios["municipio_residencia_datasus"] = (
        referencia_municipios["municipio_residencia_datasus"]
        .astype("string").str.replace(r"\.0$", "", regex=True).str.zfill(6)
    )
    referencia_municipios["codigo_municipio_ibge7"] = (
        referencia_municipios["codigo_municipio_ibge7"]
        .astype("string").str.replace(r"\.0$", "", regex=True).str.zfill(7)
    )
    return referencia_municipios.sort_values("codigo_municipio_ibge7").reset_index(drop=True)


def exigir_aprovacao(nome: str, validacao: pd.DataFrame) -> None:
    """Interrompe o ETL e mostra exatamente qual regra precisa ser revisada."""
    pendencias = validacao[validacao["resultado"].ne("APROVADO")]
    if not pendencias.empty:
        raise ValueError(
            f"{nome} possui validações pendentes:\n"
            + pendencias.to_string(index=False)
        )


LOG = logging.getLogger("hubsus360.historico")
PADRAO_ARQUIVO_RD = re.compile(
    r"^RDSP(?P<ano>\d{2})(?P<mes>\d{2})(?:\.[^.]+)?$",
    flags=re.IGNORECASE,
)
VERSAO_CACHE_SIH = 1
NOME_MANIFESTO_SIH = "arquivos_sih.json"


# ============================================================
# ETAPA 7 — LOCALIZACAO DOS ARQUIVOS MENSAIS DO SIH
# ============================================================

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


# ============================================================
# ETAPA 7.1 — CACHE DOS MESES JA TRATADOS
# ============================================================

def salvar_lista_arquivos_sih(
    pasta_cache: Path,
    entradas: dict[tuple[int, int], Path],
) -> None:
    """Salva os caminhos escolhidos para nao abrir a janela novamente."""
    pasta_cache.mkdir(parents=True, exist_ok=True)
    registros = [
        {
            "ano": ano,
            "mes": mes,
            "caminho": str(caminho.resolve()),
        }
        for (ano, mes), caminho in sorted(entradas.items())
    ]
    (pasta_cache / NOME_MANIFESTO_SIH).write_text(
        json.dumps(registros, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def carregar_lista_arquivos_sih(pasta_cache: Path) -> list[Path] | None:
    """Recupera os caminhos usados na ultima execucao, quando ainda existem."""
    manifesto = pasta_cache / NOME_MANIFESTO_SIH
    if not manifesto.is_file():
        return None

    try:
        registros = json.loads(manifesto.read_text(encoding="utf-8"))
        caminhos = [Path(item["caminho"]) for item in registros]
    except (OSError, KeyError, TypeError, json.JSONDecodeError):
        return None

    if not caminhos or not all(caminho.is_file() for caminho in caminhos):
        return None
    return caminhos


def caminho_cache_mes(pasta_cache: Path, ano: int, mes: int) -> Path:
    """Define o nome do arquivo que guarda um mes ja tratado."""
    return pasta_cache / f"SIH_TRATADO_{ano}_{mes:02d}.pkl"


def carregar_cache_mes(
    pasta_cache: Path,
    caminho_origem: Path,
    ano: int,
    mes: int,
) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Carrega o mes salvo se o arquivo original nao tiver sido alterado."""
    caminho_cache = caminho_cache_mes(pasta_cache, ano, mes)
    if not caminho_cache.is_file():
        return None

    try:
        pacote = pd.read_pickle(caminho_cache)
        dados_arquivo = caminho_origem.stat()
        cache_valido = (
            pacote.get("versao") == VERSAO_CACHE_SIH
            and pacote.get("caminho_origem") == str(caminho_origem.resolve())
            and pacote.get("tamanho_origem") == dados_arquivo.st_size
            and pacote.get("modificado_origem") == dados_arquivo.st_mtime_ns
        )
        if not cache_valido:
            return None
        return pacote["agregado"], pacote["contexto"]
    except (OSError, KeyError, AttributeError, ValueError, EOFError):
        LOG_ETL.warning(
            "Cache de %s-%02d invalido; o arquivo sera lido novamente.",
            ano,
            mes,
        )
        return None


def salvar_cache_mes(
    pasta_cache: Path,
    caminho_origem: Path,
    ano: int,
    mes: int,
    agregado: pd.DataFrame,
    contexto: pd.DataFrame,
) -> None:
    """Salva somente os resultados mensais usados para gerar as tabelas."""
    pasta_cache.mkdir(parents=True, exist_ok=True)
    dados_arquivo = caminho_origem.stat()
    pacote = {
        "versao": VERSAO_CACHE_SIH,
        "caminho_origem": str(caminho_origem.resolve()),
        "tamanho_origem": dados_arquivo.st_size,
        "modificado_origem": dados_arquivo.st_mtime_ns,
        "agregado": agregado,
        "contexto": contexto,
    }
    pd.to_pickle(
        pacote,
        caminho_cache_mes(pasta_cache, ano, mes),
    )


# ============================================================
# ETAPA 8 — TABELAS E ARQUIVOS GERADOS PELO HUBSUS360
# ============================================================
# Tabelas geradas:
#   T_GRUPO_ICSAP
#   T_DIAGNOSTICO
#   T_MUNICIPIO
#   T_ESTABELECIMENTO
#   T_PROCEDIMENTO
#   T_FAIXA_ETARIA
#   T_SEXO
#   T_RACA_COR
#   T_PERFIL_INTERNACAO
#   T_RESUMO_AIH
#   T_RESUMO_ASSISTENCIAL
#   T_CATEGORIA_LEITO
#   T_CAPACIDADE_LEITO
#
# Tambem sao gerados:
#   DATASET_AED_HUBSUS360.csv
#   CIDS_SEM_DESCRICAO.csv
#   arquivos mensais na pasta cache_hubsus360
#
# Esse arquivo reúne em uma unica tabela as informacoes necessarias para a AED.
# Assim, os colegas podem trabalhar no Pandas/Jupyter/Colab sem precisar
# combinar varios CSVs antes de iniciar a analise.
# ============================================================

LOG_ETL = logging.getLogger("hubsus360.etl")

# Referencia oficial da CID-10 disponibilizada pelo DATASUS. O arquivo possui
# os codigos sem ponto (por exemplo, N390) e suas descricoes em portugues.
NOME_ARQUIVO_CID10 = "CID-10-SUBCATEGORIAS.CSV"

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
    "Idade não informada": 13,
}

LIMITES_FAIXA = {
    "Menor de 1 ano": (0, 0),
    "1 a 4 anos": (1, 4),
    "5 a 9 anos": (5, 9),
    "10 a 14 anos": (10, 14),
    "15 a 19 anos": (15, 19),
    "20 a 29 anos": (20, 29),
    "30 a 39 anos": (30, 39),
    "40 a 49 anos": (40, 49),
    "50 a 59 anos": (50, 59),
    "60 a 69 anos": (60, 69),
    "70 a 79 anos": (70, 79),
    "80 anos ou mais": (80, pd.NA),
    "Idade não informada": (pd.NA, pd.NA),
}

PADRAO_CNES = re.compile(
    r"^(?P<grupo>LT|ST)SP(?P<ano>\d{2})(?P<mes>\d{2})(?:\.[^.]+)?$",
    flags=re.IGNORECASE,
)

CHAVES_PERFIL = [
    "DT_COMPETENCIA",
    "CD_MUNICIPIO_RESIDENCIA",
    "CD_CNES",
    "CD_CID10",
    "CD_PROCEDIMENTO",
    "ID_FAIXA_ETARIA",
    "CD_SEXO",
    "CD_RACA_COR",
]


# ============================================================
# ETAPA 9 — OPCOES DE EXECUCAO
# ============================================================

def criar_argumentos_hubsus360() -> argparse.Namespace:
    """Configura os caminhos e filtros usados pelo ETL final."""
    parser = argparse.ArgumentParser(
        description=(
            "ETL do HUBSUS360: gera os CSVs para o Oracle e um arquivo "
            "unico para AED."
        )
    )
    parser.add_argument("--sih", type=Path, nargs="+")
    parser.add_argument(
        "--saida",
        type=Path,
        default=Path("resultados_hubsus360_modelo_final"),
    )
    parser.add_argument("--anos", type=int, nargs="+")
    parser.add_argument("--meses", type=int, nargs="+")
    parser.add_argument("--municipios-ibge", type=Path)
    parser.add_argument(
        "--cid10",
        type=Path,
        help=(
            "CSV CID-10-SUBCATEGORIAS.CSV, arquivo CID10CSV.zip ou pasta "
            "que contenha um deles. Se omitido, o ETL procura o CSV na "
            "mesma pasta do programa."
        ),
    )
    parser.add_argument("--cnes", type=Path)
    parser.add_argument("--cnes-estabelecimentos", type=Path)
    parser.add_argument("--gerar-zip", action="store_true")
    parser.add_argument(
        "--refazer-cache",
        action="store_true",
        help=(
            "Ignora os meses salvos e le novamente os arquivos DBC/DBF. "
            "Use somente quando quiser reconstruir todo o tratamento."
        ),
    )
    argumentos = parser.parse_args()

    if argumentos.meses:
        argumentos.meses = sorted(set(argumentos.meses))
        invalidos = [m for m in argumentos.meses if m not in range(1, 13)]
        if invalidos:
            parser.error(f"Meses inválidos: {invalidos}.")

    if argumentos.anos:
        argumentos.anos = sorted(set(argumentos.anos))

    return argumentos


# ============================================================
# ETAPA 10 — LEITURA DA REFERENCIA CID-10
# ============================================================

def extrair_referencia_cid10_zip(
    caminho_zip: Path,
    pasta_destino: Path,
) -> Path:
    """Extrai somente o CSV de subcategorias de um pacote CID10CSV.zip."""
    pasta_destino.mkdir(parents=True, exist_ok=True)
    caminho_saida = pasta_destino / NOME_ARQUIVO_CID10

    try:
        with zipfile.ZipFile(caminho_zip) as pacote:
            candidatos = [
                membro
                for membro in pacote.namelist()
                if Path(membro).name.upper() == NOME_ARQUIVO_CID10.upper()
            ]
            if not candidatos:
                raise ValueError(
                    f"{NOME_ARQUIVO_CID10} nao foi encontrado em "
                    f"{caminho_zip}."
                )

            # Abre apenas o membro esperado e grava com nome controlado. Isso
            # evita extrair caminhos adicionais presentes no arquivo ZIP.
            with pacote.open(candidatos[0]) as origem, caminho_saida.open(
                "wb"
            ) as destino:
                shutil.copyfileobj(origem, destino)
    except zipfile.BadZipFile as erro:
        raise ValueError(
            f"O arquivo informado nao e um ZIP valido: {caminho_zip}."
        ) from erro

    return caminho_saida


def procurar_referencia_cid10_em_pasta(pasta: Path) -> Path | None:
    """Procura o CSV da CID-10 ou o pacote ZIP dentro de uma pasta."""
    if not pasta.exists() or not pasta.is_dir():
        return None

    # O Windows pode esconder a extensao e o arquivo acabar salvo como
    # .CSV.csv. Tambem podem existir pequenas diferencas de hifen ou espaco no
    # nome. A comparacao abaixo ignora esses detalhes e procura pelo conteudo
    # principal do nome do arquivo.
    candidatos_csv = []
    for caminho in pasta.rglob("*"):
        nome_normalizado = re.sub(
            r"[^A-Z0-9]",
            "",
            caminho.name.upper(),
        )
        if (
            caminho.is_file()
            and caminho.suffix.lower() == ".csv"
            and nome_normalizado.startswith("CID10SUBCATEGORIAS")
        ):
            candidatos_csv.append(caminho)

    if candidatos_csv:
        # Primeiro prioriza o arquivo que esta diretamente na pasta analisada.
        # Se houver mais de uma copia, fica com a maior, pois a tabela completa
        # possui milhares de linhas e e maior do que arquivos de teste.
        return max(
            candidatos_csv,
            key=lambda arquivo: (
                arquivo.parent == pasta,
                arquivo.stat().st_size,
            ),
        )

    for caminho in pasta.rglob("*"):
        if caminho.is_file() and caminho.name.upper() == "CID10CSV.ZIP":
            return caminho

    return None


def localizar_referencia_cid10(
    caminho_informado: Path | None,
    pasta_cache: Path,
) -> Path:
    """Localiza a referencia CID-10 informada ou colocada perto do codigo."""
    if caminho_informado is not None:
        caminho = caminho_informado.expanduser().resolve()
        if not caminho.exists():
            # Se o nome digitado nao existir exatamente, ainda tentamos achar
            # o CSV na mesma pasta. Isso resolve nomes com extensao duplicada
            # ou pequenas diferencas criadas pelo navegador/Windows.
            encontrado = procurar_referencia_cid10_em_pasta(caminho.parent)
            if encontrado is None:
                raise FileNotFoundError(
                    "Referencia CID-10 nao encontrada. Pasta verificada: "
                    f"{caminho.parent}. Confirme se o arquivo comeca com "
                    "CID-10-SUBCATEGORIAS e possui extensao CSV."
                )
            caminho = encontrado
        if caminho.is_dir():
            encontrado = procurar_referencia_cid10_em_pasta(caminho)
            if encontrado is None:
                raise FileNotFoundError(
                    f"Nenhum {NOME_ARQUIVO_CID10} ou CID10CSV.zip foi "
                    f"encontrado em {caminho}."
                )
            caminho = encontrado

        if caminho.suffix.lower() == ".zip":
            return extrair_referencia_cid10_zip(caminho, pasta_cache)
        if caminho.suffix.lower() != ".csv":
            raise ValueError(
                "A referencia CID-10 deve ser um CSV, um ZIP ou uma pasta."
            )
        return caminho

    pasta_programa = Path(__file__).resolve().parent
    pastas_busca = [
        # No uso pelo PyCharm, o CSV sera colocado ao lado deste codigo.
        pasta_programa,
        Path.cwd(),
        Path.cwd() / "referencias",
        pasta_programa / "referencias",
        pasta_cache,
    ]
    visitadas = set()
    for pasta in pastas_busca:
        pasta_resolvida = pasta.resolve()
        if pasta_resolvida in visitadas:
            continue
        visitadas.add(pasta_resolvida)
        encontrado = procurar_referencia_cid10_em_pasta(pasta_resolvida)
        if encontrado is None:
            continue
        if encontrado.suffix.lower() == ".zip":
            return extrair_referencia_cid10_zip(encontrado, pasta_cache)
        return encontrado

    raise FileNotFoundError(
        f"O arquivo {NOME_ARQUIVO_CID10} nao foi encontrado. "
        "Coloque-o na mesma pasta deste codigo ou informe o caminho com "
        "--cid10."
    )


def carregar_referencia_cid10(caminho: Path) -> pd.DataFrame:
    """Le e padroniza o dicionario CID-10 brasileiro do DATASUS."""
    ultimo_erro: Exception | None = None
    dados: pd.DataFrame | None = None

    # Tenta UTF-8 primeiro para aceitar copias modernas do arquivo. A versao
    # historica do DATASUS costuma usar Windows-1252/Latin-1.
    for codificacao in ("utf-8-sig", "cp1252", "latin1"):
        try:
            dados = pd.read_csv(
                caminho,
                sep=";",
                encoding=codificacao,
                dtype="string",
                keep_default_na=False,
                # O CSV do DATASUS usa aspas como parte de algumas
                # descricoes (por exemplo, "stress"), nao para delimitar
                # campos. Por isso elas devem ser lidas como texto comum.
                quoting=csv.QUOTE_NONE,
                engine="python",
            )
            break
        except (UnicodeDecodeError, pd.errors.ParserError) as erro:
            ultimo_erro = erro

    if dados is None:
        raise ValueError(
            f"Nao foi possivel ler a referencia CID-10: {caminho}."
        ) from ultimo_erro

    dados.columns = [
        str(coluna).replace("\ufeff", "").strip().upper()
        for coluna in dados.columns
    ]
    obrigatorias = {"SUBCAT", "DESCRICAO"}
    faltantes = sorted(obrigatorias.difference(dados.columns))
    if faltantes:
        raise ValueError(
            "A referencia CID-10 nao possui as colunas obrigatorias: "
            f"{faltantes}."
        )

    referencia = dados[["SUBCAT", "DESCRICAO"]].copy()
    referencia["CD_CID10"] = (
        referencia["SUBCAT"]
        .astype("string")
        .str.upper()
        .str.replace(r"[^A-Z0-9]", "", regex=True)
        .str.strip()
    )
    referencia["DS_DIAGNOSTICO"] = (
        referencia["DESCRICAO"]
        .astype("string")
        .str.strip()
        .replace("", pd.NA)
    )
    referencia = referencia[
        referencia["CD_CID10"].str.match(
            r"^[A-Z][0-9]{2}[A-Z0-9]?$",
            na=False,
        )
    ].copy()

    conflitos = (
        referencia.groupby("CD_CID10", dropna=False)["DS_DIAGNOSTICO"]
        .nunique(dropna=False)
        .gt(1)
    )
    if conflitos.any():
        codigos = conflitos[conflitos].index.tolist()[:10]
        raise ValueError(
            "A referencia CID-10 possui descricoes conflitantes para: "
            f"{codigos}."
        )

    referencia = (
        referencia[["CD_CID10", "DS_DIAGNOSTICO"]]
        .drop_duplicates("CD_CID10")
        .sort_values("CD_CID10")
        .reset_index(drop=True)
    )
    if referencia.empty:
        raise ValueError("A referencia CID-10 ficou vazia apos a padronizacao.")

    LOG_ETL.info(
        "Referencia CID-10 carregada: %s codigos (%s).",
        len(referencia),
        caminho.name,
    )
    return referencia


# ============================================================
# ETAPA 11 — PREPARACAO DOS CODIGOS USADOS NAS TABELAS
# ============================================================

def preparar_modelo_final(base_mestra: pd.DataFrame) -> pd.DataFrame:
    """
    Prepara os codigos que ligam as tabelas finais.

    A base detalhada existe apenas dentro do ETL para organizar o tratamento.
    Ela nao e exportada como uma tabela do banco.
    """
    base = base_mestra.copy()

    base["DT_COMPETENCIA"] = pd.to_datetime(
        base["data_competencia"], errors="coerce"
    )
    base["CD_MUNICIPIO_RESIDENCIA"] = (
        base["municipio_residencia_datasus"]
        .astype("string").str.replace(r"\.0$", "", regex=True).str.zfill(6)
    )
    base["CD_MUNICIPIO_INTERNACAO"] = (
        base["municipio_internacao_datasus"]
        .astype("string").str.replace(r"\.0$", "", regex=True).str.zfill(6)
    )
    base["CD_CNES"] = (
        base["codigo_cnes"]
        .astype("string").str.replace(r"\.0$", "", regex=True).str.zfill(7)
    )
    base["CD_CID10"] = base["cid_principal"].astype("string").str.upper()
    base["CD_CATEGORIA_CID10"] = base["CD_CID10"].str[:3]
    base["CD_PROCEDIMENTO"] = (
        base["procedimento_realizado"]
        .astype("string").str.replace(r"\.0$", "", regex=True).str.strip()
    )
    base["ID_FAIXA_ETARIA"] = (
        base["faixa_etaria"].map(MAPA_FAIXA_ID).astype("Int64")
    )
    base["CD_SEXO"] = base["sexo"].astype("string").str.strip()
    base["CD_RACA_COR"] = (
        base["raca_cor"]
        .astype("string").str.replace(r"\.0$", "", regex=True).str.zfill(2)
    )

    return base


# ============================================================
# ETAPA 12 — TABELAS DE IDENTIFICACAO E DESCRICAO
# ============================================================

def criar_t_grupo_icsap(referencia: pd.DataFrame) -> pd.DataFrame:
    """Cria os 19 grupos ICSAP."""
    tabela = (
        referencia[["grupo_icsap", "nome_grupo_icsap"]]
        .drop_duplicates()
        .rename(columns={
            "grupo_icsap": "ID_GRUPO_ICSAP",
            "nome_grupo_icsap": "NM_GRUPO_ICSAP",
        })
        .sort_values("ID_GRUPO_ICSAP")
        .reset_index(drop=True)
    )
    tabela["DS_GRUPO_ICSAP"] = tabela["NM_GRUPO_ICSAP"]
    return tabela[
        ["ID_GRUPO_ICSAP", "NM_GRUPO_ICSAP", "DS_GRUPO_ICSAP"]
    ]


def criar_t_diagnostico(
    base: pd.DataFrame,
    referencia_cid10: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cria T_DIAGNOSTICO com os CIDs encontrados no SIH.

    O grupo ICSAP continua vindo da classificacao ja feita nas AIHs. A
    referencia oficial do DATASUS e usada somente para acrescentar a descricao
    do diagnostico e validar o codigo, sem recalcular os 19 grupos.
    """
    tabela = (
        base[["CD_CID10", "CD_CATEGORIA_CID10", "grupo_icsap"]]
        .drop_duplicates()
        .rename(columns={"grupo_icsap": "ID_GRUPO_ICSAP"})
    )
    tabela = tabela.merge(
        referencia_cid10,
        on="CD_CID10",
        how="left",
        validate="many_to_one",
    )
    tabela["ID_GRUPO_ICSAP"] = tabela["ID_GRUPO_ICSAP"].astype("Int64")
    return (
        tabela[[
            "CD_CID10",
            "CD_CATEGORIA_CID10",
            "DS_DIAGNOSTICO",
            "ID_GRUPO_ICSAP",
        ]]
        .sort_values("CD_CID10")
        .reset_index(drop=True)
    )


def criar_t_municipio(municipios_ibge: pd.DataFrame) -> pd.DataFrame:
    """Converte a referência municipal para T_MUNICIPIO."""
    tabela = municipios_ibge.rename(columns={
        "municipio_residencia_datasus": "CD_MUNICIPIO",
        "nome_municipio": "NM_MUNICIPIO",
        "uf": "SG_UF",
    }).copy()

    # A API municipal do IBGE não informa Região de Saúde.
    # O campo permanece reservado para enriquecimento futuro.
    tabela["NM_REGIAO_SAUDE"] = pd.NA

    return (
        tabela[[
            "CD_MUNICIPIO",
            "NM_MUNICIPIO",
            "SG_UF",
            "NM_REGIAO_SAUDE",
        ]]
        .drop_duplicates("CD_MUNICIPIO")
        .sort_values("CD_MUNICIPIO")
        .reset_index(drop=True)
    )


def criar_t_faixa_etaria() -> pd.DataFrame:
    """Cria as faixas etárias definidas no tratamento."""
    linhas = []
    for nome, identificador in MAPA_FAIXA_ID.items():
        minimo, maximo = LIMITES_FAIXA[nome]
        linhas.append({
            "ID_FAIXA_ETARIA": identificador,
            "DS_FAIXA_ETARIA": nome,
            "NR_IDADE_MINIMA": minimo,
            "NR_IDADE_MAXIMA": maximo,
        })
    return pd.DataFrame(linhas)


def criar_t_sexo(base: pd.DataFrame) -> pd.DataFrame:
    """Cria T_SEXO com os códigos observados."""
    return (
        base[["CD_SEXO", "sexo_descricao"]]
        .drop_duplicates()
        .rename(columns={"sexo_descricao": "DS_SEXO"})
        .sort_values("CD_SEXO")
        .reset_index(drop=True)
    )


def criar_t_raca_cor(base: pd.DataFrame) -> pd.DataFrame:
    """Cria T_RACA_COR com os códigos observados."""
    return (
        base[["CD_RACA_COR", "raca_cor_descricao"]]
        .drop_duplicates()
        .rename(columns={"raca_cor_descricao": "DS_RACA_COR"})
        .sort_values("CD_RACA_COR")
        .reset_index(drop=True)
    )


def criar_t_procedimento(base: pd.DataFrame) -> pd.DataFrame:
    """
    Cria T_PROCEDIMENTO.

    O código do procedimento e a complexidade vêm do SIH.
    Descrição e tipo ficam reservados para enriquecimento via SIGTAP.
    """
    tabela = (
        base[["CD_PROCEDIMENTO", "complexidade_descricao"]]
        .drop_duplicates()
        .rename(columns={"complexidade_descricao": "TP_COMPLEXIDADE"})
        .groupby("CD_PROCEDIMENTO", as_index=False, dropna=False)
        .agg(TP_COMPLEXIDADE=("TP_COMPLEXIDADE", "first"))
    )
    tabela["DS_PROCEDIMENTO"] = pd.NA
    tabela["TP_PROCEDIMENTO"] = pd.NA
    return tabela[[
        "CD_PROCEDIMENTO",
        "DS_PROCEDIMENTO",
        "TP_PROCEDIMENTO",
        "TP_COMPLEXIDADE",
    ]]


def criar_t_estabelecimento(
    base: pd.DataFrame,
    snapshot: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Cria T_ESTABELECIMENTO.

    Sem o snapshot do CNES, CD_CNES e CD_MUNICIPIO já são preenchidos com SIH.
    Nome e tipo ficam vazios até a etapa de enriquecimento.
    """
    tabela = (
        base[["CD_CNES", "CD_MUNICIPIO_INTERNACAO"]]
        .drop_duplicates()
        .rename(columns={"CD_MUNICIPIO_INTERNACAO": "CD_MUNICIPIO"})
    )
    tabela["NM_ESTABELECIMENTO"] = pd.NA
    tabela["TP_ESTABELECIMENTO"] = pd.NA

    if snapshot is not None and not snapshot.empty:
        tabela = tabela.merge(
            snapshot,
            on="CD_CNES",
            how="left",
            suffixes=("", "_CNES"),
            validate="one_to_one",
        )
        for coluna in [
            "CD_MUNICIPIO",
            "NM_ESTABELECIMENTO",
            "TP_ESTABELECIMENTO",
        ]:
            cnes = f"{coluna}_CNES"
            if cnes in tabela.columns:
                tabela[coluna] = tabela[cnes].fillna(tabela[coluna])
                tabela = tabela.drop(columns=cnes)

    return (
        tabela[[
            "CD_CNES",
            "CD_MUNICIPIO",
            "NM_ESTABELECIMENTO",
            "TP_ESTABELECIMENTO",
        ]]
        .sort_values("CD_CNES")
        .reset_index(drop=True)
    )


# ============================================================
# ETAPA 13 — PERFIL DA INTERNACAO E SEUS RESUMOS
# ============================================================

def agregar_perfis_mes(base: pd.DataFrame) -> pd.DataFrame:
    """
    Agrupa os registros no nível da nova T_PERFIL_INTERNACAO.

    Uma linha representa:
    competência + município + hospital + CID + procedimento +
    faixa etária + sexo + raça/cor.
    """
    residentes_sp = base[base["uf_residencia"].eq(UF_ANALISE)].copy()

    return (
        residentes_sp.groupby(CHAVES_PERFIL, dropna=False)
        .agg(
            QT_REGISTROS_AIH=("id_registro_sih", "size"),
            QT_CONTINUIDADES_AIH=(
                "categoria_aih",
                lambda s: int(s.eq("CONTINUIDADE").sum()),
            ),
            VL_TOTAL_AIH=("valor_total", "sum"),
            QT_INTERNACOES=("conta_como_internacao", "sum"),
            QT_INTERNACOES_ICSAP=("internacao_icsap", "sum"),
            VL_TOTAL_ICSAP=("valor_icsap", "sum"),
            QT_DIAS_PERMANENCIA=("dias_permanencia", "sum"),
            QT_OBITOS=("obito", "sum"),
        )
        .reset_index()
    )


def criar_tabelas_perfil_e_resumos(
    agregados: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Separa o perfil, os dados das AIHs e os dados assistenciais."""
    perfil = (
        agregados[CHAVES_PERFIL]
        .drop_duplicates()
        .sort_values(CHAVES_PERFIL, na_position="last")
        .reset_index(drop=True)
    )
    perfil.insert(
        0,
        "ID_PERFIL_INTERNACAO",
        np.arange(1, len(perfil) + 1),
    )

    base = agregados.merge(
        perfil,
        on=CHAVES_PERFIL,
        how="left",
        validate="many_to_one",
    )

    resumo_aih = base[[
        "ID_PERFIL_INTERNACAO",
        "QT_REGISTROS_AIH",
        "QT_CONTINUIDADES_AIH",
        "VL_TOTAL_AIH",
    ]].copy()
    resumo_aih.insert(
        0,
        "ID_RESUMO_AIH",
        np.arange(1, len(resumo_aih) + 1),
    )

    resumo_assistencial = base[[
        "ID_PERFIL_INTERNACAO",
        "QT_INTERNACOES",
        "QT_INTERNACOES_ICSAP",
        "VL_TOTAL_ICSAP",
        "QT_DIAS_PERMANENCIA",
        "QT_OBITOS",
    ]].copy()
    resumo_assistencial.insert(
        0,
        "ID_RESUMO_ASSISTENCIAL",
        np.arange(1, len(resumo_assistencial) + 1),
    )

    return perfil, resumo_aih, resumo_assistencial


# ============================================================
# ETAPA 14 — CNES: ESTABELECIMENTOS E LEITOS
# ============================================================

def escolher_coluna(
    dados: pd.DataFrame,
    aliases: list[str],
    descricao: str,
    obrigatoria: bool = True,
) -> str | None:
    """Localiza uma coluna do CNES aceitando nomes equivalentes."""
    mapa = {str(coluna).upper(): coluna for coluna in dados.columns}
    for alias in aliases:
        if alias.upper() in mapa:
            return mapa[alias.upper()]
    if obrigatoria:
        raise ValueError(
            f"Campo CNES de {descricao} não encontrado. "
            f"Aliases aceitos: {aliases}."
        )
    return None


def ler_dbf_generico(caminho: Path) -> pd.DataFrame:
    """Lê um DBF genérico do CNES."""
    try:
        from dbfread import DBF
    except ImportError as erro:
        raise RuntimeError(
            "A biblioteca dbfread não está instalada. Execute: "
            "python -m pip install dbfread"
        ) from erro

    tabela = DBF(
        str(caminho),
        encoding="latin-1",
        load=True,
        char_decode_errors="ignore",
    )
    dados = pd.DataFrame(iter(tabela))
    if dados.empty:
        raise ValueError(f"O DBF está vazio: {caminho.name}")
    return dados


def carregar_cnes_generico(caminho: Path) -> pd.DataFrame:
    """Descompacta o DBC do CNES e devolve seus registros em uma tabela."""
    with tempfile.TemporaryDirectory(prefix="hubsus360_cnes_") as temporario:
        dbf = preparar_dbf(caminho, Path(temporario))
        return ler_dbf_generico(dbf)


def extrair_competencia_cnes(caminho: Path) -> tuple[str, int, int] | None:
    """Extrai competência de nomes como LTSP2401.dbc."""
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
    """Indexa arquivos LT/ST do CNES."""
    pasta = pasta.expanduser().resolve()
    if not pasta.is_dir():
        raise FileNotFoundError(f"Pasta CNES não encontrada: {pasta}")

    encontrados = {}
    for caminho in pasta.rglob("*"):
        if (
            caminho.is_file()
            and caminho.suffix.lower() in {".dbc", ".dbf"}
        ):
            chave = extrair_competencia_cnes(caminho)
            if chave is not None:
                encontrados[chave] = caminho

    return encontrados


def tratar_leitos_cnes(
    dados: pd.DataFrame,
    ano: int,
    mes: int,
) -> pd.DataFrame:
    """Padroniza os registros de leitos para as tabelas finais."""
    cnes = escolher_coluna(
        dados, ["CNES", "CO_CNES", "COD_CNES"], "CNES"
    )
    tipo = escolher_coluna(
        dados,
        ["TP_LEITO", "TIPO_LEITO", "CO_TIPO_LEITO"],
        "tipo de leito",
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
        dados,
        ["LEIT_SUS", "LEITOS_SUS", "QT_SUS"],
        "leitos SUS",
    )

    tabela = pd.DataFrame({
        "DT_COMPETENCIA": pd.Timestamp(year=ano, month=mes, day=1),
        "CD_CNES": dados[cnes],
        "TP_LEITO": dados[tipo],
        "DS_ESPECIALIDADE": dados[especialidade],
        "QT_LEITOS_EXISTENTES": dados[existentes],
        "QT_LEITOS_SUS": dados[sus],
    })

    tabela["CD_CNES"] = (
        tabela["CD_CNES"]
        .astype("string").str.replace(r"\.0$", "", regex=True)
        .str.strip().str.zfill(7)
    )
    tabela["TP_LEITO"] = (
        tabela["TP_LEITO"]
        .astype("string").str.replace(r"\.0$", "", regex=True)
        .str.strip().str.zfill(2)
    )
    tabela["DS_ESPECIALIDADE"] = (
        tabela["DS_ESPECIALIDADE"].astype("string").str.strip()
    )

    for coluna in ["QT_LEITOS_EXISTENTES", "QT_LEITOS_SUS"]:
        tabela[coluna] = (
            pd.to_numeric(tabela[coluna], errors="coerce")
            .fillna(0)
            .astype(int)
        )

    chaves = [
        "DT_COMPETENCIA",
        "CD_CNES",
        "TP_LEITO",
        "DS_ESPECIALIDADE",
    ]

    return (
        tabela.groupby(chaves, dropna=False)[
            ["QT_LEITOS_EXISTENTES", "QT_LEITOS_SUS"]
        ]
        .sum()
        .reset_index()
    )


def carregar_snapshot_estabelecimentos(caminho: Path) -> pd.DataFrame:
    """Lê CSV opcional para enriquecer nome e tipo do estabelecimento."""
    caminho = caminho.expanduser().resolve()
    if not caminho.is_file():
        raise FileNotFoundError(f"CSV CNES não encontrado: {caminho}")

    dados = pd.read_csv(caminho, dtype="string", low_memory=False)

    aliases = {
        "codigo_cnes": "CD_CNES",
        "codigo_municipio": "CD_MUNICIPIO",
        "nome_fantasia": "NM_ESTABELECIMENTO",
        "codigo_tipo_unidade": "TP_ESTABELECIMENTO",
    }

    presentes = [coluna for coluna in aliases if coluna in dados.columns]
    if "codigo_cnes" not in presentes:
        raise ValueError(
            "O CSV de estabelecimentos precisa possuir codigo_cnes."
        )

    tabela = dados[presentes].rename(columns=aliases).copy()
    tabela["CD_CNES"] = (
        tabela["CD_CNES"]
        .str.replace(r"\.0$", "", regex=True)
        .str.zfill(7)
    )

    if "CD_MUNICIPIO" in tabela.columns:
        tabela["CD_MUNICIPIO"] = (
            tabela["CD_MUNICIPIO"]
            .str.replace(r"\.0$", "", regex=True)
            .str.zfill(6)
        )

    return tabela.drop_duplicates("CD_CNES", keep="last")


def carregar_cache_mes(
    pasta_cache: Path,
    caminho_origem: Path,
    ano: int,
    mes: int,
) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Reutiliza o mes salvo mesmo se a pasta principal tiver sido movida."""
    caminho_cache = caminho_cache_mes(pasta_cache, ano, mes)
    if not caminho_cache.is_file():
        return None
    try:
        pacote = pd.read_pickle(caminho_cache)
        origem = caminho_origem.stat()
        nome_anterior = Path(str(pacote.get("caminho_origem", ""))).name
        valido = (
            pacote.get("versao") == VERSAO_CACHE_SIH
            and nome_anterior.lower() == caminho_origem.name.lower()
            and pacote.get("tamanho_origem") == origem.st_size
            and pacote.get("modificado_origem") == origem.st_mtime_ns
        )
        if valido:
            return pacote["agregado"], pacote["contexto"]
    except (OSError, KeyError, AttributeError, ValueError, EOFError):
        pass
    return None


_preparar_modelo_final_base = preparar_modelo_final


def preparar_modelo_final(base_mestra: pd.DataFrame) -> pd.DataFrame:
    """Acrescenta as chaves e os denominadores definidos na auditoria."""
    base = _preparar_modelo_final_base(base_mestra)
    categoria = base["CD_CID10"].astype("string").str[:3]
    parto_sem_complicacao = categoria.str.match(r"^O8[0-4]$", na=False)
    base["FL_ELEGIVEL_ICSAP_IEP"] = (~parto_sem_complicacao).astype("int8")
    base["QT_INTERNACAO_ELEGIVEL_ICSAP"] = (
        base["conta_como_internacao"].eq(1) & ~parto_sem_complicacao
    ).astype("int8")
    base["VL_TOTAL_ELEGIVEL_IEP"] = np.where(
        ~parto_sem_complicacao, base["valor_total"], 0.0
    )
    return base


def agregar_perfis_mes(base: pd.DataFrame) -> pd.DataFrame:
    """Agrupa registros mantendo os valores necessarios para ICSAP e IEP."""
    residentes_sp = base[base["uf_residencia"].eq(UF_ANALISE)].copy()
    return (
        residentes_sp.groupby(CHAVES_PERFIL, dropna=False)
        .agg(
            QT_REGISTROS_AIH=("id_registro_sih", "size"),
            QT_CONTINUIDADES_AIH=("categoria_aih", lambda s: int(s.eq("CONTINUIDADE").sum())),
            VL_TOTAL_AIH=("valor_total", "sum"),
            QT_INTERNACOES=("conta_como_internacao", "sum"),
            QT_INTERNACOES_ELEGIVEIS_ICSAP=("QT_INTERNACAO_ELEGIVEL_ICSAP", "sum"),
            QT_INTERNACOES_ICSAP=("internacao_icsap", "sum"),
            VL_TOTAL_ICSAP=("valor_icsap", "sum"),
            VL_TOTAL_ELEGIVEL_IEP=("VL_TOTAL_ELEGIVEL_IEP", "sum"),
            QT_DIAS_PERMANENCIA=("dias_permanencia", "sum"),
            QT_SAIDAS_HOSPITALARES=("conta_como_saida_hospitalar", "sum"),
            QT_OBITOS=("obito", "sum"),
        ).reset_index()
    )


def criar_tabelas_perfil_e_resumos(
    agregados: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    perfil = (
        agregados[CHAVES_PERFIL].drop_duplicates()
        .sort_values(CHAVES_PERFIL, na_position="last").reset_index(drop=True)
    )
    perfil.insert(0, "ID_PERFIL_INTERNACAO", np.arange(1, len(perfil) + 1))
    base = agregados.merge(perfil, on=CHAVES_PERFIL, how="left", validate="many_to_one")
    resumo_aih = base[[
        "ID_PERFIL_INTERNACAO", "QT_REGISTROS_AIH", "QT_CONTINUIDADES_AIH",
        "VL_TOTAL_AIH",
    ]].copy()
    resumo_aih.insert(0, "ID_RESUMO_AIH", np.arange(1, len(resumo_aih) + 1))
    resumo_assistencial = base[[
        "ID_PERFIL_INTERNACAO", "QT_INTERNACOES",
        "QT_INTERNACOES_ELEGIVEIS_ICSAP", "QT_INTERNACOES_ICSAP",
        "VL_TOTAL_ICSAP", "VL_TOTAL_ELEGIVEL_IEP",
        "QT_DIAS_PERMANENCIA", "QT_SAIDAS_HOSPITALARES", "QT_OBITOS",
    ]].copy()
    resumo_assistencial.insert(
        0, "ID_RESUMO_ASSISTENCIAL", np.arange(1, len(resumo_assistencial) + 1)
    )
    return perfil, resumo_aih, resumo_assistencial


def tratar_st_mes(
    caminho: Path,
    ano: int,
    mes: int,
    dominios: dict[str, dict[str, str]],
) -> pd.DataFrame:
    dados = carregar_cnes_colunas(caminho, ["CNES", "CODUFMUN", "TP_UNID"])
    tabela = pd.DataFrame({
        "DT_COMPETENCIA": pd.Timestamp(ano, mes, 1),
        "CD_CNES": normalizar_codigo(dados["CNES"], 7),
        "CD_MUNICIPIO_ST": normalizar_codigo(dados["CODUFMUN"], 6),
        "CD_TIPO_ESTABELECIMENTO": normalizar_codigo(dados["TP_UNID"]),
    })
    mapa = dominios["TIPOS_ESTABELECIMENTO"]
    tabela["TP_ESTABELECIMENTO"] = tabela["CD_TIPO_ESTABELECIMENTO"].map(mapa)
    return tabela.drop_duplicates(["DT_COMPETENCIA", "CD_CNES"], keep="last")


def tratar_lt_mes(
    caminho: Path,
    ano: int,
    mes: int,
    dominios: dict[str, dict[str, str]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    dados = carregar_cnes_colunas(
        caminho, ["CNES", "CODUFMUN", "TP_LEITO", "CODLEITO", "QT_EXIST", "QT_SUS"]
    )
    tabela = pd.DataFrame({
        "DT_COMPETENCIA": pd.Timestamp(ano, mes, 1),
        "CD_CNES": normalizar_codigo(dados["CNES"], 7),
        "CD_MUNICIPIO_LT": normalizar_codigo(dados["CODUFMUN"], 6),
        "CD_TIPO_LEITO": normalizar_codigo(dados["TP_LEITO"]),
        "CD_ESPECIALIDADE_LEITO": normalizar_codigo(dados["CODLEITO"]),
        "QT_LEITOS_EXISTENTES": pd.to_numeric(dados["QT_EXIST"], errors="coerce").fillna(0).astype(int),
        "QT_LEITOS_SUS": pd.to_numeric(dados["QT_SUS"], errors="coerce").fillna(0).astype(int),
    })
    tabela["TP_LEITO"] = tabela["CD_TIPO_LEITO"].map(dominios["TIPOS_LEITO"])
    tabela["DS_ESPECIALIDADE"] = tabela["CD_ESPECIALIDADE_LEITO"].map(dominios["LEITOS"])

    # O pacote de dominios usado no projeto nao traz o codigo 96, embora ele
    # apareca nos arquivos LT. Mantemos a descricao oficial como complemento
    # explicito para nao deixar a categoria sem significado.
    especialidades_complementares = {
        "96": "SUPORTE VENTILATORIO PULMONAR - COVID-19",
    }
    tabela["DS_ESPECIALIDADE"] = tabela["DS_ESPECIALIDADE"].fillna(
        tabela["CD_ESPECIALIDADE_LEITO"].map(especialidades_complementares)
    )
    sem_dominio = tabela[tabela["TP_LEITO"].isna() | tabela["DS_ESPECIALIDADE"].isna()][[
        "DT_COMPETENCIA", "CD_TIPO_LEITO", "CD_ESPECIALIDADE_LEITO"
    ]].drop_duplicates()
    tabela["TP_LEITO"] = tabela["TP_LEITO"].fillna(
        "TIPO DE LEITO NAO LOCALIZADO NO DOMINIO CNES"
    )
    tabela["DS_ESPECIALIDADE"] = tabela["DS_ESPECIALIDADE"].fillna(
        "ESPECIALIDADE DE LEITO NAO LOCALIZADA NO DOMINIO CNES"
    )
    chaves = [
        "DT_COMPETENCIA", "CD_CNES", "CD_MUNICIPIO_LT", "TP_LEITO",
        "DS_ESPECIALIDADE",
    ]
    tabela = tabela.groupby(chaves, dropna=False)[
        ["QT_LEITOS_EXISTENTES", "QT_LEITOS_SUS"]
    ].sum().reset_index()
    return tabela, sem_dominio


def criar_tabelas_capacidade_de_leitos(
    leitos: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    categoria = (
        leitos[["TP_LEITO", "DS_ESPECIALIDADE"]].drop_duplicates()
        .sort_values(["TP_LEITO", "DS_ESPECIALIDADE"]).reset_index(drop=True)
    )
    categoria.insert(0, "ID_CATEGORIA_LEITO", np.arange(1, len(categoria) + 1))
    capacidade = leitos.merge(
        categoria, on=["TP_LEITO", "DS_ESPECIALIDADE"], how="left", validate="many_to_one"
    )[[
        "CD_CNES", "ID_CATEGORIA_LEITO", "DT_COMPETENCIA",
        "QT_LEITOS_EXISTENTES", "QT_LEITOS_SUS",
    ]]
    capacidade.insert(0, "ID_CAPACIDADE_LEITO", np.arange(1, len(capacidade) + 1))
    return categoria, capacidade


def criar_t_diagnostico(
    base: pd.DataFrame,
    referencia_cid10: pd.DataFrame,
    cids_sigtap: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    tabela = (
        base[["CD_CID10", "CD_CATEGORIA_CID10", "grupo_icsap"]]
        .drop_duplicates().rename(columns={"grupo_icsap": "ID_GRUPO_ICSAP"})
    )
    tabela = tabela.merge(referencia_cid10, on="CD_CID10", how="left", validate="many_to_one")
    tabela = tabela.merge(cids_sigtap, on="CD_CID10", how="left", validate="many_to_one")
    tabela["DS_DIAGNOSTICO"] = tabela["DS_DIAGNOSTICO"].fillna(
        tabela["DS_DIAGNOSTICO_SIGTAP"]
    )
    sem_descricao = tabela[tabela["DS_DIAGNOSTICO"].isna()][[
        "CD_CID10", "CD_CATEGORIA_CID10", "ID_GRUPO_ICSAP"
    ]].drop_duplicates("CD_CID10")
    tabela["DS_DIAGNOSTICO"] = tabela["DS_DIAGNOSTICO"].fillna(
        "DESCRICAO NAO LOCALIZADA NAS REFERENCIAS CID-10 E SIGTAP"
    )
    tabela["ID_GRUPO_ICSAP"] = tabela["ID_GRUPO_ICSAP"].astype("Int64")
    return (
        tabela[["CD_CID10", "CD_CATEGORIA_CID10", "DS_DIAGNOSTICO", "ID_GRUPO_ICSAP"]]
        .sort_values("CD_CID10").reset_index(drop=True),
        sem_descricao.sort_values("CD_CID10").reset_index(drop=True),
    )


def criar_t_procedimento(
    base: pd.DataFrame,
    sigtap: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    usados = base[["CD_PROCEDIMENTO"]].drop_duplicates()
    referencia = (
        sigtap.sort_values("DT_COMPETENCIA")
        .drop_duplicates("CD_PROCEDIMENTO", keep="last")
        .drop(columns="DT_COMPETENCIA")
    )
    tabela = usados.merge(referencia, on="CD_PROCEDIMENTO", how="left", validate="one_to_one")
    sem_descricao = tabela[tabela["DS_PROCEDIMENTO"].isna()][["CD_PROCEDIMENTO"]].copy()
    tabela["DS_PROCEDIMENTO"] = tabela["DS_PROCEDIMENTO"].fillna(
        "PROCEDIMENTO NAO LOCALIZADO NA REFERENCIA SIGTAP"
    )
    tabela["CD_GRUPO_PROCEDIMENTO"] = tabela["CD_GRUPO_PROCEDIMENTO"].fillna(
        tabela["CD_PROCEDIMENTO"].str[:2]
    )
    tabela["NM_GRUPO_PROCEDIMENTO"] = tabela["NM_GRUPO_PROCEDIMENTO"].fillna(
        "GRUPO NAO LOCALIZADO NA REFERENCIA SIGTAP"
    )
    tabela["TP_COMPLEXIDADE"] = tabela["TP_COMPLEXIDADE"].fillna(
        "COMPLEXIDADE NAO LOCALIZADA NA REFERENCIA SIGTAP"
    )
    return (
        tabela[[
            "CD_PROCEDIMENTO", "DS_PROCEDIMENTO", "CD_GRUPO_PROCEDIMENTO",
            "NM_GRUPO_PROCEDIMENTO", "TP_COMPLEXIDADE",
        ]].sort_values("CD_PROCEDIMENTO").reset_index(drop=True),
        sem_descricao.sort_values("CD_PROCEDIMENTO").reset_index(drop=True),
    )


def criar_t_estabelecimento(
    base: pd.DataFrame,
    st: pd.DataFrame,
    leitos: pd.DataFrame,
    snapshot: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    origem_sih = base[["CD_CNES", "CD_MUNICIPIO_INTERNACAO"]].rename(
        columns={"CD_MUNICIPIO_INTERNACAO": "CD_MUNICIPIO_SIH"}
    ).drop_duplicates("CD_CNES", keep="last")
    origem_st = st.sort_values("DT_COMPETENCIA").drop_duplicates("CD_CNES", keep="last")
    origem_lt = (
        leitos.sort_values("DT_COMPETENCIA")[["CD_CNES", "CD_MUNICIPIO_LT"]]
        .drop_duplicates("CD_CNES", keep="last")
    )
    codigos = pd.DataFrame({
        "CD_CNES": pd.concat([
            # A tabela inclui hospitais usados no SIH ou com leitos no LT.
            # O ST tambem possui consultorios e outros estabelecimentos que
            # nao participam desta modelagem; ele serve para enriquecer os
            # codigos selecionados, nao para criar milhares de linhas extras.
            origem_sih["CD_CNES"], origem_lt["CD_CNES"]
        ]).dropna().drop_duplicates()
    })
    tabela = (
        codigos.merge(origem_sih, on="CD_CNES", how="left", validate="one_to_one")
        .merge(origem_st, on="CD_CNES", how="left", validate="one_to_one")
        .merge(origem_lt, on="CD_CNES", how="left", validate="one_to_one")
        .merge(snapshot, on="CD_CNES", how="left", validate="one_to_one")
    )
    tabela["CD_MUNICIPIO"] = (
        tabela["CD_MUNICIPIO_ST"].fillna(tabela["CD_MUNICIPIO_LT"])
        .fillna(tabela["CD_MUNICIPIO_SIH"])
    )
    if "CD_MUNICIPIO_SNAPSHOT" in tabela:
        tabela["CD_MUNICIPIO"] = tabela["CD_MUNICIPIO"].fillna(
            tabela["CD_MUNICIPIO_SNAPSHOT"]
        )
    sem_nome = tabela[tabela["NM_ESTABELECIMENTO"].isna()][[
        "CD_CNES", "CD_MUNICIPIO"
    ]].copy()
    sem_tipo = tabela[tabela["TP_ESTABELECIMENTO"].isna()][[
        "CD_CNES", "CD_TIPO_ESTABELECIMENTO"
    ]].copy()
    tabela["NM_ESTABELECIMENTO"] = tabela["NM_ESTABELECIMENTO"].fillna(
        "ESTABELECIMENTO NAO LOCALIZADO NA REFERENCIA CNES DE 12/2025"
    )
    tabela["TP_ESTABELECIMENTO"] = tabela["TP_ESTABELECIMENTO"].fillna(
        "TIPO NAO LOCALIZADO NA REFERENCIA CNES"
    )
    tabela["CD_MUNICIPIO"] = tabela["CD_MUNICIPIO"].fillna("MUNICIPIO NAO LOCALIZADO")
    return (
        tabela[["CD_CNES", "CD_MUNICIPIO", "NM_ESTABELECIMENTO", "TP_ESTABELECIMENTO"]]
        .sort_values("CD_CNES").reset_index(drop=True),
        sem_nome.sort_values("CD_CNES").reset_index(drop=True),
        sem_tipo.sort_values("CD_CNES").reset_index(drop=True),
    )


def carregar_cache_referencia(
    caminho_cache: Path,
    caminho_origem: Path,
) -> object | None:
    if not caminho_cache.is_file():
        return None
    try:
        pacote = pd.read_pickle(caminho_cache)
        origem = caminho_origem.stat()
        if (
            pacote.get("versao") == VERSAO_CACHE_REFERENCIAS
            and Path(str(pacote.get("caminho_origem", ""))).name.lower()
            == caminho_origem.name.lower()
            and pacote.get("tamanho_origem") == origem.st_size
            and pacote.get("modificado_origem") == origem.st_mtime_ns
        ):
            return pacote["dados"]
    except (OSError, KeyError, AttributeError, ValueError, EOFError):
        pass
    return None


def salvar_cache_referencia(
    caminho_cache: Path,
    caminho_origem: Path,
    dados: object,
) -> None:
    caminho_cache.parent.mkdir(parents=True, exist_ok=True)
    origem = caminho_origem.stat()
    pd.to_pickle({
        "versao": VERSAO_CACHE_REFERENCIAS,
        "caminho_origem": str(caminho_origem.resolve()),
        "tamanho_origem": origem.st_size,
        "modificado_origem": origem.st_mtime_ns,
        "dados": dados,
    }, caminho_cache)


def criar_dataset_aed_auditado(
    perfil: pd.DataFrame,
    resumo_aih: pd.DataFrame,
    resumo_assistencial: pd.DataFrame,
    t_municipio: pd.DataFrame,
    t_estabelecimento: pd.DataFrame,
    t_diagnostico: pd.DataFrame,
    t_grupo_icsap: pd.DataFrame,
    t_procedimento: pd.DataFrame,
    t_faixa_etaria: pd.DataFrame,
    t_sexo: pd.DataFrame,
    t_raca_cor: pd.DataFrame,
    t_capacidade_leito: pd.DataFrame,
) -> pd.DataFrame:
    """Cria o arquivo de AED sem multiplicar perfis pelas categorias de leito."""
    dataset = (
        perfil.merge(resumo_aih, on="ID_PERFIL_INTERNACAO", how="left", validate="one_to_one")
        .merge(resumo_assistencial, on="ID_PERFIL_INTERNACAO", how="left", validate="one_to_one")
        .merge(
            t_municipio, left_on="CD_MUNICIPIO_RESIDENCIA", right_on="CD_MUNICIPIO",
            how="left", validate="many_to_one",
        )
        .merge(t_estabelecimento, on="CD_CNES", how="left", validate="many_to_one")
        .merge(t_diagnostico, on="CD_CID10", how="left", validate="many_to_one")
        .merge(
            t_grupo_icsap[["ID_GRUPO_ICSAP", "NM_GRUPO_ICSAP"]],
            on="ID_GRUPO_ICSAP", how="left", validate="many_to_one",
        )
        .merge(t_procedimento, on="CD_PROCEDIMENTO", how="left", validate="many_to_one")
        .merge(t_faixa_etaria, on="ID_FAIXA_ETARIA", how="left", validate="many_to_one")
        .merge(t_sexo, on="CD_SEXO", how="left", validate="many_to_one")
        .merge(t_raca_cor, on="CD_RACA_COR", how="left", validate="many_to_one")
    )
    # Indicadores municipais: o denominador das internacoes exclui O80-O84.
    municipal = dataset.groupby(
        ["DT_COMPETENCIA", "CD_MUNICIPIO_RESIDENCIA"], dropna=False
    ).agg(
        QT_INTERNACOES_ICSAP_MES=("QT_INTERNACOES_ICSAP", "sum"),
        QT_INTERNACOES_ELEGIVEIS_MES=("QT_INTERNACOES_ELEGIVEIS_ICSAP", "sum"),
        VL_ICSAP_MUNICIPIO_MES=("VL_TOTAL_ICSAP", "sum"),
        VL_ELEGIVEL_IEP_MUNICIPIO_MES=("VL_TOTAL_ELEGIVEL_IEP", "sum"),
    ).reset_index()
    municipal["PC_INTERNACOES_ICSAP_MUNICIPIO_MES"] = (
        municipal["QT_INTERNACOES_ICSAP_MES"]
        / municipal["QT_INTERNACOES_ELEGIVEIS_MES"].replace(0, np.nan) * 100
    ).round(2)
    municipal["IEP_MUNICIPIO_MES"] = (
        100 - municipal["VL_ICSAP_MUNICIPIO_MES"]
        / municipal["VL_ELEGIVEL_IEP_MUNICIPIO_MES"].replace(0, np.nan) * 100
    ).round(2)
    dataset = dataset.merge(
        municipal, on=["DT_COMPETENCIA", "CD_MUNICIPIO_RESIDENCIA"],
        how="left", validate="many_to_one",
    )
    hospital = dataset.groupby(["DT_COMPETENCIA", "CD_CNES"], dropna=False).agg(
        QT_SAIDAS_HOSPITAL_MES=("QT_SAIDAS_HOSPITALARES", "sum"),
        QT_DIAS_HOSPITAL_MES=("QT_DIAS_PERMANENCIA", "sum"),
        QT_OBITOS_HOSPITAL_MES=("QT_OBITOS", "sum"),
    ).reset_index()

    # Permanencia media e mortalidade hospitalar usam saidas como denominador.
    # Isso segue o significado assistencial desses indicadores e evita dividir
    # continuidades por apenas uma AIH inicial. Meses sem nenhuma saida ficam
    # explicitamente marcados como nao calculaveis; o ETL nao inventa zero.
    denominador_saida = hospital["QT_SAIDAS_HOSPITAL_MES"].replace(0, np.nan)
    hospital["PERMANENCIA_MEDIA_HOSPITAL_MES"] = (
        hospital["QT_DIAS_HOSPITAL_MES"]
        / denominador_saida
    ).round(2)
    hospital["PC_OBITOS_HOSPITAL_MES"] = (
        hospital["QT_OBITOS_HOSPITAL_MES"]
        / denominador_saida * 100
    ).round(2)
    hospital["FL_INDICADOR_HOSPITAL_CALCULAVEL"] = (
        hospital["QT_SAIDAS_HOSPITAL_MES"].gt(0).astype("int8")
    )
    dataset = dataset.merge(
        hospital, on=["DT_COMPETENCIA", "CD_CNES"], how="left", validate="many_to_one"
    )
    capacidade = t_capacidade_leito.groupby(
        ["DT_COMPETENCIA", "CD_CNES"], dropna=False
    )[["QT_LEITOS_EXISTENTES", "QT_LEITOS_SUS"]].sum().reset_index().rename(columns={
        "QT_LEITOS_EXISTENTES": "QT_LEITOS_EXISTENTES_TOTAL",
        "QT_LEITOS_SUS": "QT_LEITOS_SUS_TOTAL",
    })
    dataset = dataset.merge(
        capacidade, on=["DT_COMPETENCIA", "CD_CNES"], how="left", validate="many_to_one"
    )
    dataset["FL_ICSAP"] = dataset["ID_GRUPO_ICSAP"].notna().astype("int8")
    colunas = [
        "ID_PERFIL_INTERNACAO", "DT_COMPETENCIA",
        "CD_MUNICIPIO_RESIDENCIA", "NM_MUNICIPIO", "SG_UF", "NM_REGIAO_SAUDE",
        "CD_CNES", "NM_ESTABELECIMENTO", "TP_ESTABELECIMENTO",
        "CD_CID10", "CD_CATEGORIA_CID10", "DS_DIAGNOSTICO",
        "ID_GRUPO_ICSAP", "NM_GRUPO_ICSAP", "FL_ICSAP",
        "CD_PROCEDIMENTO", "DS_PROCEDIMENTO", "CD_GRUPO_PROCEDIMENTO",
        "NM_GRUPO_PROCEDIMENTO", "TP_COMPLEXIDADE",
        "ID_FAIXA_ETARIA", "DS_FAIXA_ETARIA", "CD_SEXO", "DS_SEXO",
        "CD_RACA_COR", "DS_RACA_COR", "QT_REGISTROS_AIH",
        "QT_CONTINUIDADES_AIH", "VL_TOTAL_AIH", "QT_INTERNACOES",
        "QT_INTERNACOES_ELEGIVEIS_ICSAP", "QT_INTERNACOES_ICSAP",
        "VL_TOTAL_ICSAP", "VL_TOTAL_ELEGIVEL_IEP", "QT_DIAS_PERMANENCIA",
        "QT_SAIDAS_HOSPITALARES", "QT_OBITOS",
        "PC_INTERNACOES_ICSAP_MUNICIPIO_MES", "IEP_MUNICIPIO_MES",
        "QT_SAIDAS_HOSPITAL_MES", "FL_INDICADOR_HOSPITAL_CALCULAVEL",
        "PERMANENCIA_MEDIA_HOSPITAL_MES", "PC_OBITOS_HOSPITAL_MES",
        "QT_LEITOS_EXISTENTES_TOTAL",
        "QT_LEITOS_SUS_TOTAL",
    ]
    return dataset[colunas].sort_values([
        "DT_COMPETENCIA", "CD_MUNICIPIO_RESIDENCIA", "CD_CNES"
    ]).reset_index(drop=True)


def validar_modelo_auditado(
    tabelas: dict[str, pd.DataFrame],
    dataset_aed: pd.DataFrame,
    agregados: pd.DataFrame,
    relatorios: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Valida conteudo, chaves e reconciliacao antes de exportar."""
    linhas: list[tuple[str, int, str]] = []
    for nome, tabela in tabelas.items():
        linhas.append((f"{nome} vazia", int(tabela.empty), "ERRO"))
    chaves_unicas = {
        "T_GRUPO_ICSAP": ["ID_GRUPO_ICSAP"],
        "T_DIAGNOSTICO": ["CD_CID10"],
        "T_MUNICIPIO": ["CD_MUNICIPIO"],
        "T_ESTABELECIMENTO": ["CD_CNES"],
        "T_PROCEDIMENTO": ["CD_PROCEDIMENTO"],
        "T_FAIXA_ETARIA": ["ID_FAIXA_ETARIA"],
        "T_SEXO": ["CD_SEXO"], "T_RACA_COR": ["CD_RACA_COR"],
        "T_PERFIL_INTERNACAO": ["ID_PERFIL_INTERNACAO"],
        "T_RESUMO_AIH": ["ID_RESUMO_AIH"],
        "T_RESUMO_ASSISTENCIAL": ["ID_RESUMO_ASSISTENCIAL"],
        "T_CATEGORIA_LEITO": ["ID_CATEGORIA_LEITO"],
        "T_CAPACIDADE_LEITO": ["ID_CAPACIDADE_LEITO"],
    }
    for nome, chaves in chaves_unicas.items():
        linhas.append((
            f"Chave duplicada em {nome}",
            int(tabelas[nome].duplicated(chaves).sum()), "ERRO",
        ))
    if len(tabelas["T_GRUPO_ICSAP"]) != 19:
        linhas.append(("Quantidade diferente de 19 grupos ICSAP", 1, "ERRO"))
    else:
        linhas.append(("Quantidade diferente de 19 grupos ICSAP", 0, "ERRO"))
    # Campos descritivos que agora precisam estar preenchidos.
    obrigatorios = {
        "T_MUNICIPIO": ["NM_MUNICIPIO", "SG_UF", "NM_REGIAO_SAUDE"],
        "T_ESTABELECIMENTO": ["CD_MUNICIPIO", "NM_ESTABELECIMENTO", "TP_ESTABELECIMENTO"],
        "T_PROCEDIMENTO": ["DS_PROCEDIMENTO", "NM_GRUPO_PROCEDIMENTO", "TP_COMPLEXIDADE"],
        "T_DIAGNOSTICO": ["DS_DIAGNOSTICO"],
        "T_FAIXA_ETARIA": ["DS_FAIXA_ETARIA"],
        "T_CATEGORIA_LEITO": ["TP_LEITO", "DS_ESPECIALIDADE"],
    }
    for nome, colunas in obrigatorios.items():
        quantidade = int(tabelas[nome][colunas].isna().sum().sum())
        linhas.append((f"Campos obrigatorios nulos em {nome}", quantidade, "ERRO"))
    perfil = tabelas["T_PERFIL_INTERNACAO"]
    linhas.append((
        "Campos de classificacao nulos em T_PERFIL_INTERNACAO",
        int(perfil[CHAVES_PERFIL].isna().sum().sum()), "ERRO",
    ))
    fks = [
        ("Perfil municipio", perfil["CD_MUNICIPIO_RESIDENCIA"], tabelas["T_MUNICIPIO"]["CD_MUNICIPIO"]),
        ("Perfil estabelecimento", perfil["CD_CNES"], tabelas["T_ESTABELECIMENTO"]["CD_CNES"]),
        ("Perfil diagnostico", perfil["CD_CID10"], tabelas["T_DIAGNOSTICO"]["CD_CID10"]),
        ("Perfil procedimento", perfil["CD_PROCEDIMENTO"], tabelas["T_PROCEDIMENTO"]["CD_PROCEDIMENTO"]),
        ("Perfil faixa etaria", perfil["ID_FAIXA_ETARIA"], tabelas["T_FAIXA_ETARIA"]["ID_FAIXA_ETARIA"]),
        ("Perfil sexo", perfil["CD_SEXO"], tabelas["T_SEXO"]["CD_SEXO"]),
        ("Perfil raca cor", perfil["CD_RACA_COR"], tabelas["T_RACA_COR"]["CD_RACA_COR"]),
        ("Estabelecimento municipio", tabelas["T_ESTABELECIMENTO"]["CD_MUNICIPIO"], tabelas["T_MUNICIPIO"]["CD_MUNICIPIO"]),
        ("Resumo AIH perfil", tabelas["T_RESUMO_AIH"]["ID_PERFIL_INTERNACAO"], perfil["ID_PERFIL_INTERNACAO"]),
        ("Resumo assistencial perfil", tabelas["T_RESUMO_ASSISTENCIAL"]["ID_PERFIL_INTERNACAO"], perfil["ID_PERFIL_INTERNACAO"]),
        ("Capacidade estabelecimento", tabelas["T_CAPACIDADE_LEITO"]["CD_CNES"], tabelas["T_ESTABELECIMENTO"]["CD_CNES"]),
        ("Capacidade categoria", tabelas["T_CAPACIDADE_LEITO"]["ID_CATEGORIA_LEITO"], tabelas["T_CATEGORIA_LEITO"]["ID_CATEGORIA_LEITO"]),
    ]
    for nome, origem, destino in fks:
        linhas.append((f"Chave sem correspondencia: {nome}", int((~origem.isin(destino)).sum()), "ERRO"))
    grupos_diagnostico = tabelas["T_DIAGNOSTICO"]["ID_GRUPO_ICSAP"].dropna()
    linhas.append((
        "Chave sem correspondencia: Diagnostico grupo ICSAP",
        int((~grupos_diagnostico.isin(tabelas["T_GRUPO_ICSAP"]["ID_GRUPO_ICSAP"])).sum()),
        "ERRO",
    ))
    capacidade = tabelas["T_CAPACIDADE_LEITO"]
    linhas.append(("Leitos com quantidade negativa", int(((capacidade["QT_LEITOS_EXISTENTES"] < 0) | (capacidade["QT_LEITOS_SUS"] < 0)).sum()), "ERRO"))
    linhas.append(("Leitos SUS maiores que existentes", int((capacidade["QT_LEITOS_SUS"] > capacidade["QT_LEITOS_EXISTENTES"]).sum()), "ERRO"))
    resumo_aih = tabelas["T_RESUMO_AIH"]
    resumo_assist = tabelas["T_RESUMO_ASSISTENCIAL"]
    linhas.append((
        "Obitos maiores que saidas hospitalares",
        int((resumo_assist["QT_OBITOS"] > resumo_assist["QT_SAIDAS_HOSPITALARES"]).sum()),
        "ERRO",
    ))
    linhas.append((
        "Nomes de municipio com prefixo da UF",
        int(tabelas["T_MUNICIPIO"]["NM_MUNICIPIO"].astype("string")
            .str.match(r"^\s*SP\s*-", case=False, na=False).sum()),
        "ERRO",
    ))
    linhas.append((
        "Complexidade de procedimento nao mapeada",
        int(tabelas["T_PROCEDIMENTO"]["TP_COMPLEXIDADE"].astype("string")
            .str.contains(r"NAO MAPEADA|NAO LOCALIZADA", case=False, na=False).sum()),
        "ERRO",
    ))
    linhas.append((
        "Categoria de leito sem descricao do dominio",
        int(tabelas["T_CATEGORIA_LEITO"]["DS_ESPECIALIDADE"].astype("string")
            .str.contains("NAO LOCALIZADA", case=False, na=False).sum()),
        "ERRO",
    ))
    medidas = {
        "QT_REGISTROS_AIH": resumo_aih["QT_REGISTROS_AIH"].sum(),
        "VL_TOTAL_AIH": resumo_aih["VL_TOTAL_AIH"].sum(),
        "QT_INTERNACOES": resumo_assist["QT_INTERNACOES"].sum(),
        "QT_INTERNACOES_ICSAP": resumo_assist["QT_INTERNACOES_ICSAP"].sum(),
        "VL_TOTAL_ICSAP": resumo_assist["VL_TOTAL_ICSAP"].sum(),
        "QT_DIAS_PERMANENCIA": resumo_assist["QT_DIAS_PERMANENCIA"].sum(),
        "QT_SAIDAS_HOSPITALARES": resumo_assist["QT_SAIDAS_HOSPITALARES"].sum(),
        "QT_OBITOS": resumo_assist["QT_OBITOS"].sum(),
    }
    for medida, final in medidas.items():
        diferenca = abs(float(agregados[medida].sum()) - float(final))
        tolerancia = 0.01 if medida.startswith("VL_") else 0
        linhas.append((f"Reconciliacao {medida}", int(diferenca > tolerancia), "ERRO"))
    linhas.append(("Dataset AED com quantidade diferente do perfil", int(len(dataset_aed) != len(perfil)), "ERRO"))
    linhas.append(("Quantidade de municipios diferente de 645", int(len(tabelas["T_MUNICIPIO"]) != 645), "ERRO"))
    linhas.append((
        "IEP fora de 0 a 100 ou nulo",
        int((~dataset_aed["IEP_MUNICIPIO_MES"].between(0, 100)).sum()
            + dataset_aed["IEP_MUNICIPIO_MES"].isna().sum()), "ERRO",
    ))
    linhas.append((
        "Percentual ICSAP fora de 0 a 100 ou nulo",
        int((~dataset_aed["PC_INTERNACOES_ICSAP_MUNICIPIO_MES"].between(0, 100)).sum()
            + dataset_aed["PC_INTERNACOES_ICSAP_MUNICIPIO_MES"].isna().sum()), "ERRO",
    ))
    tem_saida = dataset_aed["QT_SAIDAS_HOSPITAL_MES"].gt(0)
    indicador_disponivel = dataset_aed["FL_INDICADOR_HOSPITAL_CALCULAVEL"].eq(1)
    linhas.append((
        "Sinalizador hospitalar diferente da existencia de saidas",
        int(tem_saida.ne(indicador_disponivel).sum()), "ERRO",
    ))
    linhas.append((
        "Indicador hospitalar nulo apesar de existir saida",
        int(dataset_aed.loc[tem_saida, [
            "PERMANENCIA_MEDIA_HOSPITAL_MES", "PC_OBITOS_HOSPITAL_MES"
        ]].isna().any(axis=1).sum()), "ERRO",
    ))
    linhas.append((
        "Indicador hospitalar preenchido sem existir saida",
        int(dataset_aed.loc[~tem_saida, [
            "PERMANENCIA_MEDIA_HOSPITAL_MES", "PC_OBITOS_HOSPITAL_MES"
        ]].notna().any(axis=1).sum()), "ERRO",
    ))
    mortalidade_preenchida = dataset_aed["PC_OBITOS_HOSPITAL_MES"].dropna()
    linhas.append((
        "Percentual de obitos hospitalares fora de 0 a 100",
        int((~mortalidade_preenchida.between(0, 100)).sum()), "ERRO",
    ))
    permanencia_preenchida = dataset_aed["PERMANENCIA_MEDIA_HOSPITAL_MES"].dropna()
    linhas.append((
        "Permanencia media hospitalar negativa",
        int((permanencia_preenchida < 0).sum()), "ERRO",
    ))
    linhas.append((
        "Capacidade hospitalar ausente no dataset AED",
        int(dataset_aed[["QT_LEITOS_EXISTENTES_TOTAL", "QT_LEITOS_SUS_TOTAL"]].isna().any(axis=1).sum()),
        "ERRO",
    ))
    for nome, relatorio in relatorios.items():
        linhas.append((nome.replace(".csv", "").replace("_", " "), len(relatorio), "AVISO"))
    validacao = pd.DataFrame(linhas, columns=["VERIFICACAO", "QUANTIDADE_PROBLEMAS", "TIPO"])
    validacao["RESULTADO"] = np.select(
        [
            validacao["QUANTIDADE_PROBLEMAS"].eq(0),
            validacao["TIPO"].eq("AVISO"),
        ], ["APROVADO", "AVISO"], default="REVISAR"
    )
    return validacao


def exportar_modelo_auditado(
    pasta_saida: Path,
    tabelas: dict[str, pd.DataFrame],
    dataset_aed: pd.DataFrame,
    validacao: pd.DataFrame,
    relatorios: dict[str, pd.DataFrame],
) -> None:
    """Substitui somente os CSVs gerados, evitando misturar versoes antigas."""
    destinos = [pasta_saida / "tabelas_oracle", pasta_saida / "aed", pasta_saida / "validacoes"]
    for pasta in destinos:
        pasta.mkdir(parents=True, exist_ok=True)
        for antigo in pasta.glob("*.csv"):
            antigo.unlink()
    for nome, tabela in tabelas.items():
        exportar_csv(tabela, destinos[0] / f"{nome}.csv")
    exportar_csv(dataset_aed, destinos[1] / "DATASET_AED_HUBSUS360.csv")
    exportar_csv(validacao, destinos[2] / "VALIDACAO_MODELO_FINAL.csv")
    for nome, tabela in relatorios.items():
        exportar_csv(tabela, destinos[2] / nome)
    manifesto = []
    for caminho in sorted(pasta_saida.rglob("*.csv")):
        with caminho.open("r", encoding="utf-8-sig", errors="replace") as arquivo:
            linhas = max(sum(1 for _ in arquivo) - 1, 0)
        manifesto.append({
            "ARQUIVO": str(caminho.relative_to(pasta_saida)),
            "LINHAS": linhas, "TAMANHO_BYTES": caminho.stat().st_size,
        })
    exportar_csv(pd.DataFrame(manifesto), destinos[2] / "MANIFESTO_ARQUIVOS.csv")


def validar_base_mestra_auditada(
    base_final: pd.DataFrame,
    base_tratada: pd.DataFrame,
    referencia_icsap: pd.DataFrame,
) -> pd.DataFrame:
    """Mantem as verificacoes mensais sem exigir os 19 grupos em todo mes."""
    validacao = validar_base_mestra(base_final, base_tratada, referencia_icsap)
    mascara = validacao["verificacao"].eq("Diferença na quantidade de grupos ICSAP")
    validacao.loc[mascara, "quantidade_problemas"] = 0
    validacao.loc[mascara, "resultado"] = "APROVADO"
    return validacao


def executar_etl_hubsus360_auditado(argumentos: argparse.Namespace) -> None:
    """Executa a versao auditada e preenche as 13 tabelas definidas."""
    raiz = localizar_raiz_referencias(argumentos.referencias)
    pastas = {chave: raiz / nome for chave, nome in PASTAS_REFERENCIAS.items()}
    for chave, pasta in pastas.items():
        if chave != "sih" and not pasta.is_dir():
            raise FileNotFoundError(f"Pasta obrigatoria ausente: {pasta}")
    pasta_saida = argumentos.saida.expanduser().resolve()
    pasta_cache = Path(__file__).resolve().parent / "cache_hubsus360"
    pasta_cache_sih = pasta_cache / "sih"
    pasta_cache_ref = pasta_cache / "referencias"
    pasta_cache_sih.mkdir(parents=True, exist_ok=True)

    LOG_ETL.info("1/10 - Localizando as competencias do SIH...")
    entradas_sih = argumentos.sih or [pastas["sih"]]
    entradas = localizar_arquivos_locais(
        entradas_sih, argumentos.anos, argumentos.meses
    )
    competencias = sorted(entradas)
    arquivos_st = localizar_arquivos_cnes(pastas["st"])
    arquivos_lt = localizar_arquivos_cnes(pastas["lt"])
    arquivos_sigtap = localizar_sigtap(pastas["sigtap"])
    faltantes = []
    for ano, mes in competencias:
        if ("ST", ano, mes) not in arquivos_st:
            faltantes.append(f"STSP{ano % 100:02d}{mes:02d}")
        if ("LT", ano, mes) not in arquivos_lt:
            faltantes.append(f"LTSP{ano % 100:02d}{mes:02d}")
        if (ano, mes) not in arquivos_sigtap:
            faltantes.append(f"SIGTAP {ano}-{mes:02d}")
    if faltantes:
        raise FileNotFoundError(
            "Faltam referencias das competencias selecionadas: " + ", ".join(faltantes)
        )
    referencia_icsap = criar_referencia_icsap()

    LOG_ETL.info("2/10 - Carregando CID-10, dominios, regioes e estabelecimentos...")
    pasta_cid = argumentos.cid10.expanduser().resolve() if argumentos.cid10 else pastas["cid10"]
    if pasta_cid.is_file():
        pasta_cid = pasta_cid.parent
    referencia_cid10 = carregar_cid10_completo(pasta_cid)
    arquivo_dominios = primeiro_arquivo(
        pastas["dominios"], ["*DOMINIOS*.ZIP", "*dominios*.zip", "*.xlsx", "*.xls"],
        "dominios do CNES",
    )
    dominios = carregar_dominios_cnes(arquivo_dominios)
    arquivo_regioes = primeiro_arquivo(
        pastas["regioes"], ["*.zip", "*.csv"], "regioes de saude"
    )
    t_municipio = carregar_regioes_saude(arquivo_regioes)
    if argumentos.cnes_estabelecimentos:
        arquivo_snapshot = argumentos.cnes_estabelecimentos.expanduser().resolve()
    else:
        arquivo_snapshot = primeiro_arquivo(
            pastas["estabelecimentos"], ["*.csv", "*.zip"],
            "estabelecimentos do CNES",
        )
    snapshot = carregar_snapshot_estabelecimentos(arquivo_snapshot)

    LOG_ETL.info("3/10 - Tratando SIH e aplicando os 19 grupos ICSAP...")
    agregados_mensais: list[pd.DataFrame] = []
    contextos: list[pd.DataFrame] = []
    for indice, (ano, mes) in enumerate(competencias, start=1):
        caminho = entradas[(ano, mes)]
        LOG_ETL.info("SIH %s/%s: %s", indice, len(competencias), caminho.name)
        cache = None if argumentos.refazer_cache else carregar_cache_mes(
            pasta_cache_sih, caminho, ano, mes
        )
        if cache is not None:
            agregado_mes, contexto_mes = cache
            LOG_ETL.info("Cache SIH utilizado para %s-%02d.", ano, mes)
        else:
            dados_brutos = carregar_mes_local(caminho)
            validar_competencia(dados_brutos, ano, mes)
            base = classificar_icsap(classificar_aihs(padronizar_base(dados_brutos)), referencia_icsap)
            mestra = construir_base_mestra(base, caminho.name)
            exigir_aprovacao(
                "Base SIH mensal",
                validar_base_mestra_auditada(mestra, base, referencia_icsap),
            )
            preparada = preparar_modelo_final(mestra)
            agregado_mes = agregar_perfis_mes(preparada)
            contexto_mes = preparada[[
                "CD_MUNICIPIO_INTERNACAO", "CD_CNES", "CD_CID10",
                "CD_CATEGORIA_CID10", "grupo_icsap", "CD_PROCEDIMENTO",
                "complexidade_descricao", "CD_SEXO", "sexo_descricao",
                "CD_RACA_COR", "raca_cor_descricao",
            ]].drop_duplicates()
            salvar_cache_mes(
                pasta_cache_sih, caminho, ano, mes, agregado_mes, contexto_mes
            )
        agregados_mensais.append(agregado_mes)
        contextos.append(contexto_mes)
    agregados = pd.concat(agregados_mensais, ignore_index=True)
    contexto = pd.concat(contextos, ignore_index=True).drop_duplicates().reset_index(drop=True)

    LOG_ETL.info("4/10 - Lendo SIGTAP por competencia...")
    partes_proc: list[pd.DataFrame] = []
    partes_cid_sigtap: list[pd.DataFrame] = []
    for ano, mes in competencias:
        caminho = arquivos_sigtap[(ano, mes)]
        cache_path = pasta_cache_ref / f"SIGTAP_{ano}_{mes:02d}.pkl"
        dados = None if argumentos.refazer_cache else carregar_cache_referencia(cache_path, caminho)
        if dados is None:
            dados = carregar_sigtap_mes(caminho, ano, mes)
            salvar_cache_referencia(cache_path, caminho, dados)
        procedimentos_mes, cids_mes = dados
        partes_proc.append(procedimentos_mes)
        partes_cid_sigtap.append(cids_mes)
    sigtap = pd.concat(partes_proc, ignore_index=True)
    cids_sigtap = (
        pd.concat(partes_cid_sigtap, ignore_index=True)
        .drop_duplicates("CD_CID10", keep="last")
    )

    LOG_ETL.info("5/10 - Lendo ST e LT do CNES por competencia...")
    partes_st: list[pd.DataFrame] = []
    partes_lt: list[pd.DataFrame] = []
    partes_sem_dominio: list[pd.DataFrame] = []
    for ano, mes in competencias:
        caminho_st = arquivos_st[("ST", ano, mes)]
        cache_st = pasta_cache_ref / f"ST_{ano}_{mes:02d}.pkl"
        st_mes = None if argumentos.refazer_cache else carregar_cache_referencia(cache_st, caminho_st)
        if st_mes is None:
            st_mes = tratar_st_mes(caminho_st, ano, mes, dominios)
            salvar_cache_referencia(cache_st, caminho_st, st_mes)
        partes_st.append(st_mes)
        caminho_lt = arquivos_lt[("LT", ano, mes)]
        cache_lt = pasta_cache_ref / f"LT_{ano}_{mes:02d}.pkl"
        dados_lt = None if argumentos.refazer_cache else carregar_cache_referencia(cache_lt, caminho_lt)
        if dados_lt is None:
            dados_lt = tratar_lt_mes(caminho_lt, ano, mes, dominios)
            salvar_cache_referencia(cache_lt, caminho_lt, dados_lt)
        lt_mes, sem_dominio_mes = dados_lt
        partes_lt.append(lt_mes)
        partes_sem_dominio.append(sem_dominio_mes)
    st = pd.concat(partes_st, ignore_index=True)
    leitos = pd.concat(partes_lt, ignore_index=True)
    codigos_sem_dominio = pd.concat(partes_sem_dominio, ignore_index=True).drop_duplicates()

    LOG_ETL.info("6/10 - Criando e preenchendo as 13 tabelas...")
    t_grupo_icsap = criar_t_grupo_icsap(referencia_icsap)
    t_diagnostico, cids_sem_descricao = criar_t_diagnostico(
        contexto, referencia_cid10, cids_sigtap
    )
    t_procedimento, procedimentos_sem_descricao = criar_t_procedimento(contexto, sigtap)
    t_estabelecimento, estabelecimentos_sem_nome, estabelecimentos_sem_tipo = criar_t_estabelecimento(
        contexto, st, leitos, snapshot
    )
    t_categoria_leito, t_capacidade_leito = criar_tabelas_capacidade_de_leitos(leitos)
    t_faixa_etaria = criar_t_faixa_etaria()
    t_sexo = criar_t_sexo(contexto)
    t_raca_cor = criar_t_raca_cor(contexto)
    t_perfil, t_resumo_aih, t_resumo_assistencial = criar_tabelas_perfil_e_resumos(agregados)
    tabelas = {
        "T_GRUPO_ICSAP": t_grupo_icsap,
        "T_DIAGNOSTICO": t_diagnostico,
        "T_MUNICIPIO": t_municipio,
        "T_ESTABELECIMENTO": t_estabelecimento,
        "T_PROCEDIMENTO": t_procedimento,
        "T_FAIXA_ETARIA": t_faixa_etaria,
        "T_SEXO": t_sexo,
        "T_RACA_COR": t_raca_cor,
        "T_PERFIL_INTERNACAO": t_perfil,
        "T_RESUMO_AIH": t_resumo_aih,
        "T_RESUMO_ASSISTENCIAL": t_resumo_assistencial,
        "T_CATEGORIA_LEITO": t_categoria_leito,
        "T_CAPACIDADE_LEITO": t_capacidade_leito,
    }
    relatorios = {
        "CIDS_SEM_DESCRICAO.csv": cids_sem_descricao,
        "PROCEDIMENTOS_SEM_DESCRICAO.csv": procedimentos_sem_descricao,
        "ESTABELECIMENTOS_SEM_NOME.csv": estabelecimentos_sem_nome,
        "ESTABELECIMENTOS_SEM_TIPO.csv": estabelecimentos_sem_tipo,
        "CODIGOS_CNES_SEM_DOMINIO.csv": codigos_sem_dominio,
    }

    LOG_ETL.info("7/10 - Criando o arquivo unico da AED...")
    dataset_aed = criar_dataset_aed_auditado(
        t_perfil, t_resumo_aih, t_resumo_assistencial, t_municipio,
        t_estabelecimento, t_diagnostico, t_grupo_icsap, t_procedimento,
        t_faixa_etaria, t_sexo, t_raca_cor, t_capacidade_leito,
    )
    relatorios["HOSPITAIS_MES_SEM_SAIDA.csv"] = (
        dataset_aed.loc[
            dataset_aed["FL_INDICADOR_HOSPITAL_CALCULAVEL"].eq(0),
            [
                "DT_COMPETENCIA", "CD_CNES", "NM_ESTABELECIMENTO",
                "QT_SAIDAS_HOSPITAL_MES",
            ],
        ]
        .drop_duplicates(["DT_COMPETENCIA", "CD_CNES"])
        .sort_values(["DT_COMPETENCIA", "CD_CNES"])
        .reset_index(drop=True)
    )
    LOG_ETL.info("8/10 - Executando a auditoria final...")
    validacao = validar_modelo_auditado(tabelas, dataset_aed, agregados, relatorios)
    pendencias = validacao[validacao["RESULTADO"].eq("REVISAR")]
    if not pendencias.empty:
        raise ValueError(
            "A auditoria final encontrou problemas que impedem a exportacao:\n"
            + pendencias.to_string(index=False)
        )

    LOG_ETL.info("9/10 - Exportando os CSVs...")
    exportar_modelo_auditado(pasta_saida, tabelas, dataset_aed, validacao, relatorios)
    LOG_ETL.info("10/10 - ETL concluido com %s competencias.", len(competencias))
    LOG_ETL.info("Resultados: %s", pasta_saida)
    for nome, tabela in tabelas.items():
        LOG_ETL.info("%s: %s linhas", nome, len(tabela))
    LOG_ETL.info("DATASET_AED_HUBSUS360: %s linhas", len(dataset_aed))
    if argumentos.gerar_zip:
        caminho_zip = shutil.make_archive(str(pasta_saida), "zip", root_dir=pasta_saida)
        LOG_ETL.info("ZIP gerado: %s", caminho_zip)


def criar_tabelas_capacidade(
    pasta_cnes: Path | None,
    competencias: list[tuple[int, int]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cria T_CATEGORIA_LEITO e T_CAPACIDADE_LEITO.

    Sem --cnes, os CSVs ainda são criados com cabeçalhos corretos, porém vazios.
    """
    colunas_categoria = [
        "ID_CATEGORIA_LEITO",
        "TP_LEITO",
        "DS_ESPECIALIDADE",
    ]
    colunas_capacidade = [
        "ID_CAPACIDADE_LEITO",
        "CD_CNES",
        "ID_CATEGORIA_LEITO",
        "DT_COMPETENCIA",
        "QT_LEITOS_EXISTENTES",
        "QT_LEITOS_SUS",
    ]

    if pasta_cnes is None:
        return (
            pd.DataFrame(columns=colunas_categoria),
            pd.DataFrame(columns=colunas_capacidade),
        )

    arquivos = localizar_arquivos_cnes(pasta_cnes)
    partes = []

    for ano, mes in competencias:
        chave = ("LT", ano, mes)
        if chave not in arquivos:
            LOG_ETL.warning(
                "CNES LT ausente para %s-%02d.", ano, mes
            )
            continue

        LOG_ETL.info("CNES: %s", arquivos[chave].name)
        dados = carregar_cnes_generico(arquivos[chave])
        partes.append(tratar_leitos_cnes(dados, ano, mes))

    if not partes:
        return (
            pd.DataFrame(columns=colunas_categoria),
            pd.DataFrame(columns=colunas_capacidade),
        )

    leitos = pd.concat(partes, ignore_index=True)

    categoria = (
        leitos[["TP_LEITO", "DS_ESPECIALIDADE"]]
        .drop_duplicates()
        .sort_values(["TP_LEITO", "DS_ESPECIALIDADE"])
        .reset_index(drop=True)
    )
    categoria.insert(
        0,
        "ID_CATEGORIA_LEITO",
        np.arange(1, len(categoria) + 1),
    )

    capacidade = leitos.merge(
        categoria,
        on=["TP_LEITO", "DS_ESPECIALIDADE"],
        how="left",
        validate="many_to_one",
    )
    capacidade = capacidade[[
        "CD_CNES",
        "ID_CATEGORIA_LEITO",
        "DT_COMPETENCIA",
        "QT_LEITOS_EXISTENTES",
        "QT_LEITOS_SUS",
    ]].copy()
    capacidade.insert(
        0,
        "ID_CAPACIDADE_LEITO",
        np.arange(1, len(capacidade) + 1),
    )

    return (
        categoria[colunas_categoria],
        capacidade[colunas_capacidade],
    )


# ============================================================
# ETAPA 15 — ARQUIVO UNICO PARA A AED
# ============================================================

def criar_dataset_aed(
    perfil: pd.DataFrame,
    resumo_aih: pd.DataFrame,
    resumo_assistencial: pd.DataFrame,
    t_municipio: pd.DataFrame,
    t_estabelecimento: pd.DataFrame,
    t_diagnostico: pd.DataFrame,
    t_grupo_icsap: pd.DataFrame,
    t_procedimento: pd.DataFrame,
    t_faixa_etaria: pd.DataFrame,
    t_sexo: pd.DataFrame,
    t_raca_cor: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cria um único dataset pronto para AED.

    Aqui as descrições são repetidas de propósito, pois a disciplina de AED
    precisa de um arquivo simples, sem combinar varios CSVs durante a analise.
    """
    dataset = (
        perfil
        .merge(
            resumo_aih,
            on="ID_PERFIL_INTERNACAO",
            how="left",
            validate="one_to_one",
        )
        .merge(
            resumo_assistencial,
            on="ID_PERFIL_INTERNACAO",
            how="left",
            validate="one_to_one",
        )
        .merge(
            t_municipio,
            left_on="CD_MUNICIPIO_RESIDENCIA",
            right_on="CD_MUNICIPIO",
            how="left",
            validate="many_to_one",
        )
        .merge(
            t_estabelecimento,
            on="CD_CNES",
            how="left",
            suffixes=("", "_ESTAB"),
            validate="many_to_one",
        )
        .merge(
            t_diagnostico,
            on="CD_CID10",
            how="left",
            validate="many_to_one",
        )
        .merge(
            t_grupo_icsap[[
                "ID_GRUPO_ICSAP",
                "NM_GRUPO_ICSAP",
            ]],
            on="ID_GRUPO_ICSAP",
            how="left",
            validate="many_to_one",
        )
        .merge(
            t_procedimento,
            on="CD_PROCEDIMENTO",
            how="left",
            validate="many_to_one",
        )
        .merge(
            t_faixa_etaria,
            on="ID_FAIXA_ETARIA",
            how="left",
            validate="many_to_one",
        )
        .merge(
            t_sexo,
            on="CD_SEXO",
            how="left",
            validate="many_to_one",
        )
        .merge(
            t_raca_cor,
            on="CD_RACA_COR",
            how="left",
            validate="many_to_one",
        )
    )

    # Indicadores derivados úteis para estatística descritiva.
    denominador = dataset["QT_INTERNACOES"].replace(0, np.nan)

    dataset["PC_INTERNACOES_ICSAP"] = (
        dataset["QT_INTERNACOES_ICSAP"] / denominador * 100
    ).round(2)
    dataset["PERMANENCIA_MEDIA"] = (
        dataset["QT_DIAS_PERMANENCIA"] / denominador
    ).round(2)
    dataset["PC_OBITOS"] = (
        dataset["QT_OBITOS"] / denominador * 100
    ).round(2)
    dataset["FL_ICSAP"] = (
        dataset["ID_GRUPO_ICSAP"].notna().astype(int)
    )

    colunas = [
        "ID_PERFIL_INTERNACAO",
        "DT_COMPETENCIA",
        "CD_MUNICIPIO_RESIDENCIA",
        "NM_MUNICIPIO",
        "SG_UF",
        "CD_CNES",
        "NM_ESTABELECIMENTO",
        "TP_ESTABELECIMENTO",
        "CD_CID10",
        "CD_CATEGORIA_CID10",
        "DS_DIAGNOSTICO",
        "ID_GRUPO_ICSAP",
        "NM_GRUPO_ICSAP",
        "FL_ICSAP",
        "CD_PROCEDIMENTO",
        "DS_PROCEDIMENTO",
        "TP_PROCEDIMENTO",
        "TP_COMPLEXIDADE",
        "ID_FAIXA_ETARIA",
        "DS_FAIXA_ETARIA",
        "CD_SEXO",
        "DS_SEXO",
        "CD_RACA_COR",
        "DS_RACA_COR",
        "QT_REGISTROS_AIH",
        "QT_CONTINUIDADES_AIH",
        "VL_TOTAL_AIH",
        "QT_INTERNACOES",
        "QT_INTERNACOES_ICSAP",
        "VL_TOTAL_ICSAP",
        "QT_DIAS_PERMANENCIA",
        "QT_OBITOS",
        "PC_INTERNACOES_ICSAP",
        "PERMANENCIA_MEDIA",
        "PC_OBITOS",
    ]

    return (
        dataset[colunas]
        .sort_values([
            "DT_COMPETENCIA",
            "CD_MUNICIPIO_RESIDENCIA",
            "CD_CNES",
        ])
        .reset_index(drop=True)
    )


# ============================================================
# ETAPA 16 — VERIFICACOES DAS TABELAS FINAIS
# ============================================================

def validar_modelo_final(
    perfil: pd.DataFrame,
    resumo_aih: pd.DataFrame,
    resumo_assistencial: pd.DataFrame,
    agregados: pd.DataFrame,
    t_diagnostico: pd.DataFrame,
    quantidade_cids_sem_descricao: int,
) -> pd.DataFrame:
    """Confere codigos duplicados e compara os totais antes e depois."""
    linhas = []

    linhas.append((
        "ID_PERFIL_INTERNACAO duplicado",
        int(perfil["ID_PERFIL_INTERNACAO"].duplicated().sum()),
    ))
    linhas.append((
        "Combinação de perfil duplicada",
        int(perfil.duplicated(subset=CHAVES_PERFIL).sum()),
    ))
    linhas.append((
        "ID_RESUMO_AIH duplicado",
        int(resumo_aih["ID_RESUMO_AIH"].duplicated().sum()),
    ))
    linhas.append((
        "ID_RESUMO_ASSISTENCIAL duplicado",
        int(
            resumo_assistencial[
                "ID_RESUMO_ASSISTENCIAL"
            ].duplicated().sum()
        ),
    ))
    linhas.append((
        "CD_CID10 duplicado em T_DIAGNOSTICO",
        int(t_diagnostico["CD_CID10"].duplicated().sum()),
    ))
    linhas.append((
        "CID-10 sem descricao oficial",
        quantidade_cids_sem_descricao,
    ))

    comparacoes = {
        "QT_REGISTROS_AIH": resumo_aih["QT_REGISTROS_AIH"].sum(),
        "QT_CONTINUIDADES_AIH": resumo_aih[
            "QT_CONTINUIDADES_AIH"
        ].sum(),
        "VL_TOTAL_AIH": resumo_aih["VL_TOTAL_AIH"].sum(),
        "QT_INTERNACOES": resumo_assistencial["QT_INTERNACOES"].sum(),
        "QT_INTERNACOES_ICSAP": resumo_assistencial[
            "QT_INTERNACOES_ICSAP"
        ].sum(),
        "VL_TOTAL_ICSAP": resumo_assistencial["VL_TOTAL_ICSAP"].sum(),
        "QT_DIAS_PERMANENCIA": resumo_assistencial[
            "QT_DIAS_PERMANENCIA"
        ].sum(),
        "QT_OBITOS": resumo_assistencial["QT_OBITOS"].sum(),
    }

    for medida, valor_final in comparacoes.items():
        valor_origem = agregados[medida].sum()
        diferenca = abs(float(valor_origem) - float(valor_final))
        tolerancia = 0.01 if medida.startswith("VL_") else 0.0

        linhas.append((
            f"Reconciliação {medida}",
            0 if diferenca <= tolerancia else 1,
        ))

    validacao = pd.DataFrame(
        linhas,
        columns=["VERIFICACAO", "QUANTIDADE_PROBLEMAS"],
    )
    validacao["RESULTADO"] = np.where(
        validacao["QUANTIDADE_PROBLEMAS"].eq(0),
        "APROVADO",
        "REVISAR",
    )
    # A ausencia da descricao nao elimina a internacao nem altera os totais.
    # Por isso ela e registrada como aviso e nao interrompe a exportacao.
    aviso_cid = validacao["VERIFICACAO"].eq(
        "CID-10 sem descricao oficial"
    ) & validacao["QUANTIDADE_PROBLEMAS"].gt(0)
    validacao.loc[aviso_cid, "RESULTADO"] = "AVISO"
    return validacao


# ============================================================
# ETAPA 17 — EXPORTACAO DOS ARQUIVOS CSV
# ============================================================

def exportar_csv(tabela: pd.DataFrame, caminho: Path) -> None:
    """Grava CSV em UTF-8 com BOM."""
    caminho.parent.mkdir(parents=True, exist_ok=True)
    tabela.to_csv(
        caminho,
        index=False,
        encoding="utf-8-sig",
        date_format="%Y-%m-%d",
    )


def exportar_modelo_final(
    pasta_saida: Path,
    tabelas: dict[str, pd.DataFrame],
    dataset_aed: pd.DataFrame,
    validacao: pd.DataFrame,
    cids_sem_descricao: pd.DataFrame,
) -> None:
    """Exporta os CSVs do Oracle, da AED e das verificacoes."""
    pasta_modelo = pasta_saida / "tabelas_oracle"
    pasta_aed = pasta_saida / "aed"
    pasta_validacoes = pasta_saida / "validacoes"

    for nome, tabela in tabelas.items():
        exportar_csv(
            tabela,
            pasta_modelo / f"{nome}.csv",
        )

    exportar_csv(
        dataset_aed,
        pasta_aed / "DATASET_AED_HUBSUS360.csv",
    )

    exportar_csv(
        validacao,
        pasta_validacoes / "VALIDACAO_MODELO_FINAL.csv",
    )

    exportar_csv(
        cids_sem_descricao,
        pasta_validacoes / "CIDS_SEM_DESCRICAO.csv",
    )

    manifesto = []
    for caminho in sorted(pasta_saida.rglob("*.csv")):
        with caminho.open(
            "r",
            encoding="utf-8-sig",
            errors="replace",
        ) as arquivo:
            quantidade = max(sum(1 for _ in arquivo) - 1, 0)

        manifesto.append({
            "ARQUIVO": str(caminho.relative_to(pasta_saida)),
            "LINHAS": quantidade,
            "TAMANHO_BYTES": caminho.stat().st_size,
        })

    exportar_csv(
        pd.DataFrame(manifesto),
        pasta_validacoes / "MANIFESTO_ARQUIVOS.csv",
    )


# ============================================================
# ETAPA 18 — EXECUCAO COMPLETA DO ETL
# ============================================================

def executar_etl_hubsus360(argumentos: argparse.Namespace) -> None:
    """
    Executa todo o processo do HUBSUS360.

    O processamento continua competência por competência para evitar
    carregar vários meses completos do SIH simultaneamente na memória.
    """
    pasta_saida = argumentos.saida.expanduser().resolve()
    pasta_saida.mkdir(parents=True, exist_ok=True)
    pasta_programa = Path(__file__).resolve().parent
    pasta_cache_sih = pasta_programa / "cache_hubsus360"

    # A referencia da CID-10 e carregada uma unica vez. Ela nao muda a regra
    # ICSAP: serve para preencher a descricao oficial dos codigos encontrados.
    pasta_cache_cid10 = Path.cwd() / "referencias_hubsus360"
    caminho_cid10 = localizar_referencia_cid10(
        argumentos.cid10,
        pasta_cache_cid10,
    )
    referencia_cid10 = carregar_referencia_cid10(caminho_cid10)

    # --------------------------------------------------------
    # PASSO 1/9 — Localizacao dos arquivos do SIH
    # --------------------------------------------------------
    LOG_ETL.info("1/9 - Localizando arquivos SIH/SUS...")

    # Se nenhum caminho foi informado, tenta reutilizar automaticamente os
    # arquivos escolhidos na primeira execucao. Assim, a janela nao precisa ser
    # aberta toda vez que o programa for executado.
    entradas_informadas = argumentos.sih
    if entradas_informadas is None and not argumentos.refazer_cache:
        entradas_informadas = carregar_lista_arquivos_sih(
            pasta_cache_sih
        )
        if entradas_informadas:
            LOG_ETL.info(
                "Reutilizando a lista de arquivos SIH da execucao anterior."
            )

    entradas = localizar_arquivos_locais(
        entradas_informadas,
        argumentos.anos,
        argumentos.meses,
    )
    salvar_lista_arquivos_sih(
        pasta_cache_sih,
        entradas,
    )
    competencias = sorted(entradas)
    referencia = criar_referencia_icsap()

    agregados_mensais = []
    bases_contexto = []

    # --------------------------------------------------------
    # PASSO 2/9 — Tratamento mensal
    # --------------------------------------------------------
    LOG_ETL.info("2/9 - Tratando SIH e classificando ICSAP...")
    for indice, (ano, mes) in enumerate(competencias, start=1):
        caminho = entradas[(ano, mes)]

        LOG_ETL.info(
            "Competência %s/%s: %s",
            indice,
            len(competencias),
            caminho.name,
        )

        # Quando o mes ja foi tratado e o arquivo original continua igual,
        # carregamos apenas os dois resultados pequenos salvos no cache.
        cache_mes = None
        if not argumentos.refazer_cache:
            cache_mes = carregar_cache_mes(
                pasta_cache_sih,
                caminho,
                ano,
                mes,
            )

        if cache_mes is not None:
            agregado_mes, contexto_mes = cache_mes
            agregados_mensais.append(agregado_mes)
            bases_contexto.append(contexto_mes)
            LOG_ETL.info(
                "Cache utilizado para %s-%02d; leitura do DBC ignorada.",
                ano,
                mes,
            )
            continue

        dados_brutos = carregar_mes_local(caminho)
        validar_competencia(
            dados_brutos,
            ano,
            mes,
        )

        base = padronizar_base(dados_brutos)
        base = classificar_aihs(base)
        base = classificar_icsap(base, referencia)

        mestra = construir_base_mestra(
            base,
            caminho.name,
        )

        validacao_base = validar_base_mestra(
            mestra,
            base,
            referencia,
        )
        exigir_aprovacao(
            "Base SIH mensal",
            validacao_base,
        )

        preparada = preparar_modelo_final(mestra)

        # Totais que serao separados entre perfil, AIH e assistencial.
        agregado_mes = agregar_perfis_mes(preparada)

        # Guardamos somente codigos e descricoes necessarios nas outras tabelas.
        # Assim, a base detalhada do mes pode ser liberada da memoria.
        contexto_mes = preparada[[
                "CD_MUNICIPIO_INTERNACAO",
                "CD_CNES",
                "CD_CID10",
                "CD_CATEGORIA_CID10",
                "grupo_icsap",
                "CD_PROCEDIMENTO",
                "complexidade_descricao",
                "CD_SEXO",
                "sexo_descricao",
                "CD_RACA_COR",
                "raca_cor_descricao",
            ]].drop_duplicates()

        agregados_mensais.append(agregado_mes)
        bases_contexto.append(contexto_mes)

        # O cache e salvo logo depois de cada mes. Se uma execucao futura parar
        # no meio, os meses concluidos ate aquele ponto continuam aproveitaveis.
        salvar_cache_mes(
            pasta_cache_sih,
            caminho,
            ano,
            mes,
            agregado_mes,
            contexto_mes,
        )

        del dados_brutos, base, mestra, preparada

    agregados = pd.concat(
        agregados_mensais,
        ignore_index=True,
    )
    contexto = (
        pd.concat(
            bases_contexto,
            ignore_index=True,
        )
        .drop_duplicates()
        .reset_index(drop=True)
    )

    # --------------------------------------------------------
    # PASSO 3/9 — Tabelas de identificacao e descricao
    # --------------------------------------------------------
    LOG_ETL.info("3/9 - Criando tabelas de identificacao e descricao...")

    municipios_ibge = carregar_referencia_municipios(
        argumentos.municipios_ibge
    )

    t_grupo_icsap = criar_t_grupo_icsap(
        referencia
    )
    t_diagnostico = criar_t_diagnostico(
        contexto,
        referencia_cid10,
    )

    # Se algum codigo do SIH nao existir na referencia CID-10 utilizada, ele
    # continua na tabela para nao perder internacoes. A lista original e
    # separada para conferencia e a descricao recebe um texto identificavel.
    cids_sem_descricao = (
        t_diagnostico.loc[
            t_diagnostico["DS_DIAGNOSTICO"].isna(),
            [
                "CD_CID10",
                "CD_CATEGORIA_CID10",
                "ID_GRUPO_ICSAP",
            ],
        ]
        .drop_duplicates("CD_CID10")
        .sort_values("CD_CID10", na_position="last")
        .reset_index(drop=True)
    )
    t_diagnostico["DS_DIAGNOSTICO"] = (
        t_diagnostico["DS_DIAGNOSTICO"].fillna(
            "DESCRICAO NAO ENCONTRADA NA REFERENCIA CID-10"
        )
    )
    t_municipio = criar_t_municipio(
        municipios_ibge
    )
    t_procedimento = criar_t_procedimento(
        contexto
    )
    t_faixa_etaria = criar_t_faixa_etaria()
    t_sexo = criar_t_sexo(
        contexto
    )
    t_raca_cor = criar_t_raca_cor(
        contexto
    )

    snapshot = None
    if argumentos.cnes_estabelecimentos:
        snapshot = carregar_snapshot_estabelecimentos(
            argumentos.cnes_estabelecimentos
        )

    t_estabelecimento = criar_t_estabelecimento(
        contexto,
        snapshot,
    )

    # --------------------------------------------------------
    # PASSO 4/9 — Perfil da internacao, AIH e assistencial
    # --------------------------------------------------------
    LOG_ETL.info(
        "4/9 - Separando perfil, AIH e informações assistenciais..."
    )

    (
        t_perfil_internacao,
        t_resumo_aih,
        t_resumo_assistencial,
    ) = criar_tabelas_perfil_e_resumos(
        agregados
    )

    # --------------------------------------------------------
    # PASSO 5/9 — Capacidade de leitos do CNES
    # --------------------------------------------------------
    LOG_ETL.info("5/9 - Criando tabelas de capacidade...")

    (
        t_categoria_leito,
        t_capacidade_leito,
    ) = criar_tabelas_capacidade(
        argumentos.cnes,
        competencias,
    )

    # --------------------------------------------------------
    # PASSO 6/9 — Arquivo unico para AED
    # --------------------------------------------------------
    LOG_ETL.info("6/9 - Criando dataset único para AED...")

    dataset_aed = criar_dataset_aed(
        t_perfil_internacao,
        t_resumo_aih,
        t_resumo_assistencial,
        t_municipio,
        t_estabelecimento,
        t_diagnostico,
        t_grupo_icsap,
        t_procedimento,
        t_faixa_etaria,
        t_sexo,
        t_raca_cor,
    )

    # --------------------------------------------------------
    # PASSO 7/9 — Verificacoes finais
    # --------------------------------------------------------
    LOG_ETL.info("7/9 - Verificando as tabelas finais...")

    validacao = validar_modelo_final(
        t_perfil_internacao,
        t_resumo_aih,
        t_resumo_assistencial,
        agregados,
        t_diagnostico,
        len(cids_sem_descricao),
    )

    pendencias = validacao[
        validacao["RESULTADO"].eq("REVISAR")
    ]
    if not pendencias.empty:
        raise ValueError(
            "As tabelas finais possuem verificacoes pendentes:\n"
            + pendencias.to_string(index=False)
        )

    avisos = validacao[validacao["RESULTADO"].eq("AVISO")]
    if not avisos.empty:
        LOG_ETL.warning(
            "%s CIDs ficaram sem descricao. Consulte "
            "validacoes/CIDS_SEM_DESCRICAO.csv.",
            len(cids_sem_descricao),
        )

    # --------------------------------------------------------
    # PASSO 8/9 — Exportacao dos CSVs
    # --------------------------------------------------------
    LOG_ETL.info("8/9 - Exportando CSVs...")

    tabelas = {
        "T_GRUPO_ICSAP": t_grupo_icsap,
        "T_DIAGNOSTICO": t_diagnostico,
        "T_MUNICIPIO": t_municipio,
        "T_ESTABELECIMENTO": t_estabelecimento,
        "T_PROCEDIMENTO": t_procedimento,
        "T_FAIXA_ETARIA": t_faixa_etaria,
        "T_SEXO": t_sexo,
        "T_RACA_COR": t_raca_cor,
        "T_PERFIL_INTERNACAO": t_perfil_internacao,
        "T_RESUMO_AIH": t_resumo_aih,
        "T_RESUMO_ASSISTENCIAL": t_resumo_assistencial,
        "T_CATEGORIA_LEITO": t_categoria_leito,
        "T_CAPACIDADE_LEITO": t_capacidade_leito,
    }

    exportar_modelo_final(
        pasta_saida,
        tabelas,
        dataset_aed,
        validacao,
        cids_sem_descricao,
    )

    # --------------------------------------------------------
    # PASSO 9/9 — Resumo da execucao
    # --------------------------------------------------------
    LOG_ETL.info("9/9 - ETL concluído.")
    LOG_ETL.info(
        "Competências processadas: %s",
        len(competencias),
    )
    LOG_ETL.info(
        "T_PERFIL_INTERNACAO: %s linhas",
        len(t_perfil_internacao),
    )
    LOG_ETL.info(
        "T_RESUMO_AIH: %s linhas",
        len(t_resumo_aih),
    )
    LOG_ETL.info(
        "T_RESUMO_ASSISTENCIAL: %s linhas",
        len(t_resumo_assistencial),
    )
    LOG_ETL.info(
        "DATASET_AED_HUBSUS360: %s linhas e %s colunas",
        *dataset_aed.shape,
    )
    LOG_ETL.info(
        "Resultados salvos em: %s",
        pasta_saida,
    )

    if argumentos.gerar_zip:
        caminho_zip = Path(
            shutil.make_archive(
                str(pasta_saida),
                "zip",
                root_dir=pasta_saida,
            )
        )
        LOG_ETL.info(
            "ZIP gerado: %s",
            caminho_zip,
        )


# ============================================================
# ETAPAS 9 A 18 — VERSAO AUDITADA COM TODAS AS REFERENCIAS
# ============================================================

# Esta parte substitui as funcoes anteriores que aceitavam referencias
# opcionais. A modelagem continua com as mesmas 13 tabelas; a diferenca e que
# agora o ETL exige as fontes necessarias para realmente preenche-las.

VERSAO_CACHE_SIH = 3
VERSAO_CACHE_REFERENCIAS = 2
PASTAS_REFERENCIAS = {
    "sih": "dados_sih",
    "st": "ST",
    "lt": "LT",
    "sigtap": "sigtap",
    "cid10": "cid10",
    "dominios": "dominios_cnes",
    "regioes": "regioes_saude",
    "estabelecimentos": "cnes_estabelecimentos",
}


def normalizar_nome(texto: object) -> str:
    """Padroniza nomes de colunas sem depender de acentos ou espacos."""
    valor = unicodedata.normalize("NFKD", str(texto))
    valor = "".join(letra for letra in valor if not unicodedata.combining(letra))
    return re.sub(r"[^A-Z0-9]+", "_", valor.upper()).strip("_")


def normalizar_codigo(serie: pd.Series, tamanho: int | None = None) -> pd.Series:
    """Transforma codigos lidos como numero ou texto em uma chave uniforme."""
    resultado = (
        serie.astype("string")
        .str.replace(r"\.0$", "", regex=True)
        .str.replace(r"[^A-Za-z0-9]", "", regex=True)
        .str.upper()
        .str.strip()
        .replace("", pd.NA)
    )
    if tamanho:
        resultado = resultado.str.zfill(tamanho)
    return resultado


def primeiro_arquivo(
    pasta: Path,
    padroes: list[str],
    descricao: str,
) -> Path:
    """Localiza uma referencia por nome, inclusive dentro de subpastas."""
    candidatos: list[Path] = []
    for padrao in padroes:
        candidatos.extend(pasta.rglob(padrao))
    candidatos = sorted({item.resolve() for item in candidatos if item.is_file()})
    if not candidatos:
        raise FileNotFoundError(
            f"Referencia obrigatoria nao encontrada: {descricao}. "
            f"Pasta verificada: {pasta}."
        )
    return max(candidatos, key=lambda item: item.stat().st_size)


def localizar_raiz_referencias(caminho: Path | None) -> Path:
    """Encontra a pasta referencias_hubsus360 criada para o projeto."""
    candidatos = []
    if caminho is not None:
        candidatos.append(caminho)
    candidatos.extend([
        Path.cwd() / "referencias_hubsus360",
        Path(__file__).resolve().parent / "referencias_hubsus360",
        Path(__file__).resolve().parent.parent / "referencias_hubsus360",
    ])
    for candidato in candidatos:
        resolvido = candidato.expanduser().resolve()
        if resolvido.is_dir():
            return resolvido
    raise FileNotFoundError(
        "A pasta referencias_hubsus360 nao foi encontrada. Informe-a com "
        "--referencias."
    )


def criar_argumentos_hubsus360() -> argparse.Namespace:
    """Recebe a pasta principal e filtros de periodo do HUBSUS360."""
    parser = argparse.ArgumentParser(
        description="ETL auditado do HUBSUS360 para Oracle e AED."
    )
    parser.add_argument(
        "--referencias",
        type=Path,
        help="Pasta referencias_hubsus360 com SIH, CNES, SIGTAP e CID-10.",
    )
    parser.add_argument("--sih", type=Path, nargs="+")
    parser.add_argument(
        "--saida", type=Path, default=Path("resultados_hubsus360_modelo_final")
    )
    parser.add_argument("--anos", type=int, nargs="+")
    parser.add_argument("--meses", type=int, nargs="+")
    parser.add_argument("--refazer-cache", action="store_true")
    parser.add_argument("--gerar-zip", action="store_true")
    # Opcoes antigas continuam aceitas para nao quebrar configuracoes salvas
    # no PyCharm. Quando omitidas, as pastas padrao acima sao utilizadas.
    parser.add_argument("--cid10", type=Path)
    parser.add_argument("--cnes", type=Path)
    parser.add_argument("--cnes-estabelecimentos", type=Path)
    parser.add_argument("--municipios-ibge", type=Path)
    argumentos = parser.parse_args()
    if argumentos.meses:
        argumentos.meses = sorted(set(argumentos.meses))
        invalidos = [mes for mes in argumentos.meses if mes not in range(1, 13)]
        if invalidos:
            parser.error(f"Meses invalidos: {invalidos}")
    if argumentos.anos:
        argumentos.anos = sorted(set(argumentos.anos))
    return argumentos


def ler_csv_flexivel(caminho: Path, membro: str | None = None) -> pd.DataFrame:
    """Le CSV comum ou um ZIP que recebeu extensao CSV no download."""
    if zipfile.is_zipfile(caminho):
        with zipfile.ZipFile(caminho) as pacote:
            nomes = [nome for nome in pacote.namelist() if nome.lower().endswith(".csv")]
            if membro:
                nomes = [nome for nome in nomes if membro.lower() in nome.lower()]
            if not nomes:
                raise ValueError(f"Nenhum CSV foi encontrado dentro de {caminho.name}.")
            conteudo = pacote.read(max(nomes, key=lambda nome: pacote.getinfo(nome).file_size))
    else:
        conteudo = caminho.read_bytes()

    ultimo_erro: Exception | None = None
    for codificacao in ("utf-8-sig", "cp1252", "latin1"):
        for separador in (";", ","):
            try:
                dados = pd.read_csv(
                    BytesIO(conteudo), sep=separador, encoding=codificacao,
                    dtype="string", keep_default_na=False,
                    quoting=csv.QUOTE_NONE, engine="python",
                )
                if len(dados.columns) > 1:
                    dados.columns = [normalizar_nome(coluna) for coluna in dados.columns]
                    return dados
            except (UnicodeDecodeError, pd.errors.ParserError) as erro:
                ultimo_erro = erro
    raise ValueError(f"Nao foi possivel ler {caminho.name} como CSV.") from ultimo_erro


def carregar_cid10_completo(pasta: Path) -> pd.DataFrame:
    """Une categorias e subcategorias CID-10 sem inventar descricoes."""
    arquivos_csv = [item for item in pasta.rglob("*") if item.is_file() and item.suffix.lower() == ".csv"]
    categorias = [
        item for item in arquivos_csv
        if "CATEGORIAS" in normalizar_nome(item.name)
        and "SUBCATEGORIAS" not in normalizar_nome(item.name)
    ]
    subcategorias = [
        item for item in arquivos_csv if "SUBCATEGORIAS" in normalizar_nome(item.name)
    ]
    if not categorias or not subcategorias:
        raise FileNotFoundError(
            "A pasta cid10 precisa conter CID-10-CATEGORIAS.CSV e "
            "CID-10-SUBCATEGORIAS.CSV."
        )
    arquivo_categoria = max(categorias, key=lambda item: item.stat().st_size)
    arquivo_subcategoria = max(subcategorias, key=lambda item: item.stat().st_size)
    partes = []
    for caminho, coluna_codigo in [
        (arquivo_categoria, "CAT"), (arquivo_subcategoria, "SUBCAT")
    ]:
        dados = ler_csv_flexivel(caminho)
        if coluna_codigo not in dados or "DESCRICAO" not in dados:
            raise ValueError(
                f"{caminho.name} nao possui {coluna_codigo} e DESCRICAO."
            )
        parte = pd.DataFrame({
            "CD_CID10": normalizar_codigo(dados[coluna_codigo]),
            "DS_DIAGNOSTICO": dados["DESCRICAO"].astype("string").str.strip(),
        })
        partes.append(parte)
    referencia = pd.concat(partes, ignore_index=True)
    referencia = referencia[
        referencia["CD_CID10"].str.match(r"^[A-Z][0-9]{2}[A-Z0-9]?$", na=False)
    ]
    referencia["DS_DIAGNOSTICO"] = referencia["DS_DIAGNOSTICO"].replace("", pd.NA)
    return (
        referencia.dropna(subset=["DS_DIAGNOSTICO"])
        .drop_duplicates("CD_CID10", keep="last")
        .sort_values("CD_CID10").reset_index(drop=True)
    )


def competencia_sigtap(caminho: Path) -> tuple[int, int] | None:
    correspondencia = re.search(r"TabelaUnificada_(20\d{2})(0[1-9]|1[0-2])", caminho.name, re.I)
    return (
        (int(correspondencia.group(1)), int(correspondencia.group(2)))
        if correspondencia else None
    )


def localizar_sigtap(pasta: Path) -> dict[tuple[int, int], Path]:
    encontrados: dict[tuple[int, int], Path] = {}
    for caminho in pasta.rglob("*.zip"):
        competencia = competencia_sigtap(caminho)
        if competencia:
            existentes = encontrados.get(competencia)
            if existentes is None or caminho.stat().st_size > existentes.stat().st_size:
                encontrados[competencia] = caminho.resolve()
    return encontrados


def ler_linhas_zip(pacote: zipfile.ZipFile, final_nome: str) -> list[str]:
    candidatos = [nome for nome in pacote.namelist() if nome.lower().endswith(final_nome)]
    if not candidatos:
        raise ValueError(f"{final_nome} nao foi encontrado no pacote SIGTAP.")
    return pacote.read(candidatos[0]).decode("latin1", errors="replace").splitlines()


def carregar_sigtap_mes(caminho: Path, ano: int, mes: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Le procedimento, grupo e CID da competencia SIGTAP."""
    with zipfile.ZipFile(caminho) as pacote:
        linhas_grupo = ler_linhas_zip(pacote, "tb_grupo.txt")
        grupos = {
            linha[:2].strip(): linha[2:102].strip()
            for linha in linhas_grupo if linha[:2].strip()
        }
        procedimentos = []
        for linha in ler_linhas_zip(pacote, "tb_procedimento.txt"):
            codigo = linha[:10].strip()
            if not codigo:
                continue
            cd_complexidade = linha[260:261].strip() if len(linha) > 260 else ""
            procedimentos.append({
                "DT_COMPETENCIA": pd.Timestamp(ano, mes, 1),
                "CD_PROCEDIMENTO": codigo,
                "DS_PROCEDIMENTO": linha[10:260].strip(),
                "CD_GRUPO_PROCEDIMENTO": codigo[:2],
                "NM_GRUPO_PROCEDIMENTO": grupos.get(codigo[:2]),
                "TP_COMPLEXIDADE": {
                    "0": "NAO SE APLICA",
                    "1": "ATENCAO BASICA", "2": "MEDIA COMPLEXIDADE",
                    "3": "ALTA COMPLEXIDADE",
                }.get(cd_complexidade, f"COMPLEXIDADE {cd_complexidade} NAO MAPEADA"),
            })
        cids = []
        for linha in ler_linhas_zip(pacote, "tb_cid.txt"):
            codigo = re.sub(r"[^A-Z0-9]", "", linha[:4].upper())
            descricao = linha[4:104].strip()
            if codigo and descricao:
                cids.append({"CD_CID10": codigo, "DS_DIAGNOSTICO_SIGTAP": descricao})
    return pd.DataFrame(procedimentos), pd.DataFrame(cids).drop_duplicates("CD_CID10")


def ler_dbf_colunas(caminho: Path, colunas: list[str]) -> pd.DataFrame:
    """Le campos selecionados de DBF sem exigir uma biblioteca adicional."""
    with caminho.open("rb") as arquivo:
        cabecalho = arquivo.read(32)
        if len(cabecalho) < 32:
            raise ValueError(f"Cabecalho DBF invalido: {caminho.name}")
        quantidade = struct.unpack("<I", cabecalho[4:8])[0]
        tamanho_cabecalho = struct.unpack("<H", cabecalho[8:10])[0]
        tamanho_registro = struct.unpack("<H", cabecalho[10:12])[0]
        campos = []
        while True:
            descritor = arquivo.read(32)
            if not descritor or descritor[0] == 0x0D:
                break
            campos.append({
                "nome": descritor[:11].split(b"\x00", 1)[0].decode("ascii", "ignore"),
                "tipo": chr(descritor[11]),
                "tamanho": descritor[16],
            })
    nomes = [campo["nome"] for campo in campos]
    faltantes = sorted(set(colunas).difference(nomes))
    if faltantes:
        raise ValueError(f"{caminho.name} nao possui os campos {faltantes}.")
    selecionadas = set(colunas)
    posicao = 1
    intervalos = []
    for campo in campos:
        inicio = posicao
        fim = inicio + campo["tamanho"]
        if campo["nome"] in selecionadas:
            intervalos.append((campo, inicio, fim))
        posicao = fim
    registros = []
    with caminho.open("rb") as arquivo:
        arquivo.seek(tamanho_cabecalho)
        for _ in range(quantidade):
            registro = arquivo.read(tamanho_registro)
            if len(registro) < tamanho_registro:
                break
            if registro[:1] == b"*":
                continue
            item = {}
            for campo, inicio, fim in intervalos:
                valor = registro[inicio:fim].decode("latin-1", "ignore").strip()
                if not valor:
                    item[campo["nome"]] = pd.NA
                elif campo["tipo"] in {"N", "F"}:
                    item[campo["nome"]] = pd.to_numeric(valor, errors="coerce")
                else:
                    item[campo["nome"]] = valor
            registros.append(item)
    dados = pd.DataFrame(registros, columns=colunas)
    if dados.empty:
        raise ValueError(f"O DBF esta vazio: {caminho.name}")
    return dados


def ler_dados_sih(arquivo_dbf: Path) -> pd.DataFrame:
    """Le somente os campos do SIH usados no projeto."""
    dados = ler_dbf_colunas(arquivo_dbf, COLUNAS_ORIGINAIS)
    LOG.info("Arquivo lido: %s registros e %s colunas.", len(dados), len(dados.columns))
    return dados


def carregar_cnes_colunas(caminho: Path, colunas: list[str]) -> pd.DataFrame:
    with tempfile.TemporaryDirectory(prefix="hubsus360_cnes_") as temporario:
        dbf = preparar_dbf(caminho, Path(temporario))
        return ler_dbf_colunas(dbf, colunas)


def carregar_dominios_cnes(caminho: Path) -> dict[str, dict[str, str]]:
    """Le os nomes oficiais de tipos de estabelecimento e de leito."""
    if zipfile.is_zipfile(caminho):
        with zipfile.ZipFile(caminho) as pacote:
            nomes = [nome for nome in pacote.namelist() if nome.lower().endswith((".xls", ".xlsx"))]
            if not nomes:
                raise ValueError("A planilha SCNES_DOMINIOS nao foi encontrada no ZIP.")
            conteudo = BytesIO(pacote.read(nomes[0]))
    else:
        conteudo = caminho
    planilha = pd.ExcelFile(conteudo, engine="openpyxl")
    retorno: dict[str, dict[str, str]] = {}
    selecao = {
        "TIPOS_ESTABELECIMENTO": "TIPOS DE ESTABELECIMENTO",
        "TIPOS_LEITO": "TIPOS DE LEITOS",
        "LEITOS": "LEITOS",
    }
    for chave, trecho in selecao.items():
        aba = next(
            (nome for nome in planilha.sheet_names if trecho in normalizar_nome(nome).replace("_", " ")),
            None,
        )
        if aba is None:
            # Comparacao alternativa sem espacos para nomes com pequenas variacoes.
            aba = next(
                (nome for nome in planilha.sheet_names
                 if normalizar_nome(nome).replace("_", "") == normalizar_nome(trecho).replace("_", "")),
                None,
            )
        if aba is None:
            raise ValueError(f"Aba {trecho} nao encontrada em SCNES_DOMINIOS.")
        dados = pd.read_excel(planilha, sheet_name=aba, dtype="string")
        dados.columns = [normalizar_nome(coluna) for coluna in dados.columns]
        coluna_descricao = next(coluna for coluna in dados if coluna.startswith("DESCR"))
        coluna_codigo = next(coluna for coluna in dados if coluna != coluna_descricao)
        codigos = normalizar_codigo(dados[coluna_codigo])
        retorno[chave] = {
            str(codigo): str(descricao).strip()
            for codigo, descricao in zip(codigos, dados[coluna_descricao])
            if pd.notna(codigo) and pd.notna(descricao) and str(descricao).strip()
        }
    return retorno


def carregar_regioes_saude(caminho: Path) -> pd.DataFrame:
    dados = ler_csv_flexivel(caminho, membro="macroregiao")
    obrigatorias = {"COD_MUNICIPIO", "NO_MUNICIPIO", "REGIAO_DE_SAUDE"}
    if not obrigatorias.issubset(dados.columns):
        raise ValueError(f"Regioes de saude sem colunas {sorted(obrigatorias - set(dados.columns))}.")
    tabela = pd.DataFrame({
        "CD_MUNICIPIO": normalizar_codigo(dados["COD_MUNICIPIO"]).str[:6],
        "NM_MUNICIPIO": (
            dados["NO_MUNICIPIO"].astype("string").str.strip()
            .str.replace(r"^\s*SP\s*-\s*", "", regex=True, flags=re.IGNORECASE)
        ),
        "SG_UF": dados.get("SG_UF", pd.Series(UF_ANALISE, index=dados.index)),
        "NM_REGIAO_SAUDE": dados["REGIAO_DE_SAUDE"].astype("string").str.strip(),
    })
    tabela = tabela[tabela["SG_UF"].astype("string").str.upper().eq(UF_ANALISE)]
    return tabela.drop_duplicates("CD_MUNICIPIO").sort_values("CD_MUNICIPIO").reset_index(drop=True)


def carregar_snapshot_estabelecimentos(caminho: Path) -> pd.DataFrame:
    """Le o arquivo do CNES mesmo quando o download e um ZIP chamado CSV."""
    dados = ler_csv_flexivel(caminho, membro="estabelecimentos")
    if "CNES" not in dados or "NOME_FANTASIA" not in dados:
        raise ValueError("Snapshot CNES sem as colunas CNES e NOME FANTASIA.")
    tabela = pd.DataFrame({
        "CD_CNES": normalizar_codigo(dados["CNES"], 7),
        "NM_ESTABELECIMENTO": dados["NOME_FANTASIA"].astype("string").str.strip(),
    })
    if "IBGE" in dados:
        tabela["CD_MUNICIPIO_SNAPSHOT"] = normalizar_codigo(dados["IBGE"]).str[:6]
    tabela["NM_ESTABELECIMENTO"] = tabela["NM_ESTABELECIMENTO"].replace("", pd.NA)
    return tabela.drop_duplicates("CD_CNES", keep="last")


# ============================================================
# ETAPA 19 — INICIO DO PROGRAMA
# ============================================================

def main() -> int:
    """Executa o ETL e exibe erros de forma amigável."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s | %(message)s",
    )

    try:
        executar_etl_hubsus360_auditado(
            criar_argumentos_hubsus360()
        )
    except (
        FileNotFoundError,
        ValueError,
        RuntimeError,
    ) as erro:
        LOG_ETL.error("%s", erro)
        return 1
    except KeyboardInterrupt:
        LOG_ETL.error(
            "Execução cancelada pelo usuário."
        )
        return 130

    return 0


if __name__ == "__main__":
    sys.exit(main())

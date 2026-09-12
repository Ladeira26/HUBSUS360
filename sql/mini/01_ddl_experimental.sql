--------------------------------------------------------
--  Arquivo criado - sexta-feira-setembro-11-2026   
--------------------------------------------------------
--------------------------------------------------------
--  DDL for Table T_CAPACIDADE_LEITO
--------------------------------------------------------

  CREATE TABLE "T_CAPACIDADE_LEITO" 
   (	"ID_CAPACIDADE_LEITO" NUMBER(10,0), 
	"CD_CNES" CHAR(7 BYTE) COLLATE "USING_NLS_COMP", 
	"ID_CATEGORIA_LEITO" NUMBER(2,0), 
	"DT_COMPETENCIA" DATE, 
	"QT_LEITOS_EXISTENTES" NUMBER(6,0), 
	"QT_LEITOS_SUS" NUMBER(6,0)
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_CAPACIDADE_LEITO"."ID_CAPACIDADE_LEITO" IS 'Identificador unico do registro mensal de capacidade de leitos.';
   COMMENT ON COLUMN "T_CAPACIDADE_LEITO"."CD_CNES" IS 'Codigo CNES do estabelecimento de saude ao qual a capacidade pertence.';
   COMMENT ON COLUMN "T_CAPACIDADE_LEITO"."ID_CATEGORIA_LEITO" IS 'Categoria de leito associada ao registro de capacidade.';
   COMMENT ON COLUMN "T_CAPACIDADE_LEITO"."DT_COMPETENCIA" IS 'Competencia mensal da informacao de capacidade hospitalar.';
   COMMENT ON COLUMN "T_CAPACIDADE_LEITO"."QT_LEITOS_EXISTENTES" IS 'Quantidade total de leitos existentes no estabelecimento para a categoria e competencia.';
   COMMENT ON COLUMN "T_CAPACIDADE_LEITO"."QT_LEITOS_SUS" IS 'Quantidade de leitos destinados ao SUS para a categoria e competencia.';
   COMMENT ON TABLE "T_CAPACIDADE_LEITO"  IS 'Fato mensal de capacidade de leitos por estabelecimento CNES, categoria e competencia.';
--------------------------------------------------------
--  DDL for Table T_CATEGORIA_LEITO
--------------------------------------------------------

  CREATE TABLE "T_CATEGORIA_LEITO" 
   (	"ID_CATEGORIA_LEITO" NUMBER(2,0), 
	"TP_LEITO" VARCHAR2(40 BYTE) COLLATE "USING_NLS_COMP", 
	"DS_ESPECIALIDADE" VARCHAR2(100 BYTE) COLLATE "USING_NLS_COMP"
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_CATEGORIA_LEITO"."ID_CATEGORIA_LEITO" IS 'Identificador unico da combinacao de tipo de leito e especialidade.';
   COMMENT ON COLUMN "T_CATEGORIA_LEITO"."TP_LEITO" IS 'Tipo geral do leito hospitalar conforme classificacao utilizada pelo CNES.';
   COMMENT ON COLUMN "T_CATEGORIA_LEITO"."DS_ESPECIALIDADE" IS 'Especialidade ou descricao especifica associada ao tipo de leito.';
   COMMENT ON TABLE "T_CATEGORIA_LEITO"  IS 'Dimensao de categorias de leitos hospitalares por tipo e especialidade.';
--------------------------------------------------------
--  DDL for Table T_DIAGNOSTICO
--------------------------------------------------------

  CREATE TABLE "T_DIAGNOSTICO" 
   (	"CD_CID10" VARCHAR2(5 BYTE) COLLATE "USING_NLS_COMP", 
	"CD_CATEGORIA_CID10" VARCHAR2(3 BYTE) COLLATE "USING_NLS_COMP", 
	"DS_DIAGNOSTICO" VARCHAR2(500 BYTE) COLLATE "USING_NLS_COMP", 
	"ID_GRUPO_ICSAP" NUMBER(2,0)
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_DIAGNOSTICO"."CD_CID10" IS 'Codigo do diagnostico conforme a classificacao CID-10.';
   COMMENT ON COLUMN "T_DIAGNOSTICO"."CD_CATEGORIA_CID10" IS 'Categoria principal de tres caracteres da classificacao CID-10.';
   COMMENT ON COLUMN "T_DIAGNOSTICO"."DS_DIAGNOSTICO" IS 'Descricao textual do diagnostico CID-10.';
   COMMENT ON COLUMN "T_DIAGNOSTICO"."ID_GRUPO_ICSAP" IS 'Identificador do grupo ICSAP ao qual o diagnostico pertence, quando aplicavel.';
   COMMENT ON TABLE "T_DIAGNOSTICO"  IS 'Dimensao de diagnosticos CID-10, com associacao opcional aos grupos ICSAP.';
--------------------------------------------------------
--  DDL for Table T_ESTABELECIMENTO
--------------------------------------------------------

  CREATE TABLE "T_ESTABELECIMENTO" 
   (	"CD_CNES" CHAR(7 BYTE) COLLATE "USING_NLS_COMP", 
	"CD_MUNICIPIO" CHAR(6 BYTE) COLLATE "USING_NLS_COMP", 
	"NM_ESTABELECIMENTO" VARCHAR2(100 BYTE) COLLATE "USING_NLS_COMP", 
	"TP_ESTABELECIMENTO" VARCHAR2(100 BYTE) COLLATE "USING_NLS_COMP"
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_ESTABELECIMENTO"."CD_CNES" IS 'Codigo CNES que identifica unicamente o estabelecimento de saude.';
   COMMENT ON COLUMN "T_ESTABELECIMENTO"."CD_MUNICIPIO" IS 'Codigo do municipio onde o estabelecimento de saude esta localizado.';
   COMMENT ON COLUMN "T_ESTABELECIMENTO"."NM_ESTABELECIMENTO" IS 'Nome do estabelecimento de saude.';
   COMMENT ON COLUMN "T_ESTABELECIMENTO"."TP_ESTABELECIMENTO" IS 'Tipo ou classificacao do estabelecimento de saude.';
   COMMENT ON TABLE "T_ESTABELECIMENTO"  IS 'Dimensao de estabelecimentos de saude identificados pelo codigo CNES.';
--------------------------------------------------------
--  DDL for Table T_FAIXA_ETARIA
--------------------------------------------------------

  CREATE TABLE "T_FAIXA_ETARIA" 
   (	"ID_FAIXA_ETARIA" NUMBER(2,0), 
	"DS_FAIXA_ETARIA" VARCHAR2(40 BYTE) COLLATE "USING_NLS_COMP", 
	"NR_IDADE_MINIMA" NUMBER(3,0), 
	"NR_IDADE_MAXIMA" NUMBER(3,0)
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_FAIXA_ETARIA"."ID_FAIXA_ETARIA" IS 'Identificador da faixa etaria. A categoria 13 representa idade nao informada.';
   COMMENT ON COLUMN "T_FAIXA_ETARIA"."DS_FAIXA_ETARIA" IS 'Descricao amigavel da faixa etaria utilizada nas analises do painel.';
   COMMENT ON COLUMN "T_FAIXA_ETARIA"."NR_IDADE_MINIMA" IS 'Idade minima, em anos, associada a faixa etaria quando aplicavel.';
   COMMENT ON COLUMN "T_FAIXA_ETARIA"."NR_IDADE_MAXIMA" IS 'Idade maxima, em anos, associada a faixa etaria quando aplicavel.';
   COMMENT ON TABLE "T_FAIXA_ETARIA"  IS 'Dimensao de faixas etarias utilizadas na segmentacao das internacoes.';
--------------------------------------------------------
--  DDL for Table T_GRUPO_ICSAP
--------------------------------------------------------

  CREATE TABLE "T_GRUPO_ICSAP" 
   (	"ID_GRUPO_ICSAP" NUMBER(2,0), 
	"NM_GRUPO_ICSAP" VARCHAR2(100 BYTE) COLLATE "USING_NLS_COMP", 
	"DS_GRUPO_ICSAP" VARCHAR2(200 BYTE) COLLATE "USING_NLS_COMP"
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_GRUPO_ICSAP"."ID_GRUPO_ICSAP" IS 'Identificador numerico do grupo ICSAP, de 1 a 19.';
   COMMENT ON COLUMN "T_GRUPO_ICSAP"."NM_GRUPO_ICSAP" IS 'Nome do grupo de condicoes sensiveis a atencao primaria.';
   COMMENT ON COLUMN "T_GRUPO_ICSAP"."DS_GRUPO_ICSAP" IS 'Descricao do grupo ICSAP utilizada para interpretacao analitica e apoio ao Select AI.';
   COMMENT ON TABLE "T_GRUPO_ICSAP"  IS 'Dimensao com os grupos de Internacoes por Condicoes Sensiveis a Atencao Primaria utilizados na classificacao ICSAP.';
--------------------------------------------------------
--  DDL for Table T_MUNICIPIO
--------------------------------------------------------

  CREATE TABLE "T_MUNICIPIO" 
   (	"CD_MUNICIPIO" CHAR(6 BYTE) COLLATE "USING_NLS_COMP", 
	"NM_MUNICIPIO" VARCHAR2(60 BYTE) COLLATE "USING_NLS_COMP", 
	"SG_UF" CHAR(2 BYTE) COLLATE "USING_NLS_COMP", 
	"NM_REGIAO_SAUDE" VARCHAR2(80 BYTE) COLLATE "USING_NLS_COMP"
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_MUNICIPIO"."CD_MUNICIPIO" IS 'Codigo de identificacao do municipio.';
   COMMENT ON COLUMN "T_MUNICIPIO"."NM_MUNICIPIO" IS 'Nome do municipio do estado de Sao Paulo.';
   COMMENT ON COLUMN "T_MUNICIPIO"."SG_UF" IS 'Sigla da unidade federativa. Neste projeto corresponde ao estado de Sao Paulo.';
   COMMENT ON COLUMN "T_MUNICIPIO"."NM_REGIAO_SAUDE" IS 'Nome da regiao de saude associada ao municipio.';
   COMMENT ON TABLE "T_MUNICIPIO"  IS 'Dimensao de municipios do estado de Sao Paulo utilizados nas analises do HUBSUS360.';
--------------------------------------------------------
--  DDL for Table T_PERFIL_INTERNACAO
--------------------------------------------------------

  CREATE TABLE "T_PERFIL_INTERNACAO" 
   (	"ID_PERFIL_INTERNACAO" NUMBER(8,0), 
	"DT_COMPETENCIA" DATE, 
	"CD_MUNICIPIO_RESIDENCIA" CHAR(6 BYTE) COLLATE "USING_NLS_COMP", 
	"CD_CNES" CHAR(7 BYTE) COLLATE "USING_NLS_COMP", 
	"CD_CID10" VARCHAR2(5 BYTE) COLLATE "USING_NLS_COMP", 
	"CD_PROCEDIMENTO" CHAR(10 BYTE) COLLATE "USING_NLS_COMP", 
	"ID_FAIXA_ETARIA" NUMBER(2,0), 
	"CD_SEXO" CHAR(1 BYTE) COLLATE "USING_NLS_COMP", 
	"CD_RACA_COR" CHAR(2 BYTE) COLLATE "USING_NLS_COMP"
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_PERFIL_INTERNACAO"."ID_PERFIL_INTERNACAO" IS 'Identificador unico do perfil analitico de internacao.';
   COMMENT ON COLUMN "T_PERFIL_INTERNACAO"."DT_COMPETENCIA" IS 'Mes de competencia dos dados hospitalares.';
   COMMENT ON COLUMN "T_PERFIL_INTERNACAO"."CD_MUNICIPIO_RESIDENCIA" IS 'Codigo do municipio de residencia do paciente.';
   COMMENT ON COLUMN "T_PERFIL_INTERNACAO"."CD_CNES" IS 'Codigo CNES do estabelecimento responsavel pelo atendimento.';
   COMMENT ON COLUMN "T_PERFIL_INTERNACAO"."CD_CID10" IS 'Codigo CID-10 do diagnostico associado ao perfil de internacao.';
   COMMENT ON COLUMN "T_PERFIL_INTERNACAO"."CD_PROCEDIMENTO" IS 'Codigo do procedimento hospitalar realizado.';
   COMMENT ON COLUMN "T_PERFIL_INTERNACAO"."ID_FAIXA_ETARIA" IS 'Faixa etaria do perfil demografico do paciente.';
   COMMENT ON COLUMN "T_PERFIL_INTERNACAO"."CD_SEXO" IS 'Codigo de sexo associado ao perfil.';
   COMMENT ON COLUMN "T_PERFIL_INTERNACAO"."CD_RACA_COR" IS 'Codigo padronizado de raca/cor associado ao perfil.';
   COMMENT ON TABLE "T_PERFIL_INTERNACAO"  IS 'Tabela central de perfis analiticos de internacao por competencia, municipio, estabelecimento, diagnostico, procedimento e caracteristicas demograficas.';
--------------------------------------------------------
--  DDL for Table T_PROCEDIMENTO
--------------------------------------------------------

  CREATE TABLE "T_PROCEDIMENTO" 
   (	"CD_PROCEDIMENTO" CHAR(10 BYTE) COLLATE "USING_NLS_COMP", 
	"DS_PROCEDIMENTO" VARCHAR2(250 BYTE) COLLATE "USING_NLS_COMP", 
	"CD_GRUPO_PROCEDIMENTO" CHAR(2 BYTE) COLLATE "USING_NLS_COMP", 
	"NM_GRUPO_PROCEDIMENTO" VARCHAR2(80 BYTE) COLLATE "USING_NLS_COMP", 
	"TP_COMPLEXIDADE" VARCHAR2(30 BYTE) COLLATE "USING_NLS_COMP"
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_PROCEDIMENTO"."CD_PROCEDIMENTO" IS 'Codigo do procedimento hospitalar realizado no SUS.';
   COMMENT ON COLUMN "T_PROCEDIMENTO"."DS_PROCEDIMENTO" IS 'Descricao do procedimento hospitalar realizado no SUS.';
   COMMENT ON COLUMN "T_PROCEDIMENTO"."CD_GRUPO_PROCEDIMENTO" IS 'Codigo do grupo ao qual pertence o procedimento hospitalar.';
   COMMENT ON COLUMN "T_PROCEDIMENTO"."NM_GRUPO_PROCEDIMENTO" IS 'Nome do grupo do procedimento hospitalar.';
   COMMENT ON COLUMN "T_PROCEDIMENTO"."TP_COMPLEXIDADE" IS 'Nivel de complexidade assistencial associado ao procedimento.';
   COMMENT ON TABLE "T_PROCEDIMENTO"  IS 'Dimensao de procedimentos hospitalares do SUS, incluindo grupo e complexidade.';
--------------------------------------------------------
--  DDL for Table T_RACA_COR
--------------------------------------------------------

  CREATE TABLE "T_RACA_COR" 
   (	"CD_RACA_COR" CHAR(2 BYTE) COLLATE "USING_NLS_COMP", 
	"DS_RACA_COR" VARCHAR2(40 BYTE) COLLATE "USING_NLS_COMP"
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_RACA_COR"."CD_RACA_COR" IS 'Codigo padronizado de raca/cor utilizado pelo ETL do HUBSUS360.';
   COMMENT ON COLUMN "T_RACA_COR"."DS_RACA_COR" IS 'Descricao correspondente a categoria de raca/cor utilizada nas analises demograficas.';
   COMMENT ON TABLE "T_RACA_COR"  IS 'Dimensao de raca ou cor dos pacientes conforme codificacao dos dados hospitalares.';
--------------------------------------------------------
--  DDL for Table T_RESUMO_AIH
--------------------------------------------------------

  CREATE TABLE "T_RESUMO_AIH" 
   (	"ID_RESUMO_AIH" NUMBER(8,0), 
	"ID_PERFIL_INTERNACAO" NUMBER(8,0), 
	"QT_REGISTROS_AIH" NUMBER(3,0), 
	"QT_CONTINUIDADES_AIH" NUMBER(3,0), 
	"VL_TOTAL_AIH" NUMBER(12,2)
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_RESUMO_AIH"."ID_RESUMO_AIH" IS 'Identificador unico do resumo de AIH.';
   COMMENT ON COLUMN "T_RESUMO_AIH"."ID_PERFIL_INTERNACAO" IS 'Perfil analitico ao qual pertence o resumo de AIH; possui restricao UNIQUE para garantir relacao um-para-um.';
   COMMENT ON COLUMN "T_RESUMO_AIH"."QT_REGISTROS_AIH" IS 'Quantidade total de registros de AIH associados ao perfil de internacao.';
   COMMENT ON COLUMN "T_RESUMO_AIH"."QT_CONTINUIDADES_AIH" IS 'Quantidade de registros de continuidade de AIH, que nao representam nova internacao.';
   COMMENT ON COLUMN "T_RESUMO_AIH"."VL_TOTAL_AIH" IS 'Valor total aprovado das AIHs associadas ao perfil de internacao, em reais.';
   COMMENT ON TABLE "T_RESUMO_AIH"  IS 'Resumo financeiro e quantitativo dos registros de AIH associados a cada perfil de internacao.';
--------------------------------------------------------
--  DDL for Table T_RESUMO_ASSISTENCIAL
--------------------------------------------------------

  CREATE TABLE "T_RESUMO_ASSISTENCIAL" 
   (	"ID_RESUMO_ASSISTENCIAL" NUMBER(8,0), 
	"ID_PERFIL_INTERNACAO" NUMBER(8,0), 
	"QT_INTERNACOES" NUMBER(10,0), 
	"QT_INTERNACOES_ELEGIVEIS_ICSAP" NUMBER(10,0), 
	"QT_INTERNACOES_ICSAP" NUMBER(10,0), 
	"VL_TOTAL_ICSAP" NUMBER(14,2), 
	"VL_TOTAL_ELEGIVEL_IEP" NUMBER(14,2), 
	"QT_DIAS_PERMANENCIA" NUMBER(12,0), 
	"QT_SAIDAS_HOSPITALARES" NUMBER(10,0), 
	"QT_OBITOS" NUMBER(10,0)
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_RESUMO_ASSISTENCIAL"."ID_RESUMO_ASSISTENCIAL" IS 'Identificador unico do resumo assistencial.';
   COMMENT ON COLUMN "T_RESUMO_ASSISTENCIAL"."ID_PERFIL_INTERNACAO" IS 'Perfil analitico associado ao resumo; a restricao UNIQUE garante relacao um-para-um.';
   COMMENT ON COLUMN "T_RESUMO_ASSISTENCIAL"."QT_INTERNACOES" IS 'Quantidade de novas internacoes hospitalares associadas ao perfil.';
   COMMENT ON COLUMN "T_RESUMO_ASSISTENCIAL"."QT_INTERNACOES_ELEGIVEIS_ICSAP" IS 'Quantidade de internacoes elegiveis para o calculo do indicador ICSAP.';
   COMMENT ON COLUMN "T_RESUMO_ASSISTENCIAL"."QT_INTERNACOES_ICSAP" IS 'Quantidade de internacoes classificadas como Condicoes Sensiveis a Atencao Primaria.';
   COMMENT ON COLUMN "T_RESUMO_ASSISTENCIAL"."VL_TOTAL_ICSAP" IS 'Valor financeiro total associado as internacoes classificadas como ICSAP.';
   COMMENT ON COLUMN "T_RESUMO_ASSISTENCIAL"."VL_TOTAL_ELEGIVEL_IEP" IS 'Valor total elegivel utilizado no calculo do Indicador de Eficiencia Preventiva do HUBSUS360.';
   COMMENT ON COLUMN "T_RESUMO_ASSISTENCIAL"."QT_DIAS_PERMANENCIA" IS 'Total de dias de permanencia hospitalar.';
   COMMENT ON COLUMN "T_RESUMO_ASSISTENCIAL"."QT_SAIDAS_HOSPITALARES" IS 'Quantidade de saidas hospitalares utilizadas no calculo de indicadores assistenciais.';
   COMMENT ON COLUMN "T_RESUMO_ASSISTENCIAL"."QT_OBITOS" IS 'Quantidade de obitos hospitalares associados ao perfil de internacao.';
   COMMENT ON TABLE "T_RESUMO_ASSISTENCIAL"  IS 'Resumo assistencial por perfil de internacao, incluindo internacoes, ICSAP, permanencia, saidas hospitalares, obitos e valores elegiveis para indicadores.';
--------------------------------------------------------
--  DDL for Table T_SEXO
--------------------------------------------------------

  CREATE TABLE "T_SEXO" 
   (	"CD_SEXO" CHAR(1 BYTE) COLLATE "USING_NLS_COMP", 
	"DS_SEXO" VARCHAR2(20 BYTE) COLLATE "USING_NLS_COMP"
   )  DEFAULT COLLATION "USING_NLS_COMP" ;

   COMMENT ON COLUMN "T_SEXO"."CD_SEXO" IS 'Codigo de sexo conforme dominio final utilizado pelo ETL: 1 para masculino e 3 para feminino.';
   COMMENT ON COLUMN "T_SEXO"."DS_SEXO" IS 'Descricao correspondente ao codigo de sexo utilizado no modelo analitico.';
   COMMENT ON TABLE "T_SEXO"  IS 'Dimensao de sexo dos pacientes conforme codificacao utilizada nos dados hospitalares.';
--------------------------------------------------------
--  DDL for Index PK_CAPACIDADE_LEITO
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_CAPACIDADE_LEITO" ON "T_CAPACIDADE_LEITO" ("ID_CAPACIDADE_LEITO") 
  ;
--------------------------------------------------------
--  DDL for Index UK_CAP_LEITO_NEGOCIO
--------------------------------------------------------

  CREATE UNIQUE INDEX "UK_CAP_LEITO_NEGOCIO" ON "T_CAPACIDADE_LEITO" ("CD_CNES", "ID_CATEGORIA_LEITO", "DT_COMPETENCIA") 
  ;
--------------------------------------------------------
--  DDL for Index IX_CAP_LEITO_CNES
--------------------------------------------------------

  CREATE INDEX "IX_CAP_LEITO_CNES" ON "T_CAPACIDADE_LEITO" ("CD_CNES") 
  ;
--------------------------------------------------------
--  DDL for Index IX_CAP_LEITO_COMP
--------------------------------------------------------

  CREATE INDEX "IX_CAP_LEITO_COMP" ON "T_CAPACIDADE_LEITO" ("DT_COMPETENCIA") 
  ;
--------------------------------------------------------
--  DDL for Index PK_CATEGORIA_LEITO
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_CATEGORIA_LEITO" ON "T_CATEGORIA_LEITO" ("ID_CATEGORIA_LEITO") 
  ;
--------------------------------------------------------
--  DDL for Index UK_CATEGORIA_LEITO
--------------------------------------------------------

  CREATE UNIQUE INDEX "UK_CATEGORIA_LEITO" ON "T_CATEGORIA_LEITO" ("TP_LEITO", "DS_ESPECIALIDADE") 
  ;
--------------------------------------------------------
--  DDL for Index IX_CAT_LEITO_TIPO
--------------------------------------------------------

  CREATE INDEX "IX_CAT_LEITO_TIPO" ON "T_CATEGORIA_LEITO" ("TP_LEITO") 
  ;
--------------------------------------------------------
--  DDL for Index PK_DIAGNOSTICO
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_DIAGNOSTICO" ON "T_DIAGNOSTICO" ("CD_CID10") 
  ;
--------------------------------------------------------
--  DDL for Index IX_DIAG_GRUPO_ICSAP
--------------------------------------------------------

  CREATE INDEX "IX_DIAG_GRUPO_ICSAP" ON "T_DIAGNOSTICO" ("ID_GRUPO_ICSAP") 
  ;
--------------------------------------------------------
--  DDL for Index PK_ESTABELECIMENTO
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_ESTABELECIMENTO" ON "T_ESTABELECIMENTO" ("CD_CNES") 
  ;
--------------------------------------------------------
--  DDL for Index IX_ESTAB_MUNICIPIO
--------------------------------------------------------

  CREATE INDEX "IX_ESTAB_MUNICIPIO" ON "T_ESTABELECIMENTO" ("CD_MUNICIPIO") 
  ;
--------------------------------------------------------
--  DDL for Index IX_ESTAB_TIPO
--------------------------------------------------------

  CREATE INDEX "IX_ESTAB_TIPO" ON "T_ESTABELECIMENTO" ("TP_ESTABELECIMENTO") 
  ;
--------------------------------------------------------
--  DDL for Index PK_FAIXA_ETARIA
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_FAIXA_ETARIA" ON "T_FAIXA_ETARIA" ("ID_FAIXA_ETARIA") 
  ;
--------------------------------------------------------
--  DDL for Index UK_FAIXA_ETARIA_DESC
--------------------------------------------------------

  CREATE UNIQUE INDEX "UK_FAIXA_ETARIA_DESC" ON "T_FAIXA_ETARIA" ("DS_FAIXA_ETARIA") 
  ;
--------------------------------------------------------
--  DDL for Index PK_GRUPO_ICSAP
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_GRUPO_ICSAP" ON "T_GRUPO_ICSAP" ("ID_GRUPO_ICSAP") 
  ;
--------------------------------------------------------
--  DDL for Index UK_GRUPO_ICSAP_NOME
--------------------------------------------------------

  CREATE UNIQUE INDEX "UK_GRUPO_ICSAP_NOME" ON "T_GRUPO_ICSAP" ("NM_GRUPO_ICSAP") 
  ;
--------------------------------------------------------
--  DDL for Index IX_MUNICIPIO_REGIAO_SAUDE
--------------------------------------------------------

  CREATE INDEX "IX_MUNICIPIO_REGIAO_SAUDE" ON "T_MUNICIPIO" ("NM_REGIAO_SAUDE") 
  ;
--------------------------------------------------------
--  DDL for Index PK_MUNICIPIO
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_MUNICIPIO" ON "T_MUNICIPIO" ("CD_MUNICIPIO") 
  ;
--------------------------------------------------------
--  DDL for Index UK_MUNICIPIO_NOME_UF
--------------------------------------------------------

  CREATE UNIQUE INDEX "UK_MUNICIPIO_NOME_UF" ON "T_MUNICIPIO" ("NM_MUNICIPIO", "SG_UF") 
  ;
--------------------------------------------------------
--  DDL for Index PK_PERFIL_INTERNACAO
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_PERFIL_INTERNACAO" ON "T_PERFIL_INTERNACAO" ("ID_PERFIL_INTERNACAO") 
  ;
--------------------------------------------------------
--  DDL for Index PK_PROCEDIMENTO
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_PROCEDIMENTO" ON "T_PROCEDIMENTO" ("CD_PROCEDIMENTO") 
  ;
--------------------------------------------------------
--  DDL for Index IX_PROC_GRUPO
--------------------------------------------------------

  CREATE INDEX "IX_PROC_GRUPO" ON "T_PROCEDIMENTO" ("CD_GRUPO_PROCEDIMENTO") 
  ;
--------------------------------------------------------
--  DDL for Index IX_PROC_COMPLEXIDADE
--------------------------------------------------------

  CREATE INDEX "IX_PROC_COMPLEXIDADE" ON "T_PROCEDIMENTO" ("TP_COMPLEXIDADE") 
  ;
--------------------------------------------------------
--  DDL for Index PK_RACA_COR
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_RACA_COR" ON "T_RACA_COR" ("CD_RACA_COR") 
  ;
--------------------------------------------------------
--  DDL for Index UK_RACA_COR_DESC
--------------------------------------------------------

  CREATE UNIQUE INDEX "UK_RACA_COR_DESC" ON "T_RACA_COR" ("DS_RACA_COR") 
  ;
--------------------------------------------------------
--  DDL for Index PK_RESUMO_AIH
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_RESUMO_AIH" ON "T_RESUMO_AIH" ("ID_RESUMO_AIH") 
  ;
--------------------------------------------------------
--  DDL for Index UK_RESUMO_AIH_PERFIL
--------------------------------------------------------

  CREATE UNIQUE INDEX "UK_RESUMO_AIH_PERFIL" ON "T_RESUMO_AIH" ("ID_PERFIL_INTERNACAO") 
  ;
--------------------------------------------------------
--  DDL for Index UK_RES_ASSIST_PERFIL
--------------------------------------------------------

  CREATE UNIQUE INDEX "UK_RES_ASSIST_PERFIL" ON "T_RESUMO_ASSISTENCIAL" ("ID_PERFIL_INTERNACAO") 
  ;
--------------------------------------------------------
--  DDL for Index PK_RESUMO_ASSISTENCIAL
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_RESUMO_ASSISTENCIAL" ON "T_RESUMO_ASSISTENCIAL" ("ID_RESUMO_ASSISTENCIAL") 
  ;
--------------------------------------------------------
--  DDL for Index PK_SEXO
--------------------------------------------------------

  CREATE UNIQUE INDEX "PK_SEXO" ON "T_SEXO" ("CD_SEXO") 
  ;
--------------------------------------------------------
--  DDL for Index UK_SEXO_DESC
--------------------------------------------------------

  CREATE UNIQUE INDEX "UK_SEXO_DESC" ON "T_SEXO" ("DS_SEXO") 
  ;
--------------------------------------------------------
--  Constraints for Table T_CAPACIDADE_LEITO
--------------------------------------------------------

  ALTER TABLE "T_CAPACIDADE_LEITO" MODIFY ("ID_CAPACIDADE_LEITO" NOT NULL ENABLE);
  ALTER TABLE "T_CAPACIDADE_LEITO" MODIFY ("CD_CNES" NOT NULL ENABLE);
  ALTER TABLE "T_CAPACIDADE_LEITO" MODIFY ("ID_CATEGORIA_LEITO" NOT NULL ENABLE);
  ALTER TABLE "T_CAPACIDADE_LEITO" MODIFY ("DT_COMPETENCIA" NOT NULL ENABLE);
  ALTER TABLE "T_CAPACIDADE_LEITO" MODIFY ("QT_LEITOS_EXISTENTES" NOT NULL ENABLE);
  ALTER TABLE "T_CAPACIDADE_LEITO" MODIFY ("QT_LEITOS_SUS" NOT NULL ENABLE);
  ALTER TABLE "T_CAPACIDADE_LEITO" ADD CONSTRAINT "CK_CAP_LEITOS_EXIST" CHECK (QT_LEITOS_EXISTENTES >= 0) ENABLE;
  ALTER TABLE "T_CAPACIDADE_LEITO" ADD CONSTRAINT "CK_CAP_LEITOS_SUS" CHECK (QT_LEITOS_SUS >= 0) ENABLE;
  ALTER TABLE "T_CAPACIDADE_LEITO" ADD CONSTRAINT "CK_CAP_SUS_TOTAL" CHECK (QT_LEITOS_SUS <= QT_LEITOS_EXISTENTES) ENABLE;
  ALTER TABLE "T_CAPACIDADE_LEITO" ADD CONSTRAINT "PK_CAPACIDADE_LEITO" PRIMARY KEY ("ID_CAPACIDADE_LEITO")
  USING INDEX  ENABLE;
  ALTER TABLE "T_CAPACIDADE_LEITO" ADD CONSTRAINT "UK_CAP_LEITO_NEGOCIO" UNIQUE ("CD_CNES", "ID_CATEGORIA_LEITO", "DT_COMPETENCIA")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_CATEGORIA_LEITO
--------------------------------------------------------

  ALTER TABLE "T_CATEGORIA_LEITO" MODIFY ("ID_CATEGORIA_LEITO" NOT NULL ENABLE);
  ALTER TABLE "T_CATEGORIA_LEITO" MODIFY ("TP_LEITO" NOT NULL ENABLE);
  ALTER TABLE "T_CATEGORIA_LEITO" MODIFY ("DS_ESPECIALIDADE" NOT NULL ENABLE);
  ALTER TABLE "T_CATEGORIA_LEITO" ADD CONSTRAINT "CK_CATEGORIA_LEITO_ID" CHECK (ID_CATEGORIA_LEITO BETWEEN 1 AND 66) ENABLE;
  ALTER TABLE "T_CATEGORIA_LEITO" ADD CONSTRAINT "PK_CATEGORIA_LEITO" PRIMARY KEY ("ID_CATEGORIA_LEITO")
  USING INDEX  ENABLE;
  ALTER TABLE "T_CATEGORIA_LEITO" ADD CONSTRAINT "UK_CATEGORIA_LEITO" UNIQUE ("TP_LEITO", "DS_ESPECIALIDADE")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_DIAGNOSTICO
--------------------------------------------------------

  ALTER TABLE "T_DIAGNOSTICO" MODIFY ("CD_CID10" NOT NULL ENABLE);
  ALTER TABLE "T_DIAGNOSTICO" MODIFY ("CD_CATEGORIA_CID10" NOT NULL ENABLE);
  ALTER TABLE "T_DIAGNOSTICO" MODIFY ("DS_DIAGNOSTICO" NOT NULL ENABLE);
  ALTER TABLE "T_DIAGNOSTICO" ADD CONSTRAINT "CK_DIAG_CATEGORIA" CHECK (LENGTH(CD_CATEGORIA_CID10) = 3) ENABLE;
  ALTER TABLE "T_DIAGNOSTICO" ADD CONSTRAINT "PK_DIAGNOSTICO" PRIMARY KEY ("CD_CID10")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_ESTABELECIMENTO
--------------------------------------------------------

  ALTER TABLE "T_ESTABELECIMENTO" MODIFY ("CD_CNES" NOT NULL ENABLE);
  ALTER TABLE "T_ESTABELECIMENTO" MODIFY ("CD_MUNICIPIO" NOT NULL ENABLE);
  ALTER TABLE "T_ESTABELECIMENTO" MODIFY ("NM_ESTABELECIMENTO" NOT NULL ENABLE);
  ALTER TABLE "T_ESTABELECIMENTO" MODIFY ("TP_ESTABELECIMENTO" NOT NULL ENABLE);
  ALTER TABLE "T_ESTABELECIMENTO" ADD CONSTRAINT "PK_ESTABELECIMENTO" PRIMARY KEY ("CD_CNES")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_FAIXA_ETARIA
--------------------------------------------------------

  ALTER TABLE "T_FAIXA_ETARIA" MODIFY ("ID_FAIXA_ETARIA" NOT NULL ENABLE);
  ALTER TABLE "T_FAIXA_ETARIA" MODIFY ("DS_FAIXA_ETARIA" NOT NULL ENABLE);
  ALTER TABLE "T_FAIXA_ETARIA" ADD CONSTRAINT "CK_FAIXA_ETARIA_ID" CHECK (ID_FAIXA_ETARIA BETWEEN 1 AND 13) ENABLE;
  ALTER TABLE "T_FAIXA_ETARIA" ADD CONSTRAINT "CK_FAIXA_ETARIA_LIMITES" CHECK (
            NR_IDADE_MINIMA IS NULL
            OR NR_IDADE_MAXIMA IS NULL
            OR NR_IDADE_MINIMA <= NR_IDADE_MAXIMA
        ) ENABLE;
  ALTER TABLE "T_FAIXA_ETARIA" ADD CONSTRAINT "PK_FAIXA_ETARIA" PRIMARY KEY ("ID_FAIXA_ETARIA")
  USING INDEX  ENABLE;
  ALTER TABLE "T_FAIXA_ETARIA" ADD CONSTRAINT "UK_FAIXA_ETARIA_DESC" UNIQUE ("DS_FAIXA_ETARIA")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_GRUPO_ICSAP
--------------------------------------------------------

  ALTER TABLE "T_GRUPO_ICSAP" MODIFY ("NM_GRUPO_ICSAP" NOT NULL ENABLE);
  ALTER TABLE "T_GRUPO_ICSAP" MODIFY ("ID_GRUPO_ICSAP" NOT NULL ENABLE);
  ALTER TABLE "T_GRUPO_ICSAP" MODIFY ("DS_GRUPO_ICSAP" NOT NULL ENABLE);
  ALTER TABLE "T_GRUPO_ICSAP" ADD CONSTRAINT "CK_GRUPO_ICSAP_ID" CHECK (ID_GRUPO_ICSAP BETWEEN 1 AND 19) ENABLE;
  ALTER TABLE "T_GRUPO_ICSAP" ADD CONSTRAINT "PK_GRUPO_ICSAP" PRIMARY KEY ("ID_GRUPO_ICSAP")
  USING INDEX  ENABLE;
  ALTER TABLE "T_GRUPO_ICSAP" ADD CONSTRAINT "UK_GRUPO_ICSAP_NOME" UNIQUE ("NM_GRUPO_ICSAP")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_MUNICIPIO
--------------------------------------------------------

  ALTER TABLE "T_MUNICIPIO" MODIFY ("CD_MUNICIPIO" NOT NULL ENABLE);
  ALTER TABLE "T_MUNICIPIO" MODIFY ("NM_MUNICIPIO" NOT NULL ENABLE);
  ALTER TABLE "T_MUNICIPIO" MODIFY ("SG_UF" NOT NULL ENABLE);
  ALTER TABLE "T_MUNICIPIO" MODIFY ("NM_REGIAO_SAUDE" NOT NULL ENABLE);
  ALTER TABLE "T_MUNICIPIO" ADD CONSTRAINT "CK_MUNICIPIO_UF" CHECK (SG_UF = 'SP') ENABLE;
  ALTER TABLE "T_MUNICIPIO" ADD CONSTRAINT "PK_MUNICIPIO" PRIMARY KEY ("CD_MUNICIPIO")
  USING INDEX  ENABLE;
  ALTER TABLE "T_MUNICIPIO" ADD CONSTRAINT "UK_MUNICIPIO_NOME_UF" UNIQUE ("NM_MUNICIPIO", "SG_UF")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_PERFIL_INTERNACAO
--------------------------------------------------------

  ALTER TABLE "T_PERFIL_INTERNACAO" MODIFY ("ID_PERFIL_INTERNACAO" NOT NULL ENABLE);
  ALTER TABLE "T_PERFIL_INTERNACAO" MODIFY ("DT_COMPETENCIA" NOT NULL ENABLE);
  ALTER TABLE "T_PERFIL_INTERNACAO" MODIFY ("CD_MUNICIPIO_RESIDENCIA" NOT NULL ENABLE);
  ALTER TABLE "T_PERFIL_INTERNACAO" MODIFY ("CD_CNES" NOT NULL ENABLE);
  ALTER TABLE "T_PERFIL_INTERNACAO" MODIFY ("CD_CID10" NOT NULL ENABLE);
  ALTER TABLE "T_PERFIL_INTERNACAO" MODIFY ("CD_PROCEDIMENTO" NOT NULL ENABLE);
  ALTER TABLE "T_PERFIL_INTERNACAO" MODIFY ("ID_FAIXA_ETARIA" NOT NULL ENABLE);
  ALTER TABLE "T_PERFIL_INTERNACAO" MODIFY ("CD_SEXO" NOT NULL ENABLE);
  ALTER TABLE "T_PERFIL_INTERNACAO" MODIFY ("CD_RACA_COR" NOT NULL ENABLE);
  ALTER TABLE "T_PERFIL_INTERNACAO" ADD CONSTRAINT "PK_PERFIL_INTERNACAO" PRIMARY KEY ("ID_PERFIL_INTERNACAO")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_PROCEDIMENTO
--------------------------------------------------------

  ALTER TABLE "T_PROCEDIMENTO" MODIFY ("CD_PROCEDIMENTO" NOT NULL ENABLE);
  ALTER TABLE "T_PROCEDIMENTO" MODIFY ("DS_PROCEDIMENTO" NOT NULL ENABLE);
  ALTER TABLE "T_PROCEDIMENTO" MODIFY ("CD_GRUPO_PROCEDIMENTO" NOT NULL ENABLE);
  ALTER TABLE "T_PROCEDIMENTO" MODIFY ("NM_GRUPO_PROCEDIMENTO" NOT NULL ENABLE);
  ALTER TABLE "T_PROCEDIMENTO" MODIFY ("TP_COMPLEXIDADE" NOT NULL ENABLE);
  ALTER TABLE "T_PROCEDIMENTO" ADD CONSTRAINT "CK_PROC_COMPLEXIDADE" CHECK (
            TP_COMPLEXIDADE IN (
                'ALTA COMPLEXIDADE',
                'MEDIA COMPLEXIDADE',
                'NAO SE APLICA'
            )
        ) ENABLE;
  ALTER TABLE "T_PROCEDIMENTO" ADD CONSTRAINT "CK_PROC_GRUPO" CHECK (CD_GRUPO_PROCEDIMENTO IN ('02', '03', '04', '05')) ENABLE;
  ALTER TABLE "T_PROCEDIMENTO" ADD CONSTRAINT "PK_PROCEDIMENTO" PRIMARY KEY ("CD_PROCEDIMENTO")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_RACA_COR
--------------------------------------------------------

  ALTER TABLE "T_RACA_COR" MODIFY ("CD_RACA_COR" NOT NULL ENABLE);
  ALTER TABLE "T_RACA_COR" MODIFY ("DS_RACA_COR" NOT NULL ENABLE);
  ALTER TABLE "T_RACA_COR" ADD CONSTRAINT "CK_RACA_COR_CODIGO" CHECK (
            CD_RACA_COR IN (
                '01',
                '02',
                '03',
                '04',
                '05',
                '99'
            )
        ) ENABLE;
  ALTER TABLE "T_RACA_COR" ADD CONSTRAINT "PK_RACA_COR" PRIMARY KEY ("CD_RACA_COR")
  USING INDEX  ENABLE;
  ALTER TABLE "T_RACA_COR" ADD CONSTRAINT "UK_RACA_COR_DESC" UNIQUE ("DS_RACA_COR")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_RESUMO_AIH
--------------------------------------------------------

  ALTER TABLE "T_RESUMO_AIH" MODIFY ("ID_RESUMO_AIH" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_AIH" MODIFY ("ID_PERFIL_INTERNACAO" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_AIH" MODIFY ("QT_REGISTROS_AIH" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_AIH" MODIFY ("QT_CONTINUIDADES_AIH" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_AIH" MODIFY ("VL_TOTAL_AIH" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_AIH" ADD CONSTRAINT "CK_RES_AIH_REGISTROS" CHECK (QT_REGISTROS_AIH >= 1) ENABLE;
  ALTER TABLE "T_RESUMO_AIH" ADD CONSTRAINT "CK_RES_AIH_CONTINUIDADES" CHECK (
            QT_CONTINUIDADES_AIH >= 0
            AND QT_CONTINUIDADES_AIH <= QT_REGISTROS_AIH
        ) ENABLE;
  ALTER TABLE "T_RESUMO_AIH" ADD CONSTRAINT "CK_RES_AIH_VALOR" CHECK (VL_TOTAL_AIH >= 0) ENABLE;
  ALTER TABLE "T_RESUMO_AIH" ADD CONSTRAINT "PK_RESUMO_AIH" PRIMARY KEY ("ID_RESUMO_AIH")
  USING INDEX  ENABLE;
  ALTER TABLE "T_RESUMO_AIH" ADD CONSTRAINT "UK_RESUMO_AIH_PERFIL" UNIQUE ("ID_PERFIL_INTERNACAO")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_RESUMO_ASSISTENCIAL
--------------------------------------------------------

  ALTER TABLE "T_RESUMO_ASSISTENCIAL" MODIFY ("ID_RESUMO_ASSISTENCIAL" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" MODIFY ("ID_PERFIL_INTERNACAO" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" MODIFY ("QT_INTERNACOES" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" MODIFY ("QT_INTERNACOES_ELEGIVEIS_ICSAP" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" MODIFY ("QT_INTERNACOES_ICSAP" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" MODIFY ("VL_TOTAL_ICSAP" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" MODIFY ("VL_TOTAL_ELEGIVEL_IEP" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" MODIFY ("QT_DIAS_PERMANENCIA" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" MODIFY ("QT_SAIDAS_HOSPITALARES" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" MODIFY ("QT_OBITOS" NOT NULL ENABLE);
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" ADD CONSTRAINT "CK_RES_ASSIST_INTERNACOES" CHECK (QT_INTERNACOES >= 0) ENABLE;
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" ADD CONSTRAINT "CK_RES_ASSIST_ELEGIVEIS" CHECK (
            QT_INTERNACOES_ELEGIVEIS_ICSAP >= 0
            AND QT_INTERNACOES_ELEGIVEIS_ICSAP <= QT_INTERNACOES
        ) ENABLE;
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" ADD CONSTRAINT "CK_RES_ASSIST_ICSAP" CHECK (
            QT_INTERNACOES_ICSAP >= 0
            AND QT_INTERNACOES_ICSAP <= QT_INTERNACOES_ELEGIVEIS_ICSAP
        ) ENABLE;
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" ADD CONSTRAINT "CK_RES_ASSIST_VL_ICSAP" CHECK (VL_TOTAL_ICSAP >= 0) ENABLE;
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" ADD CONSTRAINT "CK_RES_ASSIST_VL_IEP" CHECK (VL_TOTAL_ELEGIVEL_IEP >= 0) ENABLE;
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" ADD CONSTRAINT "CK_RES_ASSIST_DIAS" CHECK (QT_DIAS_PERMANENCIA >= 0) ENABLE;
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" ADD CONSTRAINT "CK_RES_ASSIST_SAIDAS" CHECK (QT_SAIDAS_HOSPITALARES >= 0) ENABLE;
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" ADD CONSTRAINT "CK_RES_ASSIST_OBITOS" CHECK (
            QT_OBITOS >= 0
            AND QT_OBITOS <= QT_SAIDAS_HOSPITALARES
        ) ENABLE;
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" ADD CONSTRAINT "PK_RESUMO_ASSISTENCIAL" PRIMARY KEY ("ID_RESUMO_ASSISTENCIAL")
  USING INDEX  ENABLE;
  ALTER TABLE "T_RESUMO_ASSISTENCIAL" ADD CONSTRAINT "UK_RES_ASSIST_PERFIL" UNIQUE ("ID_PERFIL_INTERNACAO")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Constraints for Table T_SEXO
--------------------------------------------------------

  ALTER TABLE "T_SEXO" MODIFY ("CD_SEXO" NOT NULL ENABLE);
  ALTER TABLE "T_SEXO" MODIFY ("DS_SEXO" NOT NULL ENABLE);
  ALTER TABLE "T_SEXO" ADD CONSTRAINT "CK_SEXO_CODIGO" CHECK (CD_SEXO IN ('1', '3')) ENABLE;
  ALTER TABLE "T_SEXO" ADD CONSTRAINT "PK_SEXO" PRIMARY KEY ("CD_SEXO")
  USING INDEX  ENABLE;
  ALTER TABLE "T_SEXO" ADD CONSTRAINT "UK_SEXO_DESC" UNIQUE ("DS_SEXO")
  USING INDEX  ENABLE;
--------------------------------------------------------
--  Ref Constraints for Table T_CAPACIDADE_LEITO
--------------------------------------------------------

  ALTER TABLE "T_CAPACIDADE_LEITO" ADD CONSTRAINT "FK_CAP_LEITO_ESTAB" FOREIGN KEY ("CD_CNES")
	  REFERENCES "T_ESTABELECIMENTO" ("CD_CNES") ENABLE;
  ALTER TABLE "T_CAPACIDADE_LEITO" ADD CONSTRAINT "FK_CAP_LEITO_CATEG" FOREIGN KEY ("ID_CATEGORIA_LEITO")
	  REFERENCES "T_CATEGORIA_LEITO" ("ID_CATEGORIA_LEITO") ENABLE;
--------------------------------------------------------
--  Ref Constraints for Table T_DIAGNOSTICO
--------------------------------------------------------

  ALTER TABLE "T_DIAGNOSTICO" ADD CONSTRAINT "FK_DIAG_GRUPO_ICSAP" FOREIGN KEY ("ID_GRUPO_ICSAP")
	  REFERENCES "T_GRUPO_ICSAP" ("ID_GRUPO_ICSAP") ENABLE;
--------------------------------------------------------
--  Ref Constraints for Table T_ESTABELECIMENTO
--------------------------------------------------------

  ALTER TABLE "T_ESTABELECIMENTO" ADD CONSTRAINT "FK_ESTAB_MUNICIPIO" FOREIGN KEY ("CD_MUNICIPIO")
	  REFERENCES "T_MUNICIPIO" ("CD_MUNICIPIO") ENABLE;
--------------------------------------------------------
--  Ref Constraints for Table T_PERFIL_INTERNACAO
--------------------------------------------------------

  ALTER TABLE "T_PERFIL_INTERNACAO" ADD CONSTRAINT "FK_PERFIL_MUNICIPIO" FOREIGN KEY ("CD_MUNICIPIO_RESIDENCIA")
	  REFERENCES "T_MUNICIPIO" ("CD_MUNICIPIO") ENABLE;
  ALTER TABLE "T_PERFIL_INTERNACAO" ADD CONSTRAINT "FK_PERFIL_ESTAB" FOREIGN KEY ("CD_CNES")
	  REFERENCES "T_ESTABELECIMENTO" ("CD_CNES") ENABLE;
  ALTER TABLE "T_PERFIL_INTERNACAO" ADD CONSTRAINT "FK_PERFIL_DIAG" FOREIGN KEY ("CD_CID10")
	  REFERENCES "T_DIAGNOSTICO" ("CD_CID10") ENABLE;
  ALTER TABLE "T_PERFIL_INTERNACAO" ADD CONSTRAINT "FK_PERFIL_PROC" FOREIGN KEY ("CD_PROCEDIMENTO")
	  REFERENCES "T_PROCEDIMENTO" ("CD_PROCEDIMENTO") ENABLE;
  ALTER TABLE "T_PERFIL_INTERNACAO" ADD CONSTRAINT "FK_PERFIL_FAIXA" FOREIGN KEY ("ID_FAIXA_ETARIA")
	  REFERENCES "T_FAIXA_ETARIA" ("ID_FAIXA_ETARIA") ENABLE;
  ALTER TABLE "T_PERFIL_INTERNACAO" ADD CONSTRAINT "FK_PERFIL_SEXO" FOREIGN KEY ("CD_SEXO")
	  REFERENCES "T_SEXO" ("CD_SEXO") ENABLE;
  ALTER TABLE "T_PERFIL_INTERNACAO" ADD CONSTRAINT "FK_PERFIL_RACA" FOREIGN KEY ("CD_RACA_COR")
	  REFERENCES "T_RACA_COR" ("CD_RACA_COR") ENABLE;
--------------------------------------------------------
--  Ref Constraints for Table T_RESUMO_AIH
--------------------------------------------------------

  ALTER TABLE "T_RESUMO_AIH" ADD CONSTRAINT "FK_RESUMO_AIH_PERFIL" FOREIGN KEY ("ID_PERFIL_INTERNACAO")
	  REFERENCES "T_PERFIL_INTERNACAO" ("ID_PERFIL_INTERNACAO") ENABLE;
--------------------------------------------------------
--  Ref Constraints for Table T_RESUMO_ASSISTENCIAL
--------------------------------------------------------

  ALTER TABLE "T_RESUMO_ASSISTENCIAL" ADD CONSTRAINT "FK_RES_ASSIST_PERFIL" FOREIGN KEY ("ID_PERFIL_INTERNACAO")
	  REFERENCES "T_PERFIL_INTERNACAO" ("ID_PERFIL_INTERNACAO") ENABLE;

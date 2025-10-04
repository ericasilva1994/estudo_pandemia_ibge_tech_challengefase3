-- ==========================================
-- 02_analytics.sql  
-- ==========================================

-- • Este arquivo só cria VIEWS analíticas (não altera tabelas).
-- • Tudo parte da view public.pnad_covid_top20 criada no 01_views.sql.
-- • Sempre transformo respostas 1/2 em 0/1 com (NULLIF(col::text,'')::int = 1)::int.

SET search_path TO public;

-- =========================================================
-- 1) Painel mensal (métricas por mês)
--    -> pnad_covid_painel_mensal
-- =========================================================
CREATE OR REPLACE VIEW public.pnad_covid_painel_mensal AS
WITH base AS (
  SELECT
    referencia,
    idade,
    -- sintomas/uso como 0/1 (fáceis para média)
    (NULLIF(sint_falta_ar::text,'')::int = 1)::int  AS falta_ar,
    (NULLIF(sint_dor_peito::text,'')::int = 1)::int AS dor_peito,
    (NULLIF(tem_algum_sintoma::text,'')::int = 1)::int AS algum_sintoma,
    (NULLIF(procurou_servico::text,'')::int = 1)::int AS procurou,
    (NULLIF(internacao::text,'')::int = 1)::int      AS internou,
    (NULLIF(plano_saude::text,'')::int = 1)::int     AS plano
  FROM public.pnad_covid_top20
)
SELECT
  referencia,
  COUNT(*)                               AS n,
  AVG(falta_ar)::float                   AS prop_falta_ar,
  AVG(dor_peito)::float                  AS prop_dor_peito,
  AVG(algum_sintoma)::float              AS prop_algum_sintoma,
  AVG(plano)::float                      AS prop_plano_saude,
  AVG((idade >= 60)::int)::float         AS prop_60mais,
  CASE WHEN SUM(procurou) > 0
       THEN SUM((procurou=1 AND internou=1)::int)::float / NULLIF(SUM(procurou),0)
       ELSE NULL END                     AS prop_internou_entre_buscou
FROM base
GROUP BY referencia
ORDER BY referencia;

-- =========================================================
-- 2) Painel por UF + mês
--    -> pnad_covid_painel_uf
-- =========================================================
CREATE OR REPLACE VIEW public.pnad_covid_painel_uf AS
WITH base AS (
  SELECT
    referencia,
    uf,
    (NULLIF(sint_falta_ar::text,'')::int = 1)::int  AS falta_ar,
    (NULLIF(tem_algum_sintoma::text,'')::int = 1)::int AS algum_sintoma,
    (NULLIF(procurou_servico::text,'')::int = 1)::int AS procurou,
    (NULLIF(internacao::text,'')::int = 1)::int      AS internou
  FROM public.pnad_covid_top20
)
SELECT
  referencia,
  uf,
  COUNT(*)                     AS n,
  AVG(falta_ar)::float         AS prop_falta_ar,
  AVG(algum_sintoma)::float    AS prop_algum_sintoma,
  CASE WHEN SUM(procurou) > 0
       THEN SUM((procurou=1 AND internou=1)::int)::float / NULLIF(SUM(procurou),0)
       ELSE NULL END           AS prop_internou_entre_buscou
FROM base
GROUP BY referencia, uf
ORDER BY referencia, uf;

-- =========================================================
-- 3) Painel por FAIXA ETÁRIA + mês
--    -> pnad_covid_painel_faixa
-- =========================================================
CREATE OR REPLACE VIEW public.pnad_covid_painel_faixa AS
WITH base AS (
  SELECT
    referencia,
    CASE
      WHEN idade < 20 THEN '<20'
      WHEN idade BETWEEN 20 AND 39 THEN '20-39'
      WHEN idade BETWEEN 40 AND 59 THEN '40-59'
      WHEN idade >= 60 THEN '60+'
      ELSE 'sem_idade'
    END AS faixa,
    (NULLIF(sint_falta_ar::text,'')::int = 1)::int  AS falta_ar,
    (NULLIF(sint_dor_peito::text,'')::int = 1)::int AS dor_peito,
    (NULLIF(procurou_servico::text,'')::int = 1)::int AS procurou,
    (NULLIF(internacao::text,'')::int = 1)::int      AS internou
  FROM public.pnad_covid_top20
)
SELECT
  referencia,
  faixa,
  COUNT(*)                  AS n,
  AVG(falta_ar)::float      AS prop_falta_ar,
  AVG(dor_peito)::float     AS prop_dor_peito,
  CASE WHEN SUM(procurou) > 0
       THEN SUM((procurou=1 AND internou=1)::int)::float / NULLIF(SUM(procurou),0)
       ELSE NULL END        AS prop_internou_entre_buscou
FROM base
GROUP BY referencia, faixa
ORDER BY referencia, faixa;

-- =========================================================
-- 4) Correlações GERAIS (todos os meses juntos)
--    -> pnad_covid_correlacoes
-- =========================================================
-- IMPORTANTE: aqui havia o erro – faltava o "FROM base".
CREATE OR REPLACE VIEW public.pnad_covid_correlacoes AS
WITH base AS (
  SELECT
    -- alvo
    (NULLIF(internacao::text,'')::int = 1)::int      AS y,
    -- sintomas
    (NULLIF(sint_falta_ar::text,'')::int = 1)::int   AS x_falta_ar,
    (NULLIF(sint_dor_peito::text,'')::int = 1)::int  AS x_dor_peito,
    (NULLIF(tem_algum_sintoma::text,'')::int = 1)::int AS x_algum_sintoma,
    (NULLIF(sint_0011::text,'')::int = 1)::int       AS x_0011,
    (NULLIF(sint_0012::text,'')::int = 1)::int       AS x_0012,
    (NULLIF(sint_0013::text,'')::int = 1)::int       AS x_0013,
    (NULLIF(sint_0015::text,'')::int = 1)::int       AS x_0015,
    (NULLIF(sint_0019::text,'')::int = 1)::int       AS x_0019,
    (NULLIF(sint_00110::text,'')::int = 1)::int      AS x_00110,
    (NULLIF(sint_00111::text,'')::int = 1)::int      AS x_00111,
    (NULLIF(sint_00112::text,'')::int = 1)::int      AS x_00112,
    -- uso
    (NULLIF(procurou_servico::text,'')::int = 1)::int AS x_buscou,
    (NULLIF(plano_saude::text,'')::int = 1)::int      AS x_plano,
    -- demografia simplificada
    (idade >= 60)::int                                 AS x_idoso,
    (NULLIF(sexo::text,'')::int = 1)::int              AS x_sexo1,
    (NULLIF(raca_cor::text,'')::int = 1)::int          AS x_raca1,
    (NULLIF(escolaridade::text,'0')::int >= 3)::int    AS x_escola3mais,
    (NULLIF(condicao_no_domicilio::text,'')::int = 1)::int AS x_ref_pessoa
  FROM public.pnad_covid_top20
)
SELECT
  corr(x_falta_ar::float8 , y::float8)   AS corr_falta_ar,
  corr(x_dor_peito::float8, y::float8)   AS corr_dor_peito,
  corr(x_algum_sintoma::float8, y::float8) AS corr_algum_sintoma,
  corr(x_0011::float8, y::float8)        AS corr_0011,
  corr(x_0012::float8, y::float8)        AS corr_0012,
  corr(x_0013::float8, y::float8)        AS corr_0013,
  corr(x_0015::float8, y::float8)        AS corr_0015,
  corr(x_0019::float8, y::float8)        AS corr_0019,
  corr(x_00110::float8, y::float8)       AS corr_00110,
  corr(x_00111::float8, y::float8)       AS corr_00111,
  corr(x_00112::float8, y::float8)       AS corr_00112,
  corr(x_buscou::float8, y::float8)      AS corr_buscou_servico,
  corr(x_plano::float8 , y::float8)      AS corr_plano_saude,
  corr(x_idoso::float8 , y::float8)      AS corr_idoso60,
  corr(x_sexo1::float8 , y::float8)      AS corr_sexo_cat1,
  corr(x_raca1::float8 , y::float8)      AS corr_raca_cat1,
  corr(x_escola3mais::float8, y::float8) AS corr_escolaridade_3mais,
  corr(x_ref_pessoa::float8, y::float8)  AS corr_condicao_ref_pessoa
FROM base;

-- =========================================================
-- 5) Correlações POR MÊS
--    -> pnad_covid_correlacoes_mes
-- =========================================================
CREATE OR REPLACE VIEW public.pnad_covid_correlacoes_mes AS
WITH base AS (
  SELECT
    referencia,
    (NULLIF(internacao::text,'')::int = 1)::int      AS y,
    (NULLIF(sint_falta_ar::text,'')::int = 1)::int   AS x_falta_ar,
    (NULLIF(sint_dor_peito::text,'')::int = 1)::int  AS x_dor_peito,
    (NULLIF(tem_algum_sintoma::text,'')::int = 1)::int AS x_algum_sintoma,
    (NULLIF(sint_0011::text,'')::int = 1)::int       AS x_0011,
    (NULLIF(sint_0012::text,'')::int = 1)::int       AS x_0012,
    (NULLIF(sint_0013::text,'')::int = 1)::int       AS x_0013,
    (NULLIF(sint_0015::text,'')::int = 1)::int       AS x_0015,
    (NULLIF(sint_0019::text,'')::int = 1)::int       AS x_0019,
    (NULLIF(sint_00110::text,'')::int = 1)::int      AS x_00110,
    (NULLIF(sint_00111::text,'')::int = 1)::int      AS x_00111,
    (NULLIF(sint_00112::text,'')::int = 1)::int      AS x_00112,
    (NULLIF(procurou_servico::text,'')::int = 1)::int AS x_buscou,
    (NULLIF(plano_saude::text,'')::int = 1)::int      AS x_plano,
    (idade >= 60)::int                                 AS x_idoso
  FROM public.pnad_covid_top20
)
SELECT
  referencia,
  corr(x_falta_ar::float8 , y::float8)   AS corr_falta_ar,
  corr(x_dor_peito::float8, y::float8)   AS corr_dor_peito,
  corr(x_algum_sintoma::float8, y::float8) AS corr_algum_sintoma,
  corr(x_0011::float8, y::float8)        AS corr_0011,
  corr(x_0012::float8, y::float8)        AS corr_0012,
  corr(x_0013::float8, y::float8)        AS corr_0013,
  corr(x_0015::float8, y::float8)        AS corr_0015,
  corr(x_0019::float8, y::float8)        AS corr_0019,
  corr(x_00110::float8, y::float8)       AS corr_00110,
  corr(x_00111::float8, y::float8)       AS corr_00111,
  corr(x_00112::float8, y::float8)       AS corr_00112,
  corr(x_buscou::float8, y::float8)      AS corr_buscou_servico,
  corr(x_plano::float8 , y::float8)      AS corr_plano_saude,
  corr(x_idoso::float8 , y::float8)      AS corr_idoso60
FROM base
GROUP BY referencia
ORDER BY referencia;


-- =============================================================
-- (Q1) tendência mensal: % com "algum sintoma"
-- ideia: olhar se a percepção de sintomas subiu/ caiu ao longo dos 3 meses
-- =============================================================
SELECT
  referencia,
  ROUND(prop_algum_sintoma * 100, 2) AS pct_algum_sintoma
FROM public.pnad_covid_painel_mensal
ORDER BY referencia;

-- =============================================================
-- (Q2) resumo mensal de 3 indicadores "macro"
--  - % com plano de saúde (proxy de acesso)
--  - % 60+ (envelhecimento da amostra)
--  - % internou ENTRE os que buscaram serviço (gravidade condicional)
-- =============================================================
SELECT
  referencia,
  ROUND(prop_plano_saude * 100, 2)           AS pct_plano_saude,
  ROUND(prop_60mais * 100, 2)                AS pct_60_mais,
  ROUND(prop_internou_entre_buscou * 100,2)  AS pct_internou_dos_que_buscou
FROM public.pnad_covid_painel_mensal
ORDER BY referencia;

-- =============================================================
-- (Q3) TOP 10 UFs com maior proporção de falta de ar NO ÚLTIMO MÊS disponível
-- (foco em "quem está pior agora")
-- =============================================================
WITH ultimo AS (
  SELECT MAX(referencia) AS ref FROM public.pnad_covid_painel_uf
)
SELECT
  u.uf,
  u.referencia,
  ROUND(u.prop_falta_ar * 100, 2) AS pct_falta_ar
FROM public.pnad_covid_painel_uf u
JOIN ultimo x ON u.referencia = x.ref
ORDER BY u.prop_falta_ar DESC
LIMIT 10;

-- =============================================================
-- (Q4) Série temporal para uma UF (ex.: SP) vs BRASIL
-- truque: BRASIL = média simples das UFs (boa o bastante p/ comparar tendência)
-- =============================================================
WITH br AS (
  SELECT
    referencia,
    AVG(prop_algum_sintoma) AS br_prop_algum_sintoma
  FROM public.pnad_covid_painel_uf
  GROUP BY referencia
)
SELECT
  u.referencia,
  ROUND(u.prop_algum_sintoma * 100, 2) AS sp_pct_algum_sintoma,
  ROUND(b.br_prop_algum_sintoma * 100,2) AS br_pct_algum_sintoma
FROM public.pnad_covid_painel_uf u
JOIN br b USING (referencia)
WHERE u.uf = 'SP'
ORDER BY u.referencia;

-- =============================================================
-- (Q5) Por faixa etária: internou ENTRE os que buscaram (risco condicional)
-- dica: filtrar faixas específicas quando quiser (ex.: '60+')
-- =============================================================
SELECT
  referencia,
  faixa,
  ROUND(prop_internou_entre_buscou * 100, 2) AS pct_internou_dos_que_buscou
FROM public.pnad_covid_painel_faixa
ORDER BY referencia, faixa;

-- =============================================================
-- (Q6) Correlações "gerais" com internação (1=sim, 0=não)
-- leitura: quanto mais perto de 1 (positivo) | -1 (negativo), mais forte a associação
-- =============================================================
SELECT
  corr_falta_ar,
  corr_dor_peito,
  corr_algum_sintoma,
  corr_buscou_servico,
  corr_plano_saude,
  corr_idoso60
FROM public.pnad_covid_correlacoes;

-- =============================================================
-- (Q7) Correlações mês a mês (ex.: falta de ar e dor no peito)
-- leitura: ver se a força da associação muda com o tempo
-- =============================================================
SELECT
  referencia,
  corr_falta_ar,
  corr_dor_peito
FROM public.pnad_covid_correlacoes_mes
ORDER BY referencia;

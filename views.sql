/* ===========================================================================
   01_views.sql
   objetivo: criar visões (views) para:
     unificar os 3 meses da PNAD COVID em 1 visão só
    projetar as 20+ variáveis-chave em uma visão “top20” para análises

   ideia principal:
     descobrir quais colunas são IGUAIS nas 3 tabelas
     criar a VIEW unificada usando SÓ as colunas comuns
       (e ainda adiciono uma coluna “referencia” com a data do mês).
   ========================================================================== */
/* ---------------------------------------------------------------------------
 ver a “interseção” de colunas existentes nas 3 tabelas
 gero uma string “col1, col2, col3, …” já formatada
   - OBS: a ordem segue a do mês 05/2020 (padronizo por ele)
--------------------------------------------------------------------------- */
WITH cols_0520 AS (
  SELECT column_name, ordinal_position
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='pnad_covid_052020'
),
cols_0820 AS (
  SELECT column_name
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='pnad_covid_082020'
),
cols_1120 AS (
  SELECT column_name
  FROM information_schema.columns
  WHERE table_schema='public' AND table_name='pnad_covid_112020'
),
cols_comuns AS (
  SELECT c.column_name, c.ordinal_position
  FROM cols_0520 c
  WHERE c.column_name IN (SELECT column_name FROM cols_0820)
    AND c.column_name IN (SELECT column_name FROM cols_1120)
)
SELECT string_agg(format('%I', column_name), ', ' ORDER BY ordinal_position) AS lista_colunas_comuns
FROM cols_comuns;
/* dica: essa consulta retorna 1 linha com o texto:
   ano, uf, capital, … (uso para auditar se está tudo ok) */


/* ---------------------------------------------------------------------------
 criar/atualizar a VIEW unificada usando SQL dinâmica
  deixo o banco montar a lista de colunas e criar a view
   - nome da view: public.pnad_covid_2020_auto
   - se amanhã eu recarregar as tabelas com colunas diferentes,
     basta executar de novo e a view se ajusta às colunas comuns.
--------------------------------------------------------------------------- */
DO $$
DECLARE
  collist text;   -- onde vou guardar “col1, col2, col3, …”
  sqlview text;   
BEGIN
  -- 2.1) montar a lista de colunas comuns NA ORDEM do 05/2020
  SELECT string_agg(format('%I', c0520.column_name), ', ' ORDER BY c0520.ordinal_position)
  INTO collist
  FROM information_schema.columns c0520
  JOIN information_schema.columns c0820
    ON c0820.table_schema='public'
   AND c0820.table_name='pnad_covid_082020'
   AND c0820.column_name=c0520.column_name
  JOIN information_schema.columns c1120
    ON c1120.table_schema='public'
   AND c1120.table_name='pnad_covid_112020'
   AND c1120.column_name=c0520.column_name
  WHERE c0520.table_schema='public'
    AND c0520.table_name='pnad_covid_052020';

  IF collist IS NULL OR collist = '' THEN
    RAISE EXCEPTION 'não encontrei colunas comuns entre pnad_covid_052020/082020/112020.';
  END IF;

  -- 2.2) montar a SQL da VIEW (sempre a MESMA lista em cada SELECT)
  sqlview := format($f$
    CREATE OR REPLACE VIEW public.pnad_covid_2020_auto AS
    SELECT DATE '2020-05-01' AS referencia, %1$s FROM public.pnad_covid_052020
    UNION ALL
    SELECT DATE '2020-08-01' AS referencia, %1$s FROM public.pnad_covid_082020
    UNION ALL
    SELECT DATE '2020-11-01' AS referencia, %1$s FROM public.pnad_covid_112020;
  $f$, collist);

  -- 2.3) executar a criação/atualização da VIEW
  EXECUTE sqlview;

  RAISE NOTICE 'VIEW criada/atualizada: public.pnad_covid_2020_auto';
  RAISE NOTICE 'colunas usadas (interseção): %', collist;
END $$;


-- checagem rápida: ver contagem por mês
SELECT referencia, COUNT(*) AS linhas
FROM public.pnad_covid_2020_auto
GROUP BY referencia
ORDER BY referencia;


/* ---------------------------------------------------------------------------
 criar/atualizar a VIEW com as variáveis de interesse (“top20”)
 facilitar a análise (tipos coerentes e 1 variável derivada)
   - uso a view unificada como fonte
--------------------------------------------------------------------------- */
CREATE OR REPLACE VIEW public.pnad_covid_top20 AS
SELECT
  referencia,

  -- demografia (casts simples onde faz sentido)
  NULLIF(a002::text,'')::int AS idade,
  a003       AS sexo,
  a004       AS raca_cor,
  a005       AS escolaridade,
  a001a      AS condicao_no_domicilio,
  uf         AS uf,

  -- sintomas chave (1=sim, 2=não na PNAD)
  b0014 AS sint_falta_ar,
  b0016 AS sint_dor_peito,
  b0011 AS sint_0011,
  b0012 AS sint_0012,
  b0013 AS sint_0013,
  b0015 AS sint_0015,
  b0019 AS sint_0019,
  b00110 AS sint_00110,
  b00111 AS sint_00111,
  b00112 AS sint_00112,

  -- uso de serviços/condições
  b002 AS procurou_servico,
  b005 AS internacao,
  b007 AS plano_saude,

  -- variável derivada: “tem ao menos um sintoma”
  CASE WHEN (
    COALESCE(b0011,0)=1 OR COALESCE(b0012,0)=1 OR COALESCE(b0013,0)=1 OR
    COALESCE(b0014,0)=1 OR COALESCE(b0015,0)=1 OR COALESCE(b0016,0)=1 OR
    COALESCE(b0019,0)=1 OR COALESCE(b00110,0)=1 OR COALESCE(b00111,0)=1 OR
    COALESCE(b00112,0)=1
  )
  THEN 1 ELSE 2 END AS tem_algum_sintoma
FROM public.pnad_covid_2020_auto;







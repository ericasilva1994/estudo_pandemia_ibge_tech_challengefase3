# graficos.py — versão padronizada (azul + profissional)
# ---------------------------------------------
# Notas:
# - Apenas LEITURA do banco.
# - Gráficos claros p/ público leigo, todos em % quando aplicável.
# - “Bottom 10” -> “10 MENORES”; “Top 10” -> “10 MAIORES”.
# ---------------------------------------------

import os
import math
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.cm import get_cmap
from sqlalchemy import create_engine, text, inspect
from dotenv import load_dotenv

# ------------------------------
# 0) Setup visual (tema azul)
# ------------------------------
sns.set_theme(style="whitegrid")
plt.rcParams["figure.dpi"] = 120
plt.rcParams["savefig.dpi"] = 120
plt.rcParams["axes.titlesize"] = 11
plt.rcParams["axes.labelsize"] = 10
plt.rcParams["xtick.labelsize"] = 9
plt.rcParams["ytick.labelsize"] = 9

_CMAP = get_cmap("Blues")
def blues_n(n):
    return [_CMAP(0.35 + 0.5*i/max(1, n-1)) for i in range(n)]

AZUL = {
    "linha": _CMAP(0.75),
    "destaque": _CMAP(0.85),
    "escuro": _CMAP(0.55),
    "claro": _CMAP(0.30),
    "grade": (0.1, 0.1, 0.1, .08)
}

mpl.rcParams["axes.edgecolor"] = "#1f2937"
mpl.rcParams["grid.color"] = AZUL["grade"]
mpl.rcParams["axes.prop_cycle"] = mpl.cycler(color=blues_n(6))

# ------------------------------
# 1) Acesso ao banco (.env)
# ------------------------------
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "bd_relacional")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS")

def make_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    return create_engine(url, pool_pre_ping=True)

engine = make_engine()

# ------------------------------
# 2) Utilitários
# ------------------------------
OUT_DIR = "figs"
os.makedirs(OUT_DIR, exist_ok=True)

def savefig(name: str):
    path = os.path.join(OUT_DIR, f"{name}.png")
    plt.tight_layout()
    plt.savefig(path, bbox_inches="tight")
    print(f"[OK] Figura salva em: {path}")

def df_sql(sql: str) -> pd.DataFrame:
    with engine.begin() as conn:
        return pd.read_sql(text(sql), conn)

UF_SIGLA = {
    11:"RO",12:"AC",13:"AM",14:"RR",15:"PA",16:"AP",17:"TO",
    21:"MA",22:"PI",23:"CE",24:"RN",25:"PB",26:"PE",27:"AL",28:"SE",29:"BA",
    31:"MG",32:"ES",33:"RJ",35:"SP",
    41:"PR",42:"SC",43:"RS",
    50:"MS",51:"MT",52:"GO",53:"DF"
}
def uf_to_sigla(x):
    if isinstance(x, str) and len(x) <= 2:
        return x.upper()
    try:
        i = int(x)
        return UF_SIGLA.get(i, str(x))
    except Exception:
        return str(x)

MESES_2020 = ("2020-05-01","2020-08-01","2020-11-01")

# ---------------------------------------------------------
# 3) Consultas base (usam as VIEWS criadas no 02_analytics.sql)
# ---------------------------------------------------------
SQL_MENSAL = f"""
SELECT
  referencia,
  prop_falta_ar,
  prop_dor_peito,
  prop_algum_sintoma,
  prop_plano_saude,
  prop_60mais,
  prop_internou_entre_buscou
FROM public.pnad_covid_painel_mensal
WHERE referencia IN (DATE '{MESES_2020[0]}', DATE '{MESES_2020[1]}', DATE '{MESES_2020[2]}')
ORDER BY referencia;
"""

SQL_UF = f"""
SELECT
  referencia,
  uf,
  prop_falta_ar,
  prop_algum_sintoma,
  prop_internou_entre_buscou
FROM public.pnad_covid_painel_uf
WHERE referencia IN (DATE '{MESES_2020[0]}', DATE '{MESES_2020[1]}', DATE '{MESES_2020[2]}')
ORDER BY referencia, uf;
"""

SQL_FAIXA = f"""
SELECT
  referencia,
  faixa,
  prop_falta_ar,
  prop_dor_peito,
  prop_internou_entre_buscou
FROM public.pnad_covid_painel_faixa
WHERE referencia IN (DATE '{MESES_2020[0]}', DATE '{MESES_2020[1]}', DATE '{MESES_2020[2]}')
ORDER BY referencia, faixa;
"""

SQL_SEXO = f"""
SELECT
  referencia,
  CASE
    WHEN NULLIF(sexo::text,'')::int = 1 THEN 'Homem'
    WHEN NULLIF(sexo::text,'')::int = 2 THEN 'Mulher'
    ELSE 'Sem info'
  END AS sexo,
  AVG((tem_algum_sintoma = 1)::int)::float AS prop_algum_sintoma
FROM public.pnad_covid_top20
WHERE referencia IN (DATE '{MESES_2020[0]}', DATE '{MESES_2020[1]}', DATE '{MESES_2020[2]}')
GROUP BY referencia, sexo
ORDER BY referencia, sexo;
"""

SQL_ESCOLAR = f"""
SELECT
  referencia,
  CASE
    WHEN NULLIF(escolaridade::text,'')::int BETWEEN 1 AND 2 THEN 'Fundamental'
    WHEN NULLIF(escolaridade::text,'')::int BETWEEN 3 AND 4 THEN 'Médio'
    WHEN NULLIF(escolaridade::text,'')::int >= 5 THEN 'Superior+'
    ELSE 'Sem info'
  END AS escolaridade_grp,
  AVG((plano_saude = 1)::int)::float   AS prop_plano,
  AVG((procurou_servico = 1)::int)::float AS prop_buscou
FROM public.pnad_covid_top20
WHERE referencia IN (DATE '{MESES_2020[0]}', DATE '{MESES_2020[1]}', DATE '{MESES_2020[2]}')
GROUP BY referencia, escolaridade_grp
ORDER BY referencia, escolaridade_grp;
"""

# ---------------------------------------------------------
# 4) Carregar dados
# ---------------------------------------------------------
df_mensal = df_sql(SQL_MENSAL)
df_uf     = df_sql(SQL_UF)
df_fx     = df_sql(SQL_FAIXA)
df_sexo   = df_sql(SQL_SEXO)
df_escol  = df_sql(SQL_ESCOLAR)

for df in (df_mensal, df_uf, df_fx, df_sexo, df_escol):
    df["ref_str"] = pd.to_datetime(df["referencia"]).dt.strftime("%Y-%m")
df_uf["uf_sigla"] = df_uf["uf"].apply(uf_to_sigla)

# ---------------------------------------------------------
# 5) Funções de plot (azul)
# ---------------------------------------------------------
def to_pct(s): return (s * 100.0).round(1)

def linha_mes(ax, x, y, titulo, ylabel):
    ax.plot(x, y, marker="o", linewidth=2.2, color=AZUL["linha"])
    for xi, yi in zip(x, y):
        ax.text(xi, yi, f"{yi:.1f}%", ha="center", va="bottom", fontsize=8, color=AZUL["escuro"])
    ax.set_title(titulo)
    ax.set_xlabel("Mês de referência (2020)")
    ax.set_ylabel(ylabel)
    ax.set_ylim(0, max(5.0, math.ceil(max(y) / 5) * 5))
    ax.grid(True, alpha=.3)

def barras_rank(ax, df_rank, titulo, xlabel):
    colors = blues_n(len(df_rank))
    ax.barh(df_rank["uf_sigla"], df_rank["pct"], color=colors)
    for i, (sigla, val) in enumerate(zip(df_rank["uf_sigla"], df_rank["pct"])):
        ax.text(val + 0.2, i, f"{val:.1f}%", va="center", fontsize=8, color=AZUL["escuro"])
    ax.set_title(titulo)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("UF")
    ax.set_xlim(0, max(5.0, math.ceil(df_rank["pct"].max() / 5) * 5))
    ax.grid(True, axis="x", alpha=.3)

# ---------------------------------------------------------
# 6) A1 – % com “Algum Sintoma” (linha)
# ---------------------------------------------------------
plt.figure(figsize=(7,4))
y = to_pct(df_mensal["prop_algum_sintoma"])
linha_mes(plt.gca(), df_mensal["ref_str"], y,
          titulo="% de pessoas com ALGUM SINTOMA (amostra PNAD COVID-2020)",
          ylabel="% da amostra")
savefig("A1_algum_sintoma_mensal"); plt.close()

# ---------------------------------------------------------
# 7) A2 – % com Falta de Ar (linha)
# ---------------------------------------------------------
plt.figure(figsize=(7,4))
y = to_pct(df_mensal["prop_falta_ar"])
linha_mes(plt.gca(), df_mensal["ref_str"], y,
          titulo="% com FALTA DE AR (sintoma chave) ao longo dos meses",
          ylabel="% da amostra")
savefig("A2_falta_ar_mensal"); plt.close()

# ---------------------------------------------------------
# 8) A3 – Rankings por UF (10 MAIORES e 10 MENORES) para cada mês
# ---------------------------------------------------------
for ref in MESES_2020:
    lab = pd.to_datetime(ref).strftime("%Y-%m")
    base = df_uf[df_uf["ref_str"] == lab].copy()
    base["pct"] = to_pct(base["prop_falta_ar"])
    base = base.dropna(subset=["pct"])
    if base.empty: continue

    dez_maiores = base.nlargest(10, "pct").sort_values("pct", ascending=True)
    plt.figure(figsize=(7,4.5))
    barras_rank(plt.gca(), dez_maiores,
                titulo=f"10 MAIORES UFs por % de FALTA DE AR — {lab}",
                xlabel="% com falta de ar")
    savefig(f"A3_10maiores_falta_ar_{lab}"); plt.close()

    dez_menores = base.nsmallest(10, "pct").sort_values("pct", ascending=True)
    plt.figure(figsize=(7,4.5))
    barras_rank(plt.gca(), dez_menores,
                titulo=f"10 MENORES UFs por % de FALTA DE AR — {lab}",
                xlabel="% com falta de ar")
    savefig(f"A3_10menores_falta_ar_{lab}"); plt.close()

# ---------------------------------------------------------
# 9) B1 – Heatmap: Falta de Ar por Faixa Etária e Mês (Blues)
# ---------------------------------------------------------
ord_faixa = pd.CategoricalDtype(['<20','20-39','40-59','60+'], ordered=True)
df_fx = df_fx[df_fx["faixa"].isin(ord_faixa.categories)].copy()
df_fx["faixa"] = df_fx["faixa"].astype(ord_faixa)

tab = df_fx.pivot_table(index="faixa", columns="ref_str",
                        values="prop_falta_ar", aggfunc="mean")
tab = tab.reindex(sorted(tab.columns, key=lambda s: pd.to_datetime(s)), axis=1)

plt.figure(figsize=(6.4,4.2))
sns.heatmap(tab * 100, annot=True, fmt=".1f", cmap="Blues")
plt.title("% com FALTA DE AR por FAIXA ETÁRIA e mês")
plt.xlabel("Mês de referência (2020)"); plt.ylabel("Faixa etária")
savefig("B1_heatmap_falta_ar_faixa"); plt.close()

# ---------------------------------------------------------
# 10) B2 – % com “Algum Sintoma” por Sexo (barras agrupadas azuis)
# ---------------------------------------------------------
tab_sexo = df_sexo.pivot_table(index="ref_str", columns="sexo",
                               values="prop_algum_sintoma", aggfunc="mean")
tab_sexo = tab_sexo.reindex(sorted(tab_sexo.index, key=lambda s: pd.to_datetime(s)))

plt.figure(figsize=(7.5,4.2)); ax = plt.gca()
cols = list(tab_sexo.columns); n = len(cols); colors = blues_n(n)
width = 0.8 / n; x = np.arange(len(tab_sexo.index))
for i, c in enumerate(cols):
    vals = (tab_sexo[c]*100).values
    ax.bar(x + i*width, vals, width=width, label=c, color=colors[i])
    for xi, yi in zip(x + i*width, vals):
        ax.text(xi, yi+0.2, f"{yi:.1f}%", ha="center", va="bottom", fontsize=8, color=AZUL["escuro"])
ax.set_xticks(x + width*(n-1)/2); ax.set_xticklabels(tab_sexo.index)
ax.set_title("% com ALGUM SINTOMA por SEXO"); ax.set_xlabel("Mês de referência (2020)")
ax.set_ylabel("% da amostra"); ax.legend(title="Sexo"); ax.grid(True, axis="y", alpha=.3)
savefig("B2_algum_sintoma_sexo"); plt.close()

# ---------------------------------------------------------
# 11) C1 – % com Plano de Saúde por Escolaridade (barras)
# ---------------------------------------------------------
tab_escola_plano = df_escol.pivot_table(index="ref_str", columns="escolaridade_grp",
                                        values="prop_plano", aggfunc="mean")
tab_escola_plano = tab_escola_plano.reindex(sorted(tab_escola_plano.index, key=lambda s: pd.to_datetime(s)))

plt.figure(figsize=(7.5,4.2)); ax = plt.gca()
cols = list(tab_escola_plano.columns); n = len(cols); colors = blues_n(n)
width = 0.8 / n; x = np.arange(len(tab_escola_plano.index))
for i, c in enumerate(cols):
    vals = (tab_escola_plano[c]*100).values
    ax.bar(x + i*width, vals, width=width, label=c, color=colors[i])
    for xi, yi in zip(x + i*width, vals):
        ax.text(xi, yi+0.2, f"{yi:.1f}%", ha="center", va="bottom", fontsize=8, color=AZUL["escuro"])
ax.set_xticks(x + width*(n-1)/2); ax.set_xticklabels(tab_escola_plano.index)
ax.set_title("% com PLANO DE SAÚDE por ESCOLARIDADE"); ax.set_xlabel("Mês de referência (2020)")
ax.set_ylabel("% da amostra"); ax.legend(title="Escolaridade"); ax.grid(True, axis="y", alpha=.3)
savefig("C1_plano_saude_escolaridade"); plt.close()

# ---------------------------------------------------------
# 12) C2 – % que Procurou Serviço por Escolaridade (barras)
# ---------------------------------------------------------
tab_escola_busca = df_escol.pivot_table(index="ref_str", columns="escolaridade_grp",
                                        values="prop_buscou", aggfunc="mean")
tab_escola_busca = tab_escola_busca.reindex(sorted(tab_escola_busca.index, key=lambda s: pd.to_datetime(s)))

plt.figure(figsize=(7.5,4.2)); ax = plt.gca()
cols = list(tab_escola_busca.columns); n = len(cols); colors = blues_n(n)
width = 0.8 / n; x = np.arange(len(tab_escola_busca.index))
for i, c in enumerate(cols):
    vals = (tab_escola_busca[c]*100).values
    ax.bar(x + i*width, vals, width=width, label=c, color=colors[i])
    for xi, yi in zip(x + i*width, vals):
        ax.text(xi, yi+0.2, f"{yi:.1f}%", ha="center", va="bottom", fontsize=8, color=AZUL["escuro"])
ax.set_xticks(x + width*(n-1)/2); ax.set_xticklabels(tab_escola_busca.index)
ax.set_title("% que PROCUROU SERVIÇO DE SAÚDE por ESCOLARIDADE"); ax.set_xlabel("Mês de referência (2020)")
ax.set_ylabel("% da amostra"); ax.legend(title="Escolaridade"); ax.grid(True, axis="y", alpha=.3)
savefig("C2_procurou_servico_escolaridade"); plt.close()

# ---------------------------------------------------------
# 13) A4 – Taxa de internação entre quem buscou atendimento (linha)
# ---------------------------------------------------------
plt.figure(figsize=(7,4))
y = to_pct(df_mensal["prop_internou_entre_buscou"].fillna(0))
linha_mes(plt.gca(), df_mensal["ref_str"], y,
          titulo="TAXA DE INTERNAÇÃO entre quem buscou atendimento",
          ylabel="% entre os que buscaram")
savefig("A4_taxa_internacao_entre_que_buscou"); plt.close()

# ---------------------------------------------------------
# 14) S1 – CORRELAÇÃO entre indicadores por UF (por mês) — heatmap (Blues)
# ---------------------------------------------------------
indic_cols = ["prop_algum_sintoma", "prop_falta_ar", "prop_internou_entre_buscou"]
indic_labels = {
    "prop_algum_sintoma": "% com ALGUM SINTOMA",
    "prop_falta_ar": "% com FALTA DE AR",
    "prop_internou_entre_buscou": "% INTERNOU entre quem BUSCOU"
}
for lab, grupo in df_uf.groupby("ref_str"):
    g = grupo[indic_cols].dropna()
    if g.shape[0] < 3: continue
    corr_mat = g.corr(method="pearson").rename(index=indic_labels, columns=indic_labels)
    plt.figure(figsize=(6.4, 4.6))
    sns.heatmap(corr_mat, vmin=-1, vmax=1, center=0, cmap="Blues",
                annot=True, fmt=".2f", cbar_kws={"label": "Correlação (Pearson)"})
    plt.title(f"Correlação entre indicadores por UF — {lab}")
    plt.xlabel("Indicadores"); plt.ylabel("Indicadores")
    savefig(f"S1_correlacao_heatmap_{lab}"); plt.close()

# ---------------------------------------------------------
# 15) S2 – Dispersão: ALGUM SINTOMA × INTERNOU ENTRE QUEM BUSCOU (por UF)
#     (100% Matplotlib: regressão por numpy.polyfit)
# ---------------------------------------------------------
def nice_axis_limit(series_pct):
    m = float(series_pct.max() if len(series_pct) else 0)
    return max(5.0, math.ceil(m / 5.0) * 5.0)

for lab, grupo in df_uf.groupby("ref_str"):
    g = grupo.dropna(subset=["prop_algum_sintoma", "prop_internou_entre_buscou"]).copy()
    if g.empty: continue
    g["pct_sintomas"] = (g["prop_algum_sintoma"] * 100).round(1)
    g["pct_internou_busca"] = (g["prop_internou_entre_buscou"] * 100).round(1)

    plt.figure(figsize=(6.8, 4.6)); ax = plt.gca()
    ax.scatter(g["pct_sintomas"], g["pct_internou_busca"], s=35, color=AZUL["linha"])
    # linha de tendência
    k, b = np.polyfit(g["pct_sintomas"], g["pct_internou_busca"], 1)
    xline = np.linspace(0, max(g["pct_sintomas"].max(), 1), 100)
    ax.plot(xline, k*xline + b, linewidth=2, color=AZUL["escuro"])

    for _, row in g.iterrows():
        ax.text(row["pct_sintomas"] + 0.1, row["pct_internou_busca"] + 0.1,
                uf_to_sigla(row["uf"]), fontsize=8, alpha=0.9, color=AZUL["escuro"])

    ax.set_title(f"Relação: % com ALGUM SINTOMA × % INTERNOU (entre quem buscou) — {lab}")
    ax.set_xlabel("% com ALGUM SINTOMA (UF)")
    ax.set_ylabel("% INTERNOU entre quem BUSCOU (UF)")
    ax.set_xlim(0, nice_axis_limit(g["pct_sintomas"]))
    ax.set_ylim(0, nice_axis_limit(g["pct_internou_busca"]))
    ax.grid(True, alpha=.3)
    savefig(f"S2_disp_sintoma_vs_internou_{lab}"); plt.close()

print("\nPronto! Gráficos salvos na pasta ./figs")
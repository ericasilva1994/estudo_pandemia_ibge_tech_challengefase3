# ============================================================
# main.py — Loader PNAD COVID com Medalhão + COPY
# ============================================================
# Objetivo:
# 1) Ler os CSVs originais
# 2) Criar tabelas de staging (_new) no PostgreSQL
# 3) Inserir dados usando COPY (muito mais rápido que INSERT)
# 4) Promover staging -> oficial (swap) e manter backup (_old)
# ============================================================

import os
import sys
import time
import logging
from io import StringIO

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.types import Integer, BigInteger, Float, Text, Boolean, DateTime

# ============================================================
# CONFIGURA LOG — mostra no terminal o que está acontecendo
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ============================================================
# PARÂMETROS GERAIS
# ============================================================
CHUNK_SIZE = 200_000   # linhas por chunk ao carregar
TRY_DAYFIRST_DATES = True

CSV_JOBS = [
    ("data/PNAD_COVID_052020.csv", "pnad_covid_052020"),
    ("data/PNAD_COVID_082020.csv", "pnad_covid_082020"),
    ("data/PNAD_COVID_112020.csv", "pnad_covid_112020"),
]

# ============================================================
# CONEXÃO COM POSTGRES
# ============================================================
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "bd_relacional")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS")

if not all([DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS]):
    log.error("❌ Faltam variáveis no .env (DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASS).")
    sys.exit(1)

def make_engine(db_name: str):
    """Cria engine SQLAlchemy para conexão ao PostgreSQL."""
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=int(DB_PORT),
        database=db_name,
        query={"sslmode": "require", "application_name": "pnad_loader"},
    )
    return create_engine(url, pool_pre_ping=True, pool_recycle=1800)

# ============================================================
# FUNÇÕES AUXILIARES PARA LIMPEZA E TIPOS
# ============================================================
def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Padroniza nomes de colunas para snake_case e sem caracteres especiais."""
    new_cols = []
    for c in df.columns:
        c2 = str(c).strip().lower()
        c2 = c2.replace(" ", "_").replace("-", "_").replace("/", "_")
        c2 = "".join(ch for ch in c2 if ch.isalnum() or ch == "_")
        if not c2:
            c2 = "coluna_sem_nome"
        new_cols.append(c2)
    df.columns = new_cols
    return df

def smart_read_csv(path: str) -> pd.DataFrame:
    """Tenta ler CSV com utf-8, se falhar tenta latin1."""
    try:
        return pd.read_csv(path, sep=None, engine="python", encoding="utf-8")
    except Exception:
        return pd.read_csv(path, sep=None, engine="python", encoding="latin1")

def maybe_parse_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    """Converte colunas texto em datetime se fizer sentido (mais de 70% parseável)."""
    if not TRY_DAYFIRST_DATES:
        return df
    for col in df.columns:
        if df[col].dtype == object:
            try:
                parsed = pd.to_datetime(df[col], errors="coerce", dayfirst=True)
                if parsed.notna().mean() > 0.7:
                    df[col] = parsed
            except Exception:
                pass
    return df

def infer_sqlalchemy_dtypes(df: pd.DataFrame):
    """Define tipos para criação da tabela no Postgres."""
    dtype_map = {}
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_integer_dtype(s):
            try:
                max_abs = pd.to_numeric(s, errors="coerce").abs().max()
                dtype_map[col] = BigInteger() if (pd.notna(max_abs) and max_abs > 2**31-1) else Integer()
            except Exception:
                dtype_map[col] = Integer()
        elif pd.api.types.is_float_dtype(s):
            dtype_map[col] = Float(asdecimal=False)
        elif pd.api.types.is_bool_dtype(s):
            dtype_map[col] = Boolean()
        elif pd.api.types.is_datetime64_any_dtype(s):
            dtype_map[col] = DateTime()
        else:
            dtype_map[col] = Text()
    return dtype_map

# ============================================================
# CARGA USANDO COPY
# ============================================================
def copy_chunk(conn, df_chunk: pd.DataFrame, table_name: str):
    """
    Usa COPY FROM STDIN para inserir chunk de dados de forma rápida.
    - Converte o DataFrame para CSV em memória (StringIO)
    - Usa NULL '' para representar valores nulos
    """
    buf = StringIO()
    df_chunk.to_csv(buf, index=False, header=False, sep=",", na_rep="")
    buf.seek(0)

    cols = ",".join([f'"{c}"' for c in df_chunk.columns])
    sql = f'COPY public."{table_name}" ({cols}) FROM STDIN WITH (FORMAT CSV, DELIMITER \',\', NULL \'\')'

    raw = conn.connection
    with raw.cursor() as cur:
        cur.copy_expert(sql, buf)

def load_csv_into_table(engine, csv_path: str, final_table: str):
    """Fluxo de carga com staging + COPY + swap."""
    if not os.path.isfile(csv_path):
        log.error("CSV não encontrado: %s", csv_path)
        return False

    # 1) Lê CSV completo
    df = smart_read_csv(csv_path)
    df = clean_columns(df)
    df = maybe_parse_datetimes(df)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    staging = f"{final_table}_new"
    dtypes = infer_sqlalchemy_dtypes(df)

    # 2) Cria tabela staging vazia
    with engine.begin() as conn:
        df.iloc[0:0].to_sql(
            name=staging,
            con=conn,
            schema="public",
            if_exists="replace",
            index=False,
            dtype=dtypes
        )

    # 3) Insere em chunks com COPY
    inserted = 0
    t0 = time.time()
    with engine.begin() as conn:
        for start in range(0, len(df), CHUNK_SIZE):
            end = min(start + CHUNK_SIZE, len(df))
            chunk = df.iloc[start:end]
            copy_chunk(conn, chunk, staging)
            inserted += len(chunk)
            log.info("%s inseridas %d linhas", staging, inserted)

    log.info("Staging %s concluída (%d linhas em %.1fs).", staging, inserted, time.time()-t0)

    # 4) Swap atômico: staging -> final, final -> _old
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS public."{final_table}_old" CASCADE;'))
        conn.execute(text(f'ALTER TABLE IF EXISTS public."{final_table}" RENAME TO "{final_table}_old";'))
        conn.execute(text(f'ALTER TABLE public."{staging}" RENAME TO "{final_table}";'))

    log.info("Swap concluído: %s atualizado (backup em %s_old).", final_table, final_table)
    return True

# ============================================================
# MAIN
# ============================================================
def main():
    engine = make_engine(DB_NAME)

    # Executa carga para cada CSV
    for csv_path, table in CSV_JOBS:
        load_csv_into_table(engine, csv_path, table)

    engine.dispose()
    log.info("Pipeline concluído com sucesso.")

if __name__ == "__main__":
    main()

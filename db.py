import os
from datetime import datetime
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

DB_PATH = os.environ.get("FIN_DB_PATH", "financas.db")
ENGINE = create_engine(f"sqlite:///{DB_PATH}", future=True)

def _now_iso():
    return datetime.utcnow().isoformat(timespec="seconds")

def _invalidate_cache():
    # se cache ainda não existir, ignora
    for fn in [
        "fetch_transactions",
        "fetch_cashflow_adjustments",
        "fetch_debts",
        "get_savings_goal_v2",
        "fetch_savings_deposits_v2_with_amount",
    ]:
        try:
            globals()[fn].clear()
        except Exception:
            pass

def ping_db():
    try:
        with ENGINE.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, "ok"
    except Exception as e:
        return False, str(e)

def init_db():
    with ENGINE.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            description TEXT NOT NULL,
            type TEXT NOT NULL CHECK(type IN ('entrada','saida')),
            amount REAL NOT NULL,
            category TEXT NOT NULL,
            paid INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS cashflow_adjustments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT DEFAULT '',
            created_at TEXT NOT NULL
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS debts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            creditor TEXT NOT NULL,
            description TEXT NOT NULL,
            amount REAL NOT NULL,
            due_date TEXT,
            priority INTEGER NOT NULL DEFAULT 1,
            paid INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL
        );
        """))

        # Desafio v2
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS savings_goal_v2 (
            id INTEGER PRIMARY KEY CHECK(id = 1),
            target_amount REAL,
            due_date TEXT,
            n_deposits INTEGER
        );
        """))
        conn.execute(text("""
        INSERT OR IGNORE INTO savings_goal_v2 (id, target_amount, due_date, n_deposits)
        VALUES (1, NULL, NULL, NULL);
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS savings_deposits_v2 (
            n INTEGER PRIMARY KEY,
            done INTEGER NOT NULL DEFAULT 0
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS savings_overrides_v2 (
            n INTEGER PRIMARY KEY,
            amount REAL
        );
        """))

        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS savings_tx_link_v2 (
            n INTEGER PRIMARY KEY,
            tx_id INTEGER
        );
        """))

# =========================
# TRANSACTIONS
# =========================
def add_transaction(date_, description, ttype, amount, category, paid):
    ttype = str(ttype).strip().lower()
    category = (str(category).strip() or "Outros")

    with ENGINE.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO transactions (date, description, type, amount, category, paid, created_at)
            VALUES (:date, :description, :type, :amount, :category, :paid, :created_at)
            """),
            {
                "date": str(date_),
                "description": str(description).strip(),
                "type": ttype,
                "amount": float(amount),
                "category": category,
                "paid": int(paid),
                "created_at": _now_iso(),
            },
        )
    _invalidate_cache()

@st.cache_data(show_spinner=False, ttl=10)
def fetch_transactions(date_start=None, date_end=None):
    q = "SELECT id, date, description, type, amount, category, paid FROM transactions WHERE 1=1"
    params = {}
    if date_start:
        q += " AND date >= :ds"
        params["ds"] = str(date_start)
    if date_end:
        q += " AND date <= :de"
        params["de"] = str(date_end)
    q += " ORDER BY date DESC, id DESC"

    with ENGINE.connect() as conn:
        df = pd.read_sql(text(q), conn, params=params)

    if df.empty:
        return pd.DataFrame(columns=["id","date","description","type","amount","category","paid"])

    df["paid"] = pd.to_numeric(df["paid"], errors="coerce").fillna(0).astype(int)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["type"] = df["type"].astype(str).str.strip().str.lower()
    df["category"] = df["category"].astype(str).fillna("Outros")
    df["date"] = df["date"].astype(str)
    df["id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int)
    return df

def delete_transaction(tx_id):
    tx_id = int(tx_id)
    with ENGINE.begin() as conn:
        conn.execute(text("DELETE FROM savings_tx_link_v2 WHERE tx_id = :tx_id"), {"tx_id": tx_id})
        conn.execute(text("DELETE FROM transactions WHERE id = :id"), {"id": tx_id})
    _invalidate_cache()

def update_transactions_bulk(df_updates):
    if df_updates is None or df_updates.empty:
        return

    upd = df_updates.copy()
    upd["id"] = pd.to_numeric(upd["id"], errors="coerce").fillna(0).astype(int)
    upd["paid"] = upd["paid"].apply(lambda x: 1 if bool(x) else 0)

    with ENGINE.begin() as conn:
        for _, r in upd.iterrows():
            conn.execute(
                text("""
                UPDATE transactions
                   SET date=:date,
                       description=:description,
                       type=:type,
                       amount=:amount,
                       category=:category,
                       paid=:paid
                 WHERE id=:id
                """),
                {
                    "id": int(r["id"]),
                    "date": str(r.get("date","")),
                    "description": str(r.get("description","")).strip(),
                    "type": str(r.get("type","")).strip().lower(),
                    "amount": float(r.get("amount", 0.0)),
                    "category": (str(r.get("category","")).strip() or "Outros"),
                    "paid": int(r.get("paid", 0)),
                },
            )
    _invalidate_cache()

# =========================
# AJUSTES DO FLUXO
# =========================
def add_cashflow_adjustment(date_, amount, description=None):
    with ENGINE.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO cashflow_adjustments (date, amount, description, created_at)
            VALUES (:date, :amount, :description, :created_at)
            """),
            {
                "date": str(date_),
                "amount": float(amount),
                "description": (description or "").strip(),
                "created_at": _now_iso(),
            },
        )
    _invalidate_cache()

@st.cache_data(show_spinner=False, ttl=10)
def fetch_cashflow_adjustments(date_start, date_end):
    with ENGINE.connect() as conn:
        df = pd.read_sql(
            text("""
            SELECT id, date, amount, description
              FROM cashflow_adjustments
             WHERE date >= :ds AND date <= :de
             ORDER BY date ASC, id ASC
            """),
            conn,
            params={"ds": str(date_start), "de": str(date_end)},
        )
    if df.empty:
        return pd.DataFrame(columns=["id","date","amount","description"])
    df["id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["date"] = df["date"].astype(str)
    return df

def delete_cashflow_adjustment(adj_id):
    adj_id = int(adj_id)
    with ENGINE.begin() as conn:
        conn.execute(text("DELETE FROM cashflow_adjustments WHERE id = :id"), {"id": adj_id})
    _invalidate_cache()

# =========================
# DÍVIDAS
# =========================
def add_debt(credor, descricao, valor, vencimento, prioridade):
    with ENGINE.begin() as conn:
        conn.execute(
            text("""
            INSERT INTO debts (creditor, description, amount, due_date, priority, paid, created_at)
            VALUES (:credor, :descricao, :valor, :venc, :prio, 0, :created_at)
            """),
            {
                "credor": str(credor).strip(),
                "descricao": str(descricao or "").strip(),
                "valor": float(valor),
                "venc": None if not vencimento else str(vencimento),
                "prio": int(prioridade),
                "created_at": _now_iso(),
            },
        )
    _invalidate_cache()

@st.cache_data(show_spinner=False, ttl=10)
def fetch_debts(show_quitadas=False):
    q = """
    SELECT id, creditor, description, amount, due_date, priority, paid, created_at
      FROM debts
    """
    if not show_quitadas:
        q += " WHERE paid = 0"
    q += " ORDER BY priority ASC, COALESCE(due_date,'9999-12-31') ASC, id DESC"

    with ENGINE.connect() as conn:
        df = pd.read_sql(text(q), conn)

    if df.empty:
        return pd.DataFrame(columns=["id","creditor","description","amount","due_date","priority","paid","created_at"])

    df["id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0.0)
    df["priority"] = pd.to_numeric(df["priority"], errors="coerce").fillna(1).astype(int)
    df["paid"] = pd.to_numeric(df["paid"], errors="coerce").fillna(0).astype(int)
    return df

def mark_debt_paid(debt_id, paid):
    debt_id = int(debt_id)
    with ENGINE.begin() as conn:
        conn.execute(
            text("UPDATE debts SET paid = :paid WHERE id = :id"),
            {"paid": 1 if paid else 0, "id": debt_id},
        )
    _invalidate_cache()

def delete_debt(debt_id):
    debt_id = int(debt_id)
    with ENGINE.begin() as conn:
        conn.execute(text("DELETE FROM debts WHERE id = :id"), {"id": debt_id})
    _invalidate_cache()

# =========================
# DESAFIO v2
# =========================
def _min_n_for_target(target):
    target = float(target)
    if target <= 0:
        return 1
    n = int((((1 + 8 * target) ** 0.5) - 1) / 2)
    if n * (n + 1) / 2 < target:
        n += 1
    return max(1, n)

def set_savings_goal_v2(target_amount, due_date):
    target_amount = float(target_amount)
    n = _min_n_for_target(target_amount)

    with ENGINE.begin() as conn:
        conn.execute(
            text("UPDATE savings_goal_v2 SET target_amount=:t, due_date=:d, n_deposits=:n WHERE id=1"),
            {"t": target_amount, "d": due_date, "n": n},
        )

        existing = pd.read_sql(text("SELECT n, done FROM savings_deposits_v2"), conn)
        existing_map = {int(r["n"]): int(r["done"]) for _, r in existing.iterrows()} if not existing.empty else {}

        conn.execute(text("DELETE FROM savings_deposits_v2"))
        for i in range(1, n + 1):
            conn.execute(
                text("INSERT INTO savings_deposits_v2 (n, done) VALUES (:n, :done)"),
                {"n": i, "done": existing_map.get(i, 0)},
            )

        conn.execute(text("DELETE FROM savings_overrides_v2 WHERE n > :n"), {"n": n})
        conn.execute(text("DELETE FROM savings_tx_link_v2 WHERE n > :n"), {"n": n})

    _invalidate_cache()

@st.cache_data(show_spinner=False, ttl=10)
def get_savings_goal_v2():
    with ENGINE.connect() as conn:
        row = conn.execute(text("SELECT target_amount, due_date, n_deposits FROM savings_goal_v2 WHERE id=1")).fetchone()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]

@st.cache_data(show_spinner=False, ttl=10)
def fetch_savings_deposits_v2_with_amount():
    with ENGINE.connect() as conn:
        dep = pd.read_sql(text("SELECT n, done FROM savings_deposits_v2 ORDER BY n ASC"), conn)
        ov = pd.read_sql(text("SELECT n, amount FROM savings_overrides_v2"), conn)

    if dep.empty:
        return pd.DataFrame(columns=["n","done","amount"])

    dep["n"] = pd.to_numeric(dep["n"], errors="coerce").fillna(0).astype(int)
    dep["done"] = pd.to_numeric(dep["done"], errors="coerce").fillna(0).astype(int)

    if ov.empty:
        dep["amount"] = dep["n"].astype(float)
        return dep[["n","done","amount"]]

    ov["n"] = pd.to_numeric(ov["n"], errors="coerce").fillna(0).astype(int)
    ov["amount"] = pd.to_numeric(ov["amount"], errors="coerce").fillna(0.0)

    merged = dep.merge(ov, on="n", how="left")
    merged["amount"] = merged["amount"].fillna(merged["n"].astype(float))
    return merged[["n","done","amount"]].sort_values("n")

def toggle_savings_deposit_v2(n, done):
    n = int(n)
    with ENGINE.begin() as conn:
        conn.execute(
            text("UPDATE savings_deposits_v2 SET done = :done WHERE n = :n"),
            {"done": 1 if done else 0, "n": n},
        )
    _invalidate_cache()

def set_savings_override_v2(n, amount):
    n = int(n)
    with ENGINE.begin() as conn:
        if amount is None:
            conn.execute(text("DELETE FROM savings_overrides_v2 WHERE n = :n"), {"n": n})
        else:
            conn.execute(
                text("""
                INSERT INTO savings_overrides_v2 (n, amount)
                VALUES (:n, :amount)
                ON CONFLICT(n) DO UPDATE SET amount=excluded.amount
                """),
                {"n": n, "amount": float(amount)},
            )
    _invalidate_cache()

def reset_savings_marks_v2():
    with ENGINE.begin() as conn:
        conn.execute(text("UPDATE savings_deposits_v2 SET done = 0"))
        conn.execute(text("DELETE FROM savings_tx_link_v2"))
    _invalidate_cache()

def clear_savings_goal_v2():
    with ENGINE.begin() as conn:
        conn.execute(text("UPDATE savings_goal_v2 SET target_amount=NULL, due_date=NULL, n_deposits=NULL WHERE id=1"))
        conn.execute(text("DELETE FROM savings_deposits_v2"))
        conn.execute(text("DELETE FROM savings_overrides_v2"))
        conn.execute(text("DELETE FROM savings_tx_link_v2"))
    _invalidate_cache()

def create_desafio_transaction(date_, n, amount):
    n = int(n)
    with ENGINE.begin() as conn:
        row = conn.execute(text("SELECT tx_id FROM savings_tx_link_v2 WHERE n=:n"), {"n": n}).fetchone()
        if row and row[0]:
            return int(row[0])

        conn.execute(
            text("""
            INSERT INTO transactions (date, description, type, amount, category, paid, created_at)
            VALUES (:date, :desc, 'entrada', :amount, 'Desafio', 1, :created_at)
            """),
            {"date": str(date_), "desc": f"Desafio - Depósito #{n}", "amount": float(amount), "created_at": _now_iso()},
        )
        tx_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

        conn.execute(
            text("""
            INSERT INTO savings_tx_link_v2 (n, tx_id)
            VALUES (:n, :tx_id)
            ON CONFLICT(n) DO UPDATE SET tx_id=excluded.tx_id
            """),
            {"n": n, "tx_id": int(tx_id)},
        )

    _invalidate_cache()
    return int(tx_id)

def delete_desafio_transaction(n):
    n = int(n)
    with ENGINE.begin() as conn:
        row = conn.execute(text("SELECT tx_id FROM savings_tx_link_v2 WHERE n=:n"), {"n": n}).fetchone()
        if row and row[0]:
            conn.execute(text("DELETE FROM transactions WHERE id=:id"), {"id": int(row[0])})
        conn.execute(text("DELETE FROM savings_tx_link_v2 WHERE n=:n"), {"n": n})
    _invalidate_cache()

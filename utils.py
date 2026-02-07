import pandas as pd

def fmt_brl(v) -> str:
    try:
        v = float(v)
    except Exception:
        v = 0.0
    return f"R$ {v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def build_cashflow(
    df_tx: pd.DataFrame,
    start,
    end,
    only_paid: bool,
    df_adj: pd.DataFrame | None = None
) -> pd.DataFrame:
    if df_tx is None or df_tx.empty:
        df_tx = pd.DataFrame(columns=["date", "type", "amount", "paid"])

    df = df_tx.copy()

    if only_paid and "paid" in df.columns:
        df = df[df["paid"] == 1]

    df["date"] = pd.to_datetime(df.get("date", None), errors="coerce").dt.date
    df["type"] = df.get("type", "").astype(str).str.strip().str.lower()
    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0.0)

    days = pd.date_range(start=start, end=end, freq="D")
    out = pd.DataFrame({"data": days.date})

    if not df.empty:
        g_in = df[df["type"] == "entrada"].groupby("date")["amount"].sum()
        g_out = df[df["type"] == "saida"].groupby("date")["amount"].sum()
    else:
        g_in = pd.Series(dtype=float)
        g_out = pd.Series(dtype=float)

    out["entrada"] = out["data"].map(g_in).fillna(0.0)
    out["saida"] = out["data"].map(g_out).fillna(0.0)

    if df_adj is None or df_adj.empty:
        out["ajuste"] = 0.0
    else:
        a = df_adj.copy()
        a["date"] = pd.to_datetime(a.get("date", None), errors="coerce").dt.date
        a["amount"] = pd.to_numeric(a.get("amount", 0), errors="coerce").fillna(0.0)
        g_adj = a.groupby("date")["amount"].sum()
        out["ajuste"] = out["data"].map(g_adj).fillna(0.0)

    out["saldo_dia"] = out["entrada"] - out["saida"] - out["ajuste"]
    out["saldo_acumulado"] = out["saldo_dia"].cumsum()
    return out

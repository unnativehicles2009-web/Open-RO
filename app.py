import os
import re
import io
import threading
import webbrowser
from datetime import datetime, date

import pandas as pd
from flask import Flask, jsonify, request, send_file, Response

# =========================================================
# CONFIG (edit here)
# =========================================================
# Local only (ignored on Render)
HOST = os.environ.get("HOST", "127.0.0.1")
PORT = int(os.environ.get("PORT", "5000"))
AUTO_OPEN_BROWSER = os.environ.get("AUTO_OPEN_BROWSER", "true").lower() == "true"

# Data source:
# - For Render (Google Sheet): set GOOGLE_SHEET_CSV_URL env var to the published CSV URL
# - For local Excel: set OPEN_RO_XLSX and OPEN_RO_SHEET env vars (or keep defaults below)
GOOGLE_SHEET_CSV_URL = os.environ.get("GOOGLE_SHEET_CSV_URL", "").strip()

EXCEL_PATH = os.environ.get("OPEN_RO_XLSX", r"E:\Renault\Open RO.xlsx")
SHEET_NAME = os.environ.get("OPEN_RO_SHEET", "Details")

# Cache TTL for Google Sheet reload (seconds)
CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "120"))

# =========================================================
# HELPERS
# =========================================================
def parse_date_any(v):
    if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
        return pd.NaT
    if isinstance(v, (pd.Timestamp, datetime)):
        return pd.to_datetime(v, errors="coerce")
    s = str(v).strip()
    if s in ["", "-", "nan", "NaT", "None"]:
        return pd.NaT
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%b-%Y", "%d %b %Y"):
        try:
            return pd.to_datetime(s, format=fmt, errors="raise")
        except Exception:
            pass
    return pd.to_datetime(s, errors="coerce", dayfirst=True)


def parse_iso_yyyy_mm_dd(s):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return pd.to_datetime(s, format="%Y-%m-%d", errors="raise")
    except Exception:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        return None if pd.isna(dt) else dt


def clean_money_to_float(x):
    """
    Supports:
    - "Rs 1,234.50"
    - "₹1,234"
    - "1,234"
    - blanks / nan -> 0
    """
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return 0.0
    s = str(x).strip()
    if s in ["", "-", "nan", "NaT", "None"]:
        return 0.0
    s = re.sub(r"(?i)rs\.?", "", s)
    s = s.replace("₹", "")
    s = s.replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        # fallback: keep only digits and dot
        s2 = re.sub(r"[^0-9.]+", "", s)
        try:
            return float(s2) if s2 else 0.0
        except Exception:
            return 0.0


def age_bucket_from_days(days: int) -> str:
    if days <= 3:
        return "0-3 days"
    if days <= 10:
        return "4-10 days"
    if days <= 15:
        return "11-15 days"
    if days <= 30:
        return "16-30 days"
    if days <= 60:
        return "31-60 days"
    return "Above 60"


def safe_str(v, default="-"):
    if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
        return default
    s = str(v).strip()
    return default if s == "" else s


def fmt_ddmmyyyy(ts):
    if ts is None or (isinstance(ts, float) and pd.isna(ts)) or pd.isna(ts):
        return "-"
    try:
        d = pd.to_datetime(ts, errors="coerce")
        if pd.isna(d):
            return "-"
        return d.strftime("%d/%m/%Y")
    except Exception:
        return "-"


def to_int_safe(v, default=0):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v):
            return default
        return int(float(v))
    except Exception:
        return default


def pick_first_existing_column(df: pd.DataFrame, candidates):
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    lower_map = {str(c).strip().lower(): c for c in cols}
    for cand in candidates:
        key = str(cand).strip().lower()
        if key in lower_map:
            return lower_map[key]
    return None


def proper_case_name(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return ""
    parts = re.split(r"\s+", s)
    parts = [p[:1].upper() + p[1:].lower() if p else "" for p in parts]
    return " ".join([p for p in parts if p])


def normalize_multi_values(values):
    """
    Accepts a list like ["A,B", "C"] and returns ["A","B","C"] trimmed.
    Also supports repeated query params: ?branch=A&branch=B
    """
    out = []
    for v in values or []:
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        # split by comma if present
        parts = [p.strip() for p in s.split(",")]
        for p in parts:
            if p:
                out.append(p)
    # unique preserve order
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


# =========================================================
# LOAD + PREPARE DATA
# =========================================================
DF = pd.DataFrame()
MODEL_COL = None
LAST_LOAD_TS = None
LAST_SOURCE = None

REQUIRED_COLS = [
    "Dealer Code",
    "Repair Order #",
    "RO Open Date",
    "Vehicle Registration No",
    "VIN #",
    "Odometer Reading",
    "Assigned To Full Name",
    "Status",
    "SR Type",
    "Hold Reason",
    "Total RO Amount",
    "Total Parts Amount",
    "Total Labor Amount",
    "Owner Contact First Name",
    "Owner Contact Last Name",
]

MODEL_CANDIDATES = [
    "Model Name",
    "Model",
    "Model_Name",
    "MODEL NAME",
    "MODEL",
    "Model Group",
    "ModelGroup",
    "MODEL GROUP",
]


def _read_source_df() -> pd.DataFrame:
    if GOOGLE_SHEET_CSV_URL:
        # Google published CSV
        # NOTE: must be "pub?...&output=csv" (no login)
        df = pd.read_csv(GOOGLE_SHEET_CSV_URL, dtype=str, keep_default_na=False)
        return df
    # Local Excel
    if not os.path.exists(EXCEL_PATH):
        raise FileNotFoundError(f"Excel not found: {EXCEL_PATH}")
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, dtype=object)
    return df


def load_data(force=False):
    global DF, MODEL_COL, LAST_LOAD_TS, LAST_SOURCE

    now = pd.Timestamp.utcnow()

    if not force and GOOGLE_SHEET_CSV_URL and LAST_LOAD_TS is not None:
        age = (now - LAST_LOAD_TS).total_seconds()
        if age < CACHE_TTL_SECONDS and DF is not None and not DF.empty:
            return

    try:
        df = _read_source_df()
    except Exception as e:
        DF = pd.DataFrame()
        MODEL_COL = None
        LAST_LOAD_TS = now
        LAST_SOURCE = "error"
        print(f"[ERROR] load failed: {e}")
        return

    # Ensure required columns exist
    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = None

    # Model column
    MODEL_COL = pick_first_existing_column(df, MODEL_CANDIDATES)
    if MODEL_COL is None:
        df["Model Name"] = None
        MODEL_COL = "Model Name"

    # Dates + Age buckets
    df["RO_DATE_DT"] = df["RO Open Date"].apply(parse_date_any)
    today = pd.Timestamp(date.today())
    df["DAYS_OPEN"] = (today - df["RO_DATE_DT"]).dt.days
    df["DAYS_OPEN"] = df["DAYS_OPEN"].fillna(0).astype(int)
    df.loc[df["DAYS_OPEN"] < 0, "DAYS_OPEN"] = 0
    df["AGE_BUCKET"] = df["DAYS_OPEN"].apply(age_bucket_from_days)

    # Hold reason (blank => No reason)
    df["HOLD_REASON_CLEAN"] = df["Hold Reason"].apply(lambda x: safe_str(x, "")).astype(str).str.strip()
    df.loc[df["HOLD_REASON_CLEAN"] == "", "HOLD_REASON_CLEAN"] = "No reason"

    # Model name (blank => Unknown)
    df["MODEL_NAME_CLEAN"] = df[MODEL_COL].apply(lambda x: safe_str(x, "")).astype(str).str.strip()
    df.loc[df["MODEL_NAME_CLEAN"] == "", "MODEL_NAME_CLEAN"] = "Unknown"

    # Customer Name (Proper Case)
    fn = df["Owner Contact First Name"].apply(lambda x: safe_str(x, "")).astype(str)
    ln = df["Owner Contact Last Name"].apply(lambda x: safe_str(x, "")).astype(str)
    df["CUSTOMER_NAME"] = (fn.str.strip() + " " + ln.str.strip()).str.strip()
    df["CUSTOMER_NAME"] = df["CUSTOMER_NAME"].apply(proper_case_name)
    df.loc[df["CUSTOMER_NAME"] == "", "CUSTOMER_NAME"] = "Unknown"

    # Money columns
    df["RO_AMOUNT_NUM"] = df["Total RO Amount"].apply(clean_money_to_float)
    df["PARTS_AMOUNT_NUM"] = df["Total Parts Amount"].apply(clean_money_to_float)
    df["LABOR_AMOUNT_NUM"] = df["Total Labor Amount"].apply(clean_money_to_float)

    # Sort by RO date descending
    df = df.sort_values("RO_DATE_DT", ascending=False, na_position="last").reset_index(drop=True)

    DF = df
    LAST_LOAD_TS = now
    LAST_SOURCE = "google_csv" if GOOGLE_SHEET_CSV_URL else "excel"
    print(f"[OK] Loaded rows: {len(DF)} | model_col: {MODEL_COL} | source: {LAST_SOURCE}")


load_data(force=True)

# =========================================================
# FILTERING
# =========================================================
def _get_multi_param(name: str):
    """
    Supports:
    - ?branch=A&branch=B
    - ?branch=A,B
    - missing => []
    """
    raw = request.args.getlist(name)  # list
    vals = normalize_multi_values(raw)
    # remove All
    vals = [v for v in vals if v and v != "All"]
    return vals


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    branches = _get_multi_param("branch")
    statuses = _get_multi_param("status")
    age_buckets = _get_multi_param("age_bucket")
    sr_types = _get_multi_param("sr_type")
    hold_reasons = _get_multi_param("hold_reason")
    model_names = _get_multi_param("model_name")

    reg_search = (request.args.get("reg_search", "") or "").strip()
    from_date = (request.args.get("from_date", "") or "").strip()
    to_date = (request.args.get("to_date", "") or "").strip()

    if branches:
        out = out[out["Dealer Code"].astype(str).isin([str(x) for x in branches])]
    if statuses:
        out = out[out["Status"].astype(str).isin([str(x) for x in statuses])]
    if age_buckets:
        out = out[out["AGE_BUCKET"].astype(str).isin([str(x) for x in age_buckets])]
    if sr_types:
        out = out[out["SR Type"].astype(str).isin([str(x) for x in sr_types])]
    if hold_reasons:
        out = out[out["HOLD_REASON_CLEAN"].astype(str).isin([str(x) for x in hold_reasons])]
    if model_names:
        out = out[out["MODEL_NAME_CLEAN"].astype(str).isin([str(x) for x in model_names])]

    if reg_search:
        key = reg_search.upper()
        out = out[out["Vehicle Registration No"].astype(str).str.upper().str.contains(key, na=False)]

    if "RO_DATE_DT" not in out.columns:
        out["RO_DATE_DT"] = out["RO Open Date"].apply(parse_date_any)

    fd = parse_iso_yyyy_mm_dd(from_date) if from_date else None
    td = parse_iso_yyyy_mm_dd(to_date) if to_date else None
    if fd is not None:
        out = out[out["RO_DATE_DT"] >= fd]
    if td is not None:
        td_end = td + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        out = out[out["RO_DATE_DT"] <= td_end]

    return out


def json_row(r) -> dict:
    return {
        "ro_id": safe_str(r.get("Repair Order #")),
        "ro_date": fmt_ddmmyyyy(r.get("RO_DATE_DT")),
        "branch": safe_str(r.get("Dealer Code")),
        "status": safe_str(r.get("Status")),
        "sr_type": safe_str(r.get("SR Type")),
        "hold_reason": safe_str(r.get("HOLD_REASON_CLEAN")),
        "model_name": safe_str(r.get("MODEL_NAME_CLEAN")),
        "customer_name": safe_str(r.get("CUSTOMER_NAME")),
        "sa_name": safe_str(r.get("Assigned To Full Name")),
        "reg_number": safe_str(r.get("Vehicle Registration No")),
        "km": to_int_safe(r.get("Odometer Reading"), 0),
        "age_bucket": safe_str(r.get("AGE_BUCKET")),
        "days": to_int_safe(r.get("DAYS_OPEN"), 0),
        "total_ro_amount": float(r.get("RO_AMOUNT_NUM", 0.0) or 0.0),
        "total_parts_amount": float(r.get("PARTS_AMOUNT_NUM", 0.0) or 0.0),
        "total_labor_amount": float(r.get("LABOR_AMOUNT_NUM", 0.0) or 0.0),
    }


# =========================================================
# FLASK APP
# =========================================================
app = Flask(__name__)

@app.after_request
def add_cors_headers(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    return resp


@app.route("/health")
def health():
    load_data()
    return jsonify({
        "status": "ok",
        "rows": int(len(DF)) if DF is not None else 0,
        "source": LAST_SOURCE,
        "last_load_utc": str(LAST_LOAD_TS) if LAST_LOAD_TS is not None else None,
    })


@app.route("/api/reload")
def api_reload():
    load_data(force=True)
    return jsonify({"ok": True, "rows": int(len(DF)), "model_col": MODEL_COL, "source": LAST_SOURCE})


@app.route("/api/filter-options")
def filter_options():
    load_data()
    if DF is None or DF.empty:
        return jsonify({
            "branches": ["All"],
            "statuses": ["All"],
            "age_buckets": ["All"],
            "sr_types": ["All"],
            "hold_reasons": ["All"],
            "model_names": ["All"],
        })

    branches = ["All"] + sorted([safe_str(x) for x in DF["Dealer Code"].dropna().unique().tolist()])
    statuses = ["All"] + sorted([safe_str(x) for x in DF["Status"].dropna().unique().tolist()])
    sr_types = ["All"] + sorted([safe_str(x) for x in DF["SR Type"].dropna().unique().tolist()])
    hold_reasons = ["All"] + sorted([safe_str(x) for x in DF["HOLD_REASON_CLEAN"].dropna().unique().tolist()])
    model_names = ["All"] + sorted([safe_str(x) for x in DF["MODEL_NAME_CLEAN"].dropna().unique().tolist()])

    age_order = ["0-3 days", "4-10 days", "11-15 days", "16-30 days", "31-60 days", "Above 60"]
    present = [x for x in age_order if x in set(DF["AGE_BUCKET"].astype(str).unique())]
    age_buckets = ["All"] + present

    return jsonify({
        "branches": branches,
        "statuses": statuses,
        "age_buckets": age_buckets,
        "sr_types": sr_types,
        "hold_reasons": hold_reasons,
        "model_names": model_names,
    })


@app.route("/api/stats")
def stats():
    load_data()
    if DF is None or DF.empty:
        return jsonify({
            "total_ros": 0,
            "total_ro_amount": 0.0,
            "total_parts_amount": 0.0,
            "total_labor_amount": 0.0,
        })

    filtered = apply_filters(DF)
    return jsonify({
        "total_ros": int(len(filtered)),
        "total_ro_amount": float(filtered["RO_AMOUNT_NUM"].sum()) if "RO_AMOUNT_NUM" in filtered.columns else 0.0,
        "total_parts_amount": float(filtered["PARTS_AMOUNT_NUM"].sum()) if "PARTS_AMOUNT_NUM" in filtered.columns else 0.0,
        "total_labor_amount": float(filtered["LABOR_AMOUNT_NUM"].sum()) if "LABOR_AMOUNT_NUM" in filtered.columns else 0.0,
    })


@app.route("/api/rows")
def rows():
    load_data()
    if DF is None or DF.empty:
        return jsonify({"total_count": 0, "filtered_count": 0, "rows": []})

    limit = int(request.args.get("limit", "50"))
    skip = int(request.args.get("skip", "0"))

    filtered = apply_filters(DF)
    total_count = int(len(DF))
    filtered_count = int(len(filtered))

    page = filtered.iloc[skip: skip + limit] if limit > 0 else filtered
    out = [json_row(r) for _, r in page.iterrows()]

    return jsonify({
        "total_count": total_count,
        "filtered_count": filtered_count,
        "rows": out,
    })


@app.route("/api/export")
def export_excel():
    load_data()
    if DF is None or DF.empty:
        return jsonify({"error": "No data"})

    filtered = apply_filters(DF).copy()
    if filtered.empty:
        return jsonify({"error": "No data for filters"})

    export_df = pd.DataFrame([json_row(r) for _, r in filtered.iterrows()])

    export_df = export_df.rename(columns={
        "ro_id": "RO ID",
        "ro_date": "RO Date",
        "branch": "Branch",
        "status": "Status",
        "sr_type": "SR Type",
        "hold_reason": "Hold Reason",
        "model_name": "Model Name",
        "customer_name": "Customer Name",
        "sa_name": "SA Name",
        "reg_number": "Reg Number",
        "km": "KM",
        "age_bucket": "Age Bucket",
        "days": "Days",
        "total_ro_amount": "Total RO Amount",
        "total_parts_amount": "Total Parts Amount",
        "total_labor_amount": "Total Labor Amount",
    })

    desired_order = [
        "RO ID", "RO Date", "Branch", "Status", "SR Type", "Hold Reason",
        "SA Name", "Reg Number", "Customer Name", "Model Name", "KM",
        "Age Bucket", "Days", "Total RO Amount", "Total Parts Amount", "Total Labor Amount"
    ]
    existing = [c for c in desired_order if c in export_df.columns]
    remaining = [c for c in export_df.columns if c not in existing]
    export_df = export_df[existing + remaining]

    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="Open_RO")
    bio.seek(0)

    filename = f"Open_RO_Export_{date.today().isoformat()}.xlsx"
    return send_file(
        bio,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# =========================================================
# FRONTEND (embedded HTML)
# =========================================================
HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Unnati Vehicles Open RO Dashboard</title>
<style>
    * { margin:0; padding:0; box-sizing:border-box; }
    body{
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        min-height: 100vh;
        padding: 18px;
    }
    .container{ max-width: 1400px; margin: 0 auto; }
    header{
        background:#fff;
        padding:18px 18px;
        border-radius:12px;
        margin-bottom:16px;
        box-shadow:0 10px 30px rgba(0,0,0,0.1);
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap:10px;
        flex-wrap:wrap;
    }
    h1{ font-size:28px; color:#111; }
    .header-actions{ display:flex; gap:10px; align-items:center; }
    .btn{
        border:none;
        border-radius:10px;
        padding:10px 14px;
        font-weight:800;
        cursor:pointer;
        font-size:13px;
        transition: transform 0.15s ease;
    }
    .btn:active{ transform: scale(0.98); }
    .btn-clear{ background:#e74c3c; color:#fff; }
    .btn-clear:hover{ background:#c0392b; }
    .btn-theme{
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color:#fff;
        padding:10px 14px;
        box-shadow:0 4px 15px rgba(102,126,234,0.30);
    }

    .stats-grid{
        display:grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 14px;
        margin-bottom: 14px;
    }
    .card{
        background:#fff;
        border-radius:12px;
        padding:16px;
        box-shadow:0 5px 15px rgba(0,0,0,0.10);
        text-align:center;
    }
    .card .label{ font-size:11px; letter-spacing:0.6px; color:#666; font-weight:900; text-transform:uppercase; }
    .card .value{ margin-top:10px; font-size:28px; color:#667eea; font-weight:900; }
    .card.grad{
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color:#fff;
    }
    .card.grad .label{ color: rgba(255,255,255,0.85); }
    .card.grad .value{ color:#fff; font-size:24px; }

    .filters{
        background:#fff;
        border-radius:12px;
        padding:14px;
        box-shadow:0 5px 15px rgba(0,0,0,0.10);
        margin-bottom: 14px;
    }
    .filters-grid{
        display:grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap:12px;
    }
    label{ display:block; font-size:12px; font-weight:900; color:#111; margin-bottom:6px; }

    input{
        width:100%;
        padding:10px;
        border-radius:10px;
        border:1px solid #ddd;
        font-size:13px;
        outline:none;
    }

    .table-wrap{
        background:#fff;
        border-radius:12px;
        box-shadow:0 5px 15px rgba(0,0,0,0.10);
        overflow:hidden;
    }
    .table-header{
        display:flex;
        align-items:center;
        justify-content:space-between;
        padding:12px 14px;
        background:#f7f7f7;
        border-bottom:1px solid #e7e7e7;
        gap:10px;
        flex-wrap:wrap;
    }
    .info{ font-size:12px; color:#444; font-weight:800; }
    .btn-export{ background:#27ae60; color:#fff; }
    .btn-export:hover{ background:#229954; }

    .scroll{
        overflow:auto;
        max-height: 560px;
    }
    table{
        width:100%;
        border-collapse:collapse;
        min-width: 1500px;
    }
    thead th{
        position:sticky;
        top:0;
        background:#fff;
        z-index:10;
        border-bottom:2px solid #eee;
        padding:12px 10px;
        font-size:11px;
        text-transform:uppercase;
        letter-spacing:0.5px;
        text-align:left;
    }
    tbody td{
        border-bottom:1px solid #f0f0f0;
        padding:12px 10px;
        font-size:12px;
        vertical-align:top;
    }
    tbody tr:hover{ background:#fafafa; }
    .badge{
        padding:6px 10px;
        border-radius:999px;
        font-weight:900;
        font-size:11px;
        display:inline-block;
    }
    .badge-green{ background:#dff4df; color:#0b7a28; }
    .badge-amber{ background:#fff0d9; color:#b85d00; }
    .ro-id{ font-weight:900; }
    .reg{ font-weight:900; }
    .money{ font-weight:900; }
    .muted{ color:#666; }

    /* Dark theme */
    body.dark{
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        color:#e0e0e0;
    }
    body.dark header,
    body.dark .card,
    body.dark .filters,
    body.dark .table-wrap{
        background:#2d3561;
        color:#e0e0e0;
        box-shadow:0 5px 15px rgba(0,0,0,0.30);
    }
    body.dark h1{ color:#e0e0e0; }
    body.dark .table-header{ background:#3a4575; border-bottom-color:#4a5585; }
    body.dark input{
        background:#3a4575;
        color:#e0e0e0;
        border-color:#4a5585;
    }
    body.dark thead th{ background:#2d3561; color:#e0e0e0; border-bottom-color:#3a4575; }
    body.dark tbody td{ border-bottom-color:#3a4575; color:#e0e0e0; }
    body.dark tbody tr:hover{ background:#3a4575; }

    /* Multi-select dropdown (checkbox) */
    .ms{
        position:relative;
        width:100%;
    }
    .ms-btn{
        width:100%;
        text-align:left;
        padding:10px;
        border-radius:10px;
        border:1px solid #ddd;
        background:#fff;
        font-size:13px;
        font-weight:700;
        cursor:pointer;
        display:flex;
        align-items:center;
        justify-content:space-between;
        gap:10px;
    }
    .ms-btn span{
        overflow:hidden;
        text-overflow:ellipsis;
        white-space:nowrap;
    }
    .ms-panel{
        position:absolute;
        left:0;
        right:0;
        top: calc(100% + 6px);
        background:#fff;
        border:1px solid #ddd;
        border-radius:12px;
        box-shadow:0 10px 30px rgba(0,0,0,0.12);
        padding:10px;
        z-index:50;
        display:none;
        max-height:260px;
        overflow:auto;
    }
    .ms.open .ms-panel{ display:block; }
    .ms-search{
        width:100%;
        padding:10px;
        border-radius:10px;
        border:1px solid #e0e0e0;
        outline:none;
        font-size:13px;
        margin-bottom:8px;
    }
    .ms-actions{
        display:flex;
        gap:8px;
        margin-bottom:8px;
    }
    .ms-actions button{
        border:none;
        border-radius:10px;
        padding:8px 10px;
        font-size:12px;
        font-weight:800;
        cursor:pointer;
        background:#f3f3f3;
    }
    .ms-item{
        display:flex;
        align-items:center;
        gap:10px;
        padding:6px 6px;
        border-radius:8px;
        cursor:pointer;
        user-select:none;
    }
    .ms-item:hover{ background:#f7f7f7; }
    .ms-item input{ width:auto; }
    .ms-item .txt{ font-size:13px; font-weight:650; color:#111; }
    body.dark .ms-btn{ background:#3a4575; border-color:#4a5585; color:#e0e0e0; }
    body.dark .ms-panel{ background:#2d3561; border-color:#4a5585; }
    body.dark .ms-search{ background:#3a4575; border-color:#4a5585; color:#e0e0e0; }
    body.dark .ms-actions button{ background:#3a4575; color:#e0e0e0; }
    body.dark .ms-item:hover{ background:#3a4575; }
    body.dark .ms-item .txt{ color:#e0e0e0; }
</style>
</head>
<body>
<div class="container">
    <header>
        <h1>Unnati Vehicles Open RO Dashboard</h1>
        <div class="header-actions">
            <button class="btn btn-theme" id="themeBtn" title="Toggle Theme">Theme</button>
            <button class="btn btn-clear" id="clearBtn">Clear All</button>
        </div>
    </header>

    <div class="stats-grid">
        <div class="card">
            <div class="label">Total ROs</div>
            <div class="value" id="kpi_total_ros">0</div>
        </div>
        <div class="card grad">
            <div class="label">Total RO Amount</div>
            <div class="value" id="kpi_ro_amt">₹0.00</div>
        </div>
        <div class="card grad">
            <div class="label">Total Parts Amount</div>
            <div class="value" id="kpi_parts_amt">₹0.00</div>
        </div>
        <div class="card grad">
            <div class="label">Total Labor Amount</div>
            <div class="value" id="kpi_labor_amt">₹0.00</div>
        </div>
    </div>

    <div class="filters">
        <div class="filters-grid">
            <div>
                <label>Branch</label>
                <div class="ms" id="ms_branch"></div>
            </div>
            <div>
                <label>RO Status</label>
                <div class="ms" id="ms_status"></div>
            </div>
            <div>
                <label>Age Bucket</label>
                <div class="ms" id="ms_age_bucket"></div>
            </div>
            <div>
                <label>SR Type</label>
                <div class="ms" id="ms_sr_type"></div>
            </div>
            <div>
                <label>Hold Reason</label>
                <div class="ms" id="ms_hold_reason"></div>
            </div>
            <div>
                <label>Model Name</label>
                <div class="ms" id="ms_model_name"></div>
            </div>

            <div>
                <label>From Date</label>
                <input type="date" id="from_date"/>
            </div>
            <div>
                <label>To Date</label>
                <input type="date" id="to_date"/>
            </div>
            <div>
                <label>Reg. Number (Search)</label>
                <input type="text" id="reg_search" placeholder="Enter registration number..."/>
            </div>
            <div>
                <label>Records</label>
                <select id="limit" style="width:100%; padding:10px; border-radius:10px; border:1px solid #ddd; font-size:13px; outline:none;">
                    <option value="10">10 Records</option>
                    <option value="20">20 Records</option>
                    <option value="50" selected>50 Records</option>
                    <option value="100">100 Records</option>
                    <option value="500">500 Records</option>
                </select>
            </div>
        </div>
    </div>

    <div class="table-wrap">
        <div class="table-header">
            <div class="info" id="tableInfo">Loading...</div>
            <button class="btn btn-export" id="exportBtn">Export Filtered Data to Excel</button>
        </div>
        <div class="scroll">
            <table>
                <thead>
                    <tr>
                        <th>RO ID</th>
                        <th>RO Date</th>
                        <th>Branch</th>
                        <th>Status</th>
                        <th>SR Type</th>
                        <th>Hold Reason</th>
                        <th>SA Name</th>
                        <th>Reg Number</th>
                        <th>Model Name</th>
                        <th>Customer Name</th>
                        <th>KM</th>
                        <th>Age Bucket</th>
                        <th>Days</th>
                        <th>Total RO Amount</th>
                        <th>Total Parts Amount</th>
                        <th>Total Labor Amount</th>
                    </tr>
                </thead>
                <tbody id="tbody">
                    <tr><td colspan="16" class="muted">Loading...</td></tr>
                </tbody>
            </table>
        </div>
    </div>
</div>

<script>
const API = window.location.origin;

function inr(x){
    const n = Number(x || 0);
    if (isNaN(n)) return "₹0.00";
    return "₹" + n.toLocaleString("en-IN", {minimumFractionDigits:2, maximumFractionDigits:2});
}
function badgeClass(status){
    const s = String(status || "").toLowerCase();
    if (s.includes("approved") || s.includes("ready")) return "badge badge-green";
    if (s.includes("hold") || s.includes("await") || s.includes("progress")) return "badge badge-amber";
    return "badge badge-green";
}

/* ===========================
   Multi-select widget
   =========================== */
function createMultiSelect(containerId, labelAllText){
    const root = document.getElementById(containerId);
    root.innerHTML = `
      <button type="button" class="ms-btn"><span class="ms-title">${labelAllText || "All"}</span><span class="ms-caret">▾</span></button>
      <div class="ms-panel">
        <input class="ms-search" type="text" placeholder="Search..."/>
        <div class="ms-actions">
          <button type="button" data-act="all">Select All</button>
          <button type="button" data-act="none">Clear</button>
        </div>
        <div class="ms-list"></div>
      </div>
    `;

    const btn = root.querySelector(".ms-btn");
    const panel = root.querySelector(".ms-panel");
    const list = root.querySelector(".ms-list");
    const search = root.querySelector(".ms-search");
    const title = root.querySelector(".ms-title");

    const state = {
        options: [],
        selected: new Set(),   // values excluding "All"
        allowAll: true,
        onChange: null
    };

    function updateTitle(){
        if (state.selected.size === 0){
            title.textContent = labelAllText || "All";
            return;
        }
        if (state.selected.size === 1){
            title.textContent = Array.from(state.selected)[0];
            return;
        }
        title.textContent = `${state.selected.size} Selected`;
    }

    function render(){
        const q = (search.value || "").trim().toLowerCase();
        list.innerHTML = "";

        // Render "All" checkbox row
        const allChecked = state.selected.size === 0;
        const allRow = document.createElement("div");
        allRow.className = "ms-item";
        allRow.innerHTML = `<input type="checkbox" ${allChecked ? "checked": ""} /> <div class="txt">${labelAllText || "All"}</div>`;
        allRow.addEventListener("click", (e) => {
            e.preventDefault();
            state.selected.clear();
            render();
            fireChange();
        });
        list.appendChild(allRow);

        // Render real options (exclude "All" in options list)
        const opts = state.options.filter(x => x !== "All");
        for (const v of opts){
            if (q && String(v).toLowerCase().indexOf(q) === -1) continue;
            const checked = state.selected.has(v);
            const row = document.createElement("div");
            row.className = "ms-item";
            row.innerHTML = `<input type="checkbox" ${checked ? "checked": ""} /> <div class="txt"></div>`;
            row.querySelector(".txt").textContent = v;
            row.addEventListener("click", (e) => {
                e.preventDefault();
                if (state.selected.has(v)) state.selected.delete(v);
                else state.selected.add(v);
                render();
                fireChange();
            });
            list.appendChild(row);
        }
        updateTitle();
    }

    function fireChange(){
        if (typeof state.onChange === "function") state.onChange(getSelectedValues());
    }

    function open(){
        root.classList.add("open");
        panel.style.display = "block";
        search.focus();
    }
    function close(){
        root.classList.remove("open");
        panel.style.display = "none";
        search.value = "";
        render();
    }

    btn.addEventListener("click", () => {
        if (root.classList.contains("open")) close();
        else open();
    });

    root.querySelectorAll(".ms-actions button").forEach(b => {
        b.addEventListener("click", (e) => {
            e.preventDefault();
            const act = b.getAttribute("data-act");
            if (act === "all"){
                state.selected = new Set(state.options.filter(x => x !== "All"));
            } else if (act === "none"){
                state.selected.clear();
            }
            render();
            fireChange();
        });
    });

    search.addEventListener("input", render);

    // click outside to close
    document.addEventListener("click", (e) => {
        if (!root.contains(e.target)) {
            if (root.classList.contains("open")) close();
        }
    });

    function setOptions(arr){
        state.options = (arr || ["All"]).slice();
        // If current selections contain values not present anymore, remove them
        const allowed = new Set(state.options.filter(x => x !== "All"));
        state.selected = new Set(Array.from(state.selected).filter(x => allowed.has(x)));
        render();
    }

    function setSelected(values){
        const vals = (values || []).filter(x => x && x !== "All");
        state.selected = new Set(vals);
        render();
    }

    function getSelectedValues(){
        return Array.from(state.selected);
    }

    function clear(){
        state.selected.clear();
        render();
    }

    function onChange(fn){
        state.onChange = fn;
    }

    // init
    panel.style.display = "none";
    setOptions(["All"]);

    return { setOptions, setSelected, getSelectedValues, clear, onChange };
}

/* ===========================
   App state / widgets
   =========================== */
const MS = {
    branch: createMultiSelect("ms_branch", "All"),
    status: createMultiSelect("ms_status", "All"),
    age_bucket: createMultiSelect("ms_age_bucket", "All"),
    sr_type: createMultiSelect("ms_sr_type", "All"),
    hold_reason: createMultiSelect("ms_hold_reason", "All"),
    model_name: createMultiSelect("ms_model_name", "All")
};

function getParams(){
    const p = new URLSearchParams();

    const addMulti = (key, widget) => {
        const vals = widget.getSelectedValues();
        if (vals && vals.length > 0){
            // send comma-separated (backend supports both)
            p.append(key, vals.join(","));
        }
    };

    addMulti("branch", MS.branch);
    addMulti("status", MS.status);
    addMulti("age_bucket", MS.age_bucket);
    addMulti("sr_type", MS.sr_type);
    addMulti("hold_reason", MS.hold_reason);
    addMulti("model_name", MS.model_name);

    const from_date = document.getElementById("from_date").value;
    const to_date = document.getElementById("to_date").value;
    const reg_search = document.getElementById("reg_search").value;

    if (from_date) p.append("from_date", from_date);
    if (to_date) p.append("to_date", to_date);
    if (reg_search && reg_search.trim() !== "") p.append("reg_search", reg_search.trim());

    return p;
}

async function loadFilterOptions(){
    const res = await fetch(`${API}/api/filter-options`);
    const data = await res.json();

    MS.branch.setOptions(data.branches || ["All"]);
    MS.status.setOptions(data.statuses || ["All"]);
    MS.age_bucket.setOptions(data.age_buckets || ["All"]);
    MS.sr_type.setOptions(data.sr_types || ["All"]);
    MS.hold_reason.setOptions(data.hold_reasons || ["All"]);
    MS.model_name.setOptions(data.model_names || ["All"]);
}

async function loadStats(){
    const p = getParams();
    const res = await fetch(`${API}/api/stats?${p.toString()}`);
    const s = await res.json();

    document.getElementById("kpi_total_ros").textContent = s.total_ros || 0;
    document.getElementById("kpi_ro_amt").textContent = inr(s.total_ro_amount || 0);
    document.getElementById("kpi_parts_amt").textContent = inr(s.total_parts_amount || 0);
    document.getElementById("kpi_labor_amt").textContent = inr(s.total_labor_amount || 0);
}

async function loadRows(){
    const limit = document.getElementById("limit").value;
    const p = getParams();
    p.append("skip", "0");
    p.append("limit", String(limit));

    const res = await fetch(`${API}/api/rows?${p.toString()}`);
    const data = await res.json();

    const rows = data.rows || [];
    document.getElementById("tableInfo").textContent =
        `Showing ${rows.length} of ${data.filtered_count} vehicles (Total: ${data.total_count})`;

    const tb = document.getElementById("tbody");
    tb.innerHTML = "";

    if (rows.length === 0){
        tb.innerHTML = `<tr><td colspan="16" class="muted">No data found</td></tr>`;
        return;
    }

    rows.forEach(r => {
        tb.innerHTML += `
        <tr>
            <td class="ro-id">${r.ro_id || "-"}</td>
            <td>${r.ro_date || "-"}</td>
            <td>${r.branch || "-"}</td>
            <td><span class="${badgeClass(r.status)}">${r.status || "-"}</span></td>
            <td>${r.sr_type || "-"}</td>
            <td>${r.hold_reason || "-"}</td>
            <td>${r.sa_name || "-"}</td>
            <td class="reg">${r.reg_number || "-"}</td>
            <td>${r.model_name || "-"}</td>
            <td>${r.customer_name || "-"}</td>
            <td>${(r.km || 0).toLocaleString("en-IN")}</td>
            <td>${r.age_bucket || "-"}</td>
            <td>${r.days || 0}</td>
            <td class="money">${inr(r.total_ro_amount || 0)}</td>
            <td class="money">${inr(r.total_parts_amount || 0)}</td>
            <td class="money">${inr(r.total_labor_amount || 0)}</td>
        </tr>`;
    });
}

async function refreshAll(){
    await loadStats();
    await loadRows();
}

function clearAll(){
    MS.branch.clear();
    MS.status.clear();
    MS.age_bucket.clear();
    MS.sr_type.clear();
    MS.hold_reason.clear();
    MS.model_name.clear();

    document.getElementById("from_date").value = "";
    document.getElementById("to_date").value = "";
    document.getElementById("reg_search").value = "";
    document.getElementById("limit").value = "50";

    refreshAll();
}

function toggleTheme(){
    document.body.classList.toggle("dark");
    const isDark = document.body.classList.contains("dark");
    localStorage.setItem("uv_openro_theme", isDark ? "dark" : "light");
}
function initTheme(){
    const v = localStorage.getItem("uv_openro_theme");
    if (v === "dark"){
        document.body.classList.add("dark");
    }
}

function hookEvents(){
    // Multi-select change events
    Object.values(MS).forEach(w => w.onChange(() => refreshAll()));

    document.getElementById("from_date").addEventListener("change", refreshAll);
    document.getElementById("to_date").addEventListener("change", refreshAll);
    document.getElementById("limit").addEventListener("change", refreshAll);

    document.getElementById("reg_search").addEventListener("keyup", () => {
        window.clearTimeout(window.__t);
        window.__t = window.setTimeout(refreshAll, 250);
    });

    document.getElementById("clearBtn").addEventListener("click", clearAll);
    document.getElementById("themeBtn").addEventListener("click", toggleTheme);

    document.getElementById("exportBtn").addEventListener("click", async () => {
        const p = getParams();
        const url = `${API}/api/export?${p.toString()}`;
        window.location.href = url;
    });
}

(async function main(){
    initTheme();
    await loadFilterOptions();
    hookEvents();
    await refreshAll();
})();
</script>
</body>
</html>
"""

@app.route("/")
def home():
    return Response(HTML, mimetype="text/html")


# =========================================================
# MAIN
# =========================================================
def open_browser():
    try:
        webbrowser.open(f"http://{HOST}:{PORT}", new=2)
    except Exception:
        pass


if __name__ == "__main__":
    if AUTO_OPEN_BROWSER:
        threading.Timer(1.0, open_browser).start()
    app.run(host=HOST, port=PORT, debug=False)

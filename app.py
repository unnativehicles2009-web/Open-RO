import os
import re
import io
import threading
import webbrowser
from datetime import datetime, date

import pandas as pd
import requests
from flask import Flask, jsonify, request, send_file, Response

# =========================================================
# RENDER + CONFIG
# =========================================================
HOST = os.environ.get("HOST", "0.0.0.0")
PORT = int(os.environ.get("PORT", "5000"))

# Auto-open browser ONLY for local PC, not on Render
AUTO_OPEN_BROWSER = os.environ.get("AUTO_OPEN_BROWSER", "true").strip().lower() == "true"
IS_RENDER = os.environ.get("RENDER", "").strip().lower() == "true" or os.environ.get("RENDER_SERVICE_ID")

# Google Sheet Published CSV URL (your link)
GOOGLE_SHEET_CSV_URL = os.environ.get(
    "GOOGLE_SHEET_CSV_URL",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vS5ZtziwobOOI3q4nOCyd0bJoQk0IW7GtSeszy23yLveqRZHBZJajVw7BTFngJnREqS8xaIH93RzGOe/pub?gid=0&single=true&output=csv"
)

# Cache seconds (Render performance)
CACHE_SECONDS = int(os.environ.get("CACHE_SECONDS", "60"))  # recommended 30-120

# If you ever want local Excel fallback (optional)
EXCEL_PATH = os.environ.get("OPEN_RO_XLSX", r"E:\Renault\Open RO.xlsx")
SHEET_NAME = os.environ.get("OPEN_RO_SHEET", "Details")

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
    Handles:
    "Rs 12,345.00", "₹12,345", "12345", "-", blank, NaN
    """
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return 0.0
    s = str(x).strip()
    if s in ["", "-", "nan", "NaT", "None"]:
        return 0.0

    s = s.replace("₹", "")
    s = re.sub(r"(?i)rs\.?", "", s)
    s = s.replace(",", "").strip()

    # keep digits, minus, dot
    s = re.sub(r"[^0-9\.\-]", "", s)
    if s in ["", "-", ".", "-."]:
        return 0.0

    try:
        return float(s)
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

# =========================================================
# REQUIRED COLUMNS (your mapping)
# =========================================================
REQUIRED_COLS = [
    "Dealer Code",                # Branch
    "Repair Order #",             # RO ID
    "RO Open Date",               # RO Date base
    "Vehicle Registration No",    # Reg
    "VIN #",                      # VIN (we won't show in table, but keep if needed)
    "Odometer Reading",           # KM
    "Assigned To Full Name",      # SA
    "Status",                     # RO Status
    "SR Type",                    # Dropdown
    "Hold Reason",                # Dropdown (blank -> No reason)
    "Total RO Amount",            # KPI + table
    "Total Parts Amount",         # KPI + table
    "Total Labor Amount",         # KPI + table
    "Owner Contact First Name",   # Customer name part
    "Owner Contact Last Name",    # Customer name part
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

# =========================================================
# CACHE + LOADING FROM GOOGLE SHEET CSV
# =========================================================
CACHE = {
    "df": pd.DataFrame(),
    "model_col": None,
    "loaded_at": None,
    "error": None,
}

def _read_google_sheet_csv() -> pd.DataFrame:
    # Strong timeouts for Render stability
    r = requests.get(GOOGLE_SHEET_CSV_URL, timeout=(8, 20))
    r.raise_for_status()

    # Some sheets return UTF-8 with BOM
    text = r.content.decode("utf-8-sig", errors="replace")
    df = pd.read_csv(io.StringIO(text))

    # Trim column names
    df.columns = [str(c).strip() for c in df.columns]
    return df

def _read_local_excel() -> pd.DataFrame:
    if not os.path.exists(EXCEL_PATH):
        raise FileNotFoundError(f"Excel not found: {EXCEL_PATH}")
    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME)
    df.columns = [str(c).strip() for c in df.columns]
    return df

def load_data(force: bool = False):
    """
    Loads data into CACHE['df'].
    Uses Google Sheet CSV by default.
    """
    now = datetime.now()
    if (not force) and CACHE["loaded_at"] is not None:
        age = (now - CACHE["loaded_at"]).total_seconds()
        if age < CACHE_SECONDS and CACHE["df"] is not None and not CACHE["df"].empty:
            return

    try:
        df = _read_google_sheet_csv()

        # Ensure required columns exist
        for c in REQUIRED_COLS:
            if c not in df.columns:
                df[c] = None

        model_col = pick_first_existing_column(df, MODEL_CANDIDATES)
        if model_col is None:
            df["Model Name"] = None
            model_col = "Model Name"

        # Dates
        df["RO_DATE_DT"] = df["RO Open Date"].apply(parse_date_any)
        today = pd.Timestamp(date.today())
        df["DAYS_OPEN"] = (today - df["RO_DATE_DT"]).dt.days
        df["DAYS_OPEN"] = df["DAYS_OPEN"].fillna(0).astype(int)
        df.loc[df["DAYS_OPEN"] < 0, "DAYS_OPEN"] = 0
        df["AGE_BUCKET"] = df["DAYS_OPEN"].apply(age_bucket_from_days)

        # Hold reason: blank => "No reason"
        df["HOLD_REASON_CLEAN"] = df["Hold Reason"].apply(lambda x: safe_str(x, "")).astype(str).str.strip()
        df.loc[df["HOLD_REASON_CLEAN"] == "", "HOLD_REASON_CLEAN"] = "No reason"

        # Model name: blank => "Unknown"
        df["MODEL_NAME_CLEAN"] = df[model_col].apply(lambda x: safe_str(x, "")).astype(str).str.strip()
        df.loc[df["MODEL_NAME_CLEAN"] == "", "MODEL_NAME_CLEAN"] = "Unknown"

        # Customer name proper case
        fn = df["Owner Contact First Name"].apply(lambda x: safe_str(x, "")).astype(str)
        ln = df["Owner Contact Last Name"].apply(lambda x: safe_str(x, "")).astype(str)
        df["CUSTOMER_NAME"] = (fn.str.strip() + " " + ln.str.strip()).str.strip()
        df["CUSTOMER_NAME"] = df["CUSTOMER_NAME"].apply(proper_case_name)
        df.loc[df["CUSTOMER_NAME"] == "", "CUSTOMER_NAME"] = "Unknown"

        # Money
        df["RO_AMOUNT_NUM"] = df["Total RO Amount"].apply(clean_money_to_float)
        df["PARTS_AMOUNT_NUM"] = df["Total Parts Amount"].apply(clean_money_to_float)
        df["LABOR_AMOUNT_NUM"] = df["Total Labor Amount"].apply(clean_money_to_float)

        # Sort
        df = df.sort_values("RO_DATE_DT", ascending=False, na_position="last").reset_index(drop=True)

        CACHE["df"] = df
        CACHE["model_col"] = model_col
        CACHE["loaded_at"] = now
        CACHE["error"] = None
        print(f"[OK] Loaded rows: {len(df)} | source: GoogleSheet CSV | model_col: {model_col}")

    except Exception as e:
        CACHE["error"] = str(e)
        # If cache already has data, keep it
        if CACHE["df"] is None or CACHE["df"].empty:
            CACHE["df"] = pd.DataFrame()
        CACHE["loaded_at"] = now
        print(f"[ERROR] Load failed: {e}")

def get_df() -> pd.DataFrame:
    load_data(force=False)
    return CACHE["df"]

# =========================================================
# FILTERS
# =========================================================
def apply_filters(df: pd.DataFrame, args: dict) -> pd.DataFrame:
    out = df.copy()

    branch = args.get("branch", "All")
    status = args.get("status", "All")
    age_bucket = args.get("age_bucket", "All")
    sr_type = args.get("sr_type", "All")
    hold_reason = args.get("hold_reason", "All")
    model_name = args.get("model_name", "All")

    reg_search = (args.get("reg_search", "") or "").strip()
    from_date = (args.get("from_date", "") or "").strip()
    to_date = (args.get("to_date", "") or "").strip()

    if branch and branch != "All":
        out = out[out["Dealer Code"].astype(str) == str(branch)]
    if status and status != "All":
        out = out[out["Status"].astype(str) == str(status)]
    if age_bucket and age_bucket != "All":
        out = out[out["AGE_BUCKET"].astype(str) == str(age_bucket)]
    if sr_type and sr_type != "All":
        out = out[out["SR Type"].astype(str) == str(sr_type)]
    if hold_reason and hold_reason != "All":
        out = out[out["HOLD_REASON_CLEAN"].astype(str) == str(hold_reason)]
    if model_name and model_name != "All":
        out = out[out["MODEL_NAME_CLEAN"].astype(str) == str(model_name)]

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
    # no flask_cors needed
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    return resp

@app.route("/health")
def health():
    df = get_df()
    return jsonify({
        "status": "ok",
        "rows": int(len(df)) if df is not None else 0,
        "cache_loaded_at": CACHE["loaded_at"].isoformat() if CACHE["loaded_at"] else None,
        "cache_seconds": CACHE_SECONDS,
        "error": CACHE["error"],
    })

@app.route("/api/reload")
def api_reload():
    load_data(force=True)
    df = CACHE["df"]
    return jsonify({
        "ok": True,
        "rows": int(len(df)) if df is not None else 0,
        "model_col": CACHE["model_col"],
        "error": CACHE["error"],
    })

@app.route("/api/filter-options")
def filter_options():
    df = get_df()
    if df is None or df.empty:
        return jsonify({
            "branches": ["All"],
            "statuses": ["All"],
            "age_buckets": ["All"],
            "sr_types": ["All"],
            "hold_reasons": ["All"],
            "model_names": ["All"],
        })

    branches = ["All"] + sorted([safe_str(x) for x in df["Dealer Code"].dropna().unique().tolist()])
    statuses = ["All"] + sorted([safe_str(x) for x in df["Status"].dropna().unique().tolist()])
    sr_types = ["All"] + sorted([safe_str(x) for x in df["SR Type"].dropna().unique().tolist()])
    hold_reasons = ["All"] + sorted([safe_str(x) for x in df["HOLD_REASON_CLEAN"].dropna().unique().tolist()])
    model_names = ["All"] + sorted([safe_str(x) for x in df["MODEL_NAME_CLEAN"].dropna().unique().tolist()])

    age_order = ["0-3 days", "4-10 days", "11-15 days", "16-30 days", "31-60 days", "Above 60"]
    present = [x for x in age_order if x in set(df["AGE_BUCKET"].astype(str).unique())]
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
    df = get_df()
    if df is None or df.empty:
        return jsonify({
            "total_ros": 0,
            "total_ro_amount": 0.0,
            "total_parts_amount": 0.0,
            "total_labor_amount": 0.0,
        })

    filtered = apply_filters(df, request.args)
    return jsonify({
        "total_ros": int(len(filtered)),
        "total_ro_amount": float(filtered["RO_AMOUNT_NUM"].sum()) if "RO_AMOUNT_NUM" in filtered.columns else 0.0,
        "total_parts_amount": float(filtered["PARTS_AMOUNT_NUM"].sum()) if "PARTS_AMOUNT_NUM" in filtered.columns else 0.0,
        "total_labor_amount": float(filtered["LABOR_AMOUNT_NUM"].sum()) if "LABOR_AMOUNT_NUM" in filtered.columns else 0.0,
    })

@app.route("/api/rows")
def rows():
    df = get_df()
    if df is None or df.empty:
        return jsonify({"total_count": 0, "filtered_count": 0, "rows": []})

    limit = int(request.args.get("limit", "50"))
    skip = int(request.args.get("skip", "0"))

    filtered = apply_filters(df, request.args)
    total_count = int(len(df))
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
    df = get_df()
    if df is None or df.empty:
        return jsonify({"error": "No data"})

    filtered = apply_filters(df, request.args).copy()
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

    # Neat order (VIN removed, Model added)
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
        font-weight:700;
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
        width:46px; height:46px;
        display:flex; align-items:center; justify-content:center;
        font-size:18px;
        box-shadow:0 4px 15px rgba(102,126,234,0.30);
    }
    .btn-reload{ background:#2d3436; color:#fff; }
    .btn-reload:hover{ background:#111; }

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
    .card .label{ font-size:11px; letter-spacing:0.6px; color:#666; font-weight:800; text-transform:uppercase; }
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
    label{ display:block; font-size:12px; font-weight:800; color:#111; margin-bottom:6px; }
    select, input{
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
    .info{ font-size:12px; color:#444; font-weight:700; }
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
    .errorbar{
        margin-top:8px;
        font-size:12px;
        font-weight:700;
        color:#b00020;
    }

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
    body.dark select, body.dark input{
        background:#3a4575;
        color:#e0e0e0;
        border-color:#4a5585;
    }
    body.dark thead th{ background:#2d3561; color:#e0e0e0; border-bottom-color:#3a4575; }
    body.dark tbody td{ border-bottom-color:#3a4575; color:#e0e0e0; }
    body.dark tbody tr:hover{ background:#3a4575; }
</style>
</head>
<body>
<div class="container">
    <header>
        <div>
            <h1>Unnati Vehicles Open RO Dashboard</h1>
            <div class="errorbar" id="errbar" style="display:none;"></div>
        </div>
        <div class="header-actions">
            <button class="btn btn-reload" id="reloadBtn" title="Reload from Google Sheet">Reload</button>
            <button class="btn btn-theme" id="themeBtn" title="Toggle Theme">🌙</button>
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
                <select id="branch"></select>
            </div>
            <div>
                <label>RO Status</label>
                <select id="status"></select>
            </div>
            <div>
                <label>Age Bucket</label>
                <select id="age_bucket"></select>
            </div>
            <div>
                <label>SR Type</label>
                <select id="sr_type"></select>
            </div>
            <div>
                <label>Hold Reason</label>
                <select id="hold_reason"></select>
            </div>
            <div>
                <label>Model Name</label>
                <select id="model_name"></select>
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
                <select id="limit">
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
                        <th>Customer Name</th>
                        <th>Model Name</th>
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

function getParams(){
    const p = new URLSearchParams();
    const branch = document.getElementById("branch").value;
    const status = document.getElementById("status").value;
    const age_bucket = document.getElementById("age_bucket").value;
    const sr_type = document.getElementById("sr_type").value;
    const hold_reason = document.getElementById("hold_reason").value;
    const model_name = document.getElementById("model_name").value;
    const from_date = document.getElementById("from_date").value;
    const to_date = document.getElementById("to_date").value;
    const reg_search = document.getElementById("reg_search").value;

    if (branch && branch !== "All") p.append("branch", branch);
    if (status && status !== "All") p.append("status", status);
    if (age_bucket && age_bucket !== "All") p.append("age_bucket", age_bucket);
    if (sr_type && sr_type !== "All") p.append("sr_type", sr_type);
    if (hold_reason && hold_reason !== "All") p.append("hold_reason", hold_reason);
    if (model_name && model_name !== "All") p.append("model_name", model_name);
    if (from_date) p.append("from_date", from_date);
    if (to_date) p.append("to_date", to_date);
    if (reg_search && reg_search.trim() !== "") p.append("reg_search", reg_search.trim());

    return p;
}

function showErr(msg){
    const el = document.getElementById("errbar");
    if (!msg){
        el.style.display = "none";
        el.textContent = "";
        return;
    }
    el.style.display = "block";
    el.textContent = msg;
}

async function loadFilterOptions(){
    const res = await fetch(`${API}/api/filter-options`);
    const data = await res.json();

    const setOptions = (id, arr) => {
        const el = document.getElementById(id);
        el.innerHTML = "";
        (arr || ["All"]).forEach(v => {
            const op = document.createElement("option");
            op.value = v;
            op.textContent = v;
            el.appendChild(op);
        });
    };

    setOptions("branch", data.branches);
    setOptions("status", data.statuses);
    setOptions("age_bucket", data.age_buckets);
    setOptions("sr_type", data.sr_types);
    setOptions("hold_reason", data.hold_reasons);
    setOptions("model_name", data.model_names);
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
            <td>${r.customer_name || "-"}</td>
            <td>${r.model_name || "-"}</td>
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
    showErr("");
    const h = await fetch(`${API}/health`);
    const health = await h.json();
    if (health.error){
        showErr("Data load issue: " + health.error);
    }
    await loadStats();
    await loadRows();
}

function clearAll(){
    document.getElementById("branch").value = "All";
    document.getElementById("status").value = "All";
    document.getElementById("age_bucket").value = "All";
    document.getElementById("sr_type").value = "All";
    document.getElementById("hold_reason").value = "All";
    document.getElementById("model_name").value = "All";
    document.getElementById("from_date").value = "";
    document.getElementById("to_date").value = "";
    document.getElementById("reg_search").value = "";
    document.getElementById("limit").value = "50";
    refreshAll();
}

function toggleTheme(){
    document.body.classList.toggle("dark");
    const btn = document.getElementById("themeBtn");
    const isDark = document.body.classList.contains("dark");
    localStorage.setItem("uv_openro_theme", isDark ? "dark" : "light");
    btn.textContent = isDark ? "☀️" : "🌙";
}
function initTheme(){
    const v = localStorage.getItem("uv_openro_theme");
    if (v === "dark"){
        document.body.classList.add("dark");
        document.getElementById("themeBtn").textContent = "☀️";
    }
}

function hookEvents(){
    ["branch","status","age_bucket","sr_type","hold_reason","model_name","from_date","to_date","limit"].forEach(id => {
        document.getElementById(id).addEventListener("change", refreshAll);
    });
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

    document.getElementById("reloadBtn").addEventListener("click", async () => {
        showErr("");
        await fetch(`${API}/api/reload`);
        await loadFilterOptions();
        await refreshAll();
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
# LOCAL AUTO-OPEN
# =========================================================
def open_browser():
    try:
        webbrowser.open(f"http://127.0.0.1:{PORT}", new=2)
    except Exception:
        pass

if __name__ == "__main__":
    # local run only
    load_data(force=True)
    if AUTO_OPEN_BROWSER and (not IS_RENDER):
        threading.Timer(1.0, open_browser).start()
    app.run(host="127.0.0.1", port=PORT, debug=False)

import os
import re
import io
from datetime import datetime, date
from typing import Optional

import pandas as pd
import requests
from flask import Flask, jsonify, request, send_file, Response

# =========================================================
# CONFIG
# =========================================================
APP_TITLE = os.environ.get("APP_TITLE", "Unnati Vehicles Open RO Dashboard")

PORT = int(os.environ.get("PORT", "5000"))

GOOGLE_SHEET_CSV_URL = os.environ.get(
    "GOOGLE_SHEET_CSV_URL",
    "https://docs.google.com/spreadsheets/d/e/2PACX-1vS5ZtziwobOOI3q4nOCyd0bJoQk0IW7GtSeszy23yLveqRZHBZJajVw7BTFngJnREqS8xaIH93RzGOe/pub?gid=0&single=true&output=csv",
)

EXCEL_PATH = os.environ.get("OPEN_RO_XLSX", "")
SHEET_NAME = os.environ.get("OPEN_RO_SHEET", "Details")

CACHE_TTL_SECONDS = int(os.environ.get("CACHE_TTL_SECONDS", "120"))

# =========================================================
# BRANCH CODE → NAME MAPPING
# =========================================================
BRANCH_CODE_TO_NAME = {
    "AKJA": "AKOLA",
    "AUJA": "AURANGABAD",
    "AUJB": "AURANGABAD BP",
    "AVJA": "AMRAVATI",
    "BAKA": "BARAMATI",
    "CNLA": "CHANDRAPUR",
    "NAJB": "WADI",
    "NAJE": "KALAMNA",
    "NSKB": "NASHIK SATPUR",
    "PUMB": "CHINCHWAD PUNE",
    "PUME": "HADAPSAR",
}

def branch_display(code: str) -> str:
    """Return human-readable branch name for a dealer code, or the code itself if unknown."""
    c = (code or "").strip()
    return BRANCH_CODE_TO_NAME.get(c, c)

def branch_display_with_code(code: str) -> str:
    """Return 'NAME (CODE)' for display, or just code if unknown."""
    c = (code or "").strip()
    name = BRANCH_CODE_TO_NAME.get(c)
    if name:
        return f"{name} ({c})"
    return c

# =========================================================
# HELPERS
# =========================================================
def parse_date_any(v):
    if v is None:
        return pd.NaT
    if isinstance(v, (pd.Timestamp, datetime)):
        return pd.to_datetime(v, errors="coerce")
    try:
        if isinstance(v, float) and pd.isna(v):
            return pd.NaT
    except Exception:
        pass
    try:
        num = float(str(v).strip())
        if not pd.isna(num) and 20000 < num < 100000:
            return pd.Timestamp("1899-12-30") + pd.Timedelta(days=num)
    except Exception:
        pass
    s = str(v).strip()
    if s in ["", "-", "nan", "NaT", "None", "NaN"]:
        return pd.NaT

    import re as _re
    m = _re.match(r"^(\d{2}-\d{2}-\d{4}) (\d{1,2})\.(\d{2})$", s)
    if m:
        s = m.group(1) + " " + m.group(2) + ":" + m.group(3)

    for fmt in (
        "%d-%m-%Y %H:%M", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y",
        "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d/%m/%y",
        "%d-%m-%y", "%m/%d/%y", "%d-%b-%Y", "%d %b %Y",
        "%b %d, %Y", "%Y/%m/%d", "%d-%b-%y",
        "%m/%d/%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
    ):
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


def clean_money_to_float(x) -> float:
    if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
        return 0.0
    s = str(x).strip()
    if s in ["", "-", "nan", "NaT", "None"]:
        return 0.0
    s = re.sub(r"(?i)\b(rs\.?|inr)\b", "", s)
    s = s.replace("₹", "").replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        s2 = re.sub(r"[^0-9\.\-]", "", s)
        try:
            return float(s2) if s2 else 0.0
        except Exception:
            return 0.0


def clean_odometer_to_int(x) -> int:
    if x is None:
        return 0
    try:
        if isinstance(x, float) and pd.isna(x):
            return 0
        if pd.isna(x):
            return 0
    except Exception:
        pass
    s = str(x).strip()
    if s in ["", "-", "nan", "NaT", "None", "NaN"]:
        return 0
    s = re.sub(r"(?i)\s*kms?\s*$", "", s).strip()
    s = s.replace(",", "").strip()
    try:
        return int(float(s))
    except Exception:
        s2 = re.sub(r"[^0-9\.]", "", s)
        try:
            return int(float(s2)) if s2 else 0
        except Exception:
            return 0


def age_bucket_from_days(days: int) -> str:
    if days <= 3:   return "0-3 days"
    if days <= 10:  return "4-10 days"
    if days <= 15:  return "11-15 days"
    if days <= 30:  return "16-30 days"
    if days <= 60:  return "31-60 days"
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
    lower_map = {str(c).strip().lower(): c for c in df.columns}
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


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    return df


# =========================================================
# LOAD + PREPARE DATA (with caching)
# =========================================================
DF = pd.DataFrame()
MODEL_COL = None
_LAST_LOAD_TS: Optional[float] = None

REQUIRED_COLS = [
    "Dealer Code", "Repair Order #", "RO Open Date",
    "Vehicle Registration No", "VIN #", "Odometer Reading",
    "Assigned To Full Name", "Status", "SR Type",
    "RO Type",
    "Visit Type",
    "Hold Reason", "Total RO Amount", "Total Parts Amount",
    "Total Labor Amount", "Owner Contact First Name", "Owner Contact Last Name",
]

MODEL_CANDIDATES = [
    "Model Name", "Model", "Model_Name", "MODEL NAME",
    "MODEL", "Model Group", "ModelGroup", "MODEL GROUP",
]


def _load_from_google_csv(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return pd.read_csv(io.BytesIO(r.content))


def _load_from_excel(path: str, sheet: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet)


def load_data(force: bool = False):
    global DF, MODEL_COL, _LAST_LOAD_TS

    now_ts = datetime.utcnow().timestamp()
    if (not force) and _LAST_LOAD_TS and (now_ts - _LAST_LOAD_TS) < CACHE_TTL_SECONDS \
            and (DF is not None) and (not DF.empty):
        return

    df = None
    last_error = None

    if GOOGLE_SHEET_CSV_URL:
        try:
            df = _load_from_google_csv(GOOGLE_SHEET_CSV_URL)
        except Exception as e:
            last_error = f"Google CSV load failed: {e}"

    if df is None:
        if EXCEL_PATH and os.path.exists(EXCEL_PATH):
            try:
                df = _load_from_excel(EXCEL_PATH, SHEET_NAME)
            except Exception as e:
                last_error = f"Excel load failed: {e}"
        else:
            last_error = last_error or "No data source available"

    if df is None:
        DF = pd.DataFrame()
        MODEL_COL = None
        _LAST_LOAD_TS = now_ts
        print(f"[ERROR] load_data: {last_error}")
        return

    df = normalize_columns(df)

    for c in REQUIRED_COLS:
        if c not in df.columns:
            df[c] = None

    MODEL_COL = pick_first_existing_column(df, MODEL_CANDIDATES)
    if MODEL_COL is None:
        df["Model Name"] = None
        MODEL_COL = "Model Name"

    df["RO_DATE_DT"] = df["RO Open Date"].apply(parse_date_any)

    _sample_raw    = df["RO Open Date"].dropna().head(5).tolist()
    _sample_parsed = df["RO_DATE_DT"].dropna().head(5).tolist()
    _nat_count     = int(df["RO_DATE_DT"].isna().sum())
    print(f"[DATE] raw samples   : {_sample_raw}")
    print(f"[DATE] parsed samples: {_sample_parsed}")
    print(f"[DATE] NaT count     : {_nat_count} / {len(df)}")

    today = pd.Timestamp(date.today())
    df["DAYS_OPEN"] = (today - df["RO_DATE_DT"]).dt.days
    df["DAYS_OPEN"] = df["DAYS_OPEN"].fillna(0).astype(int)
    df.loc[df["DAYS_OPEN"] < 0, "DAYS_OPEN"] = 0
    df["AGE_BUCKET"] = df["DAYS_OPEN"].apply(age_bucket_from_days)

    df["HOLD_REASON_CLEAN"] = df["Hold Reason"].apply(lambda x: safe_str(x, "")).astype(str).str.strip()
    df.loc[df["HOLD_REASON_CLEAN"] == "", "HOLD_REASON_CLEAN"] = "No reason"

    df["MODEL_NAME_CLEAN"] = df[MODEL_COL].apply(lambda x: safe_str(x, "")).astype(str).str.strip()
    df.loc[df["MODEL_NAME_CLEAN"] == "", "MODEL_NAME_CLEAN"] = "Unknown"

    fn = df["Owner Contact First Name"].apply(lambda x: safe_str(x, "")).astype(str)
    ln = df["Owner Contact Last Name"].apply(lambda x: safe_str(x, "")).astype(str)
    df["CUSTOMER_NAME"] = (fn.str.strip() + " " + ln.str.strip()).str.strip()
    df["CUSTOMER_NAME"] = df["CUSTOMER_NAME"].apply(proper_case_name)
    df.loc[df["CUSTOMER_NAME"] == "", "CUSTOMER_NAME"] = "Unknown"

    df["RO_AMOUNT_NUM"]    = df["Total RO Amount"].apply(clean_money_to_float)
    df["PARTS_AMOUNT_NUM"] = df["Total Parts Amount"].apply(clean_money_to_float)
    df["LABOR_AMOUNT_NUM"] = df["Total Labor Amount"].apply(clean_money_to_float)

    df["ODOMETER_NUM"] = df["Odometer Reading"].apply(clean_odometer_to_int)
    print(f"[ODO] sample cleaned values: {df['ODOMETER_NUM'].head(5).tolist()}")

    df["RO_TYPE_CLEAN"] = df["RO Type"].apply(lambda x: safe_str(x, "")).astype(str).str.strip()
    df.loc[df["RO_TYPE_CLEAN"] == "", "RO_TYPE_CLEAN"] = "Unknown"
    print(f"[RO_TYPE] unique values: {sorted(df['RO_TYPE_CLEAN'].unique().tolist())}")

    df["VISIT_TYPE_CLEAN"] = df["Visit Type"].apply(lambda x: safe_str(x, "")).astype(str).str.strip()
    df.loc[df["VISIT_TYPE_CLEAN"] == "", "VISIT_TYPE_CLEAN"] = "Unknown"
    print(f"[VISIT_TYPE] unique values: {sorted(df['VISIT_TYPE_CLEAN'].unique().tolist())}")

    # Branch display name (code → city name, with code for filter matching)
    df["BRANCH_DISPLAY"] = df["Dealer Code"].apply(lambda x: branch_display(safe_str(x, "")))

    df = df.sort_values("RO_DATE_DT", ascending=False, na_position="last").reset_index(drop=True)

    DF = df
    _LAST_LOAD_TS = now_ts
    print(f"[OK] Loaded rows: {len(DF)} | model_col: {MODEL_COL} | source: {'google_csv' if GOOGLE_SHEET_CSV_URL else 'excel'}")


load_data(force=True)


# =========================================================
# FILTERING
# =========================================================
def _multi(args, key):
    raw = args.get(key, "") or ""
    return [v.strip() for v in raw.split(",") if v.strip() and v.strip() != "All"]


def apply_filters(df: pd.DataFrame, args: dict) -> pd.DataFrame:
    out = df.copy()

    # Branch filter: frontend sends dealer codes (e.g. "AKJA"), filter on raw Dealer Code column
    branches     = _multi(args, "branch")
    statuses     = _multi(args, "status")
    age_buckets  = _multi(args, "age_bucket")
    sr_types     = _multi(args, "sr_type")      # kept for API compatibility
    ro_types     = _multi(args, "ro_type")
    visit_types  = _multi(args, "visit_type")
    hold_reasons = _multi(args, "hold_reason")
    model_names  = _multi(args, "model_name")
    sa_names     = _multi(args, "sa_name")
    reg_search   = (args.get("reg_search", "") or "").strip()
    from_date    = (args.get("from_date",  "") or "").strip()
    to_date      = (args.get("to_date",    "") or "").strip()

    if branches:
        out = out[out["Dealer Code"].astype(str).isin(branches)]
    if statuses:
        out = out[out["Status"].astype(str).isin(statuses)]
    if age_buckets:
        out = out[out["AGE_BUCKET"].astype(str).isin(age_buckets)]
    if sr_types:
        out = out[out["SR Type"].astype(str).isin(sr_types)]
    if ro_types:
        out = out[out["RO_TYPE_CLEAN"].astype(str).isin(ro_types)]
    if visit_types:
        out = out[out["VISIT_TYPE_CLEAN"].astype(str).isin(visit_types)]
    if hold_reasons:
        out = out[out["HOLD_REASON_CLEAN"].astype(str).isin(hold_reasons)]
    if model_names:
        out = out[out["MODEL_NAME_CLEAN"].astype(str).isin(model_names)]
    if sa_names:
        out = out[out["Assigned To Full Name"].astype(str).isin(sa_names)]

    if reg_search:
        key = reg_search.upper()
        out = out[out["Vehicle Registration No"].astype(str).str.upper().str.contains(key, na=False)]

    if "RO_DATE_DT" not in out.columns:
        out["RO_DATE_DT"] = out["RO Open Date"].apply(parse_date_any)

    fd = parse_iso_yyyy_mm_dd(from_date) if from_date else None
    td = parse_iso_yyyy_mm_dd(to_date)   if to_date   else None
    if fd is not None:
        out = out[out["RO_DATE_DT"] >= fd]
    if td is not None:
        td_end = td + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        out = out[out["RO_DATE_DT"] <= td_end]

    return out


def json_row(r) -> dict:
    dealer_code = safe_str(r.get("Dealer Code"))
    return {
        "ro_id":              safe_str(r.get("Repair Order #")),
        "ro_date":            fmt_ddmmyyyy(r.get("RO_DATE_DT")),
        "branch":             dealer_code,                         # raw code for filter compat
        "branch_name":        branch_display(dealer_code),         # human-readable city name
        "status":             safe_str(r.get("Status")),
        "sr_type":            safe_str(r.get("SR Type")),
        "ro_type":            safe_str(r.get("RO_TYPE_CLEAN")),
        "visit_type":         safe_str(r.get("VISIT_TYPE_CLEAN")),
        "hold_reason":        safe_str(r.get("HOLD_REASON_CLEAN")),
        "model_name":         safe_str(r.get("MODEL_NAME_CLEAN")),
        "customer_name":      safe_str(r.get("CUSTOMER_NAME")),
        "sa_name":            safe_str(r.get("Assigned To Full Name")),
        "reg_number":         safe_str(r.get("Vehicle Registration No")),
        "km":                 int(r.get("ODOMETER_NUM") or 0),
        "age_bucket":         safe_str(r.get("AGE_BUCKET")),
        "days":               to_int_safe(r.get("DAYS_OPEN"), 0),
        "total_ro_amount":    float(r.get("RO_AMOUNT_NUM",    0.0) or 0.0),
        "total_parts_amount": float(r.get("PARTS_AMOUNT_NUM", 0.0) or 0.0),
        "total_labor_amount": float(r.get("LABOR_AMOUNT_NUM", 0.0) or 0.0),
    }


# =========================================================
# FLASK APP
# =========================================================
app = Flask(__name__)


@app.after_request
def add_cors_headers(resp):
    resp.headers["Access-Control-Allow-Origin"]  = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type,Authorization"
    return resp


@app.route("/health")
def health():
    load_data(force=False)
    return jsonify({"status": "ok", "rows": int(len(DF)) if DF is not None else 0})


@app.route("/api/debug")
def api_debug():
    load_data(force=False)
    if DF is None or DF.empty:
        return jsonify({"error": "No data loaded"})
    nat_count = int(DF["RO_DATE_DT"].isna().sum()) if "RO_DATE_DT" in DF.columns else -1
    rows = []
    for _, r in DF.head(10).iterrows():
        rows.append({
            "raw_ro_open_date":  str(r.get("RO Open Date", "")),
            "parsed_ro_date_dt": str(r.get("RO_DATE_DT", "")),
            "days_open":         int(r.get("DAYS_OPEN", 0)),
            "age_bucket":        str(r.get("AGE_BUCKET", "")),
            "raw_odometer":      str(r.get("Odometer Reading", "")),
            "parsed_odometer":   int(r.get("ODOMETER_NUM", 0)),
            "ro_type":           str(r.get("RO_TYPE_CLEAN", "")),
            "visit_type":        str(r.get("VISIT_TYPE_CLEAN", "")),
        })
    return jsonify({
        "total_rows":             len(DF),
        "nat_count":              nat_count,
        "sample_raw_dates":       DF["RO Open Date"].dropna().head(5).tolist(),
        "age_buckets_present":    sorted(DF["AGE_BUCKET"].unique().tolist()) if "AGE_BUCKET" in DF.columns else [],
        "sample_odometer_raw":    DF["Odometer Reading"].dropna().head(5).tolist(),
        "sample_odometer_parsed": DF["ODOMETER_NUM"].head(5).tolist() if "ODOMETER_NUM" in DF.columns else [],
        "ro_types_present":       sorted(DF["RO_TYPE_CLEAN"].unique().tolist())    if "RO_TYPE_CLEAN"    in DF.columns else [],
        "visit_types_present":    sorted(DF["VISIT_TYPE_CLEAN"].unique().tolist()) if "VISIT_TYPE_CLEAN" in DF.columns else [],
        "rows":                   rows,
    })


@app.route("/api/reload")
def api_reload():
    load_data(force=True)
    return jsonify({"ok": True, "rows": int(len(DF)), "model_col": MODEL_COL})


@app.route("/api/filter-options")
def filter_options():
    load_data(force=False)
    if DF is None or DF.empty:
        return jsonify({
            "branches": [{"code": "All", "name": "All"}],
            "statuses": ["All"], "age_buckets": ["All"],
            "ro_types": ["All"], "visit_types": ["All"],
            "hold_reasons": ["All"], "model_names": ["All"], "sa_names": ["All"],
        })

    # Return branch objects with code + display name, sorted by display name
    raw_codes = [safe_str(x) for x in DF["Dealer Code"].dropna().unique()]
    branch_objects = sorted(
        [{"code": c, "name": branch_display(c)} for c in raw_codes if c not in ("-", "")],
        key=lambda b: b["name"]
    )

    statuses     = ["All"] + sorted([safe_str(x) for x in DF["Status"].dropna().unique()])
    hold_reasons = ["All"] + sorted([safe_str(x) for x in DF["HOLD_REASON_CLEAN"].dropna().unique()])
    model_names  = ["All"] + sorted([safe_str(x) for x in DF["MODEL_NAME_CLEAN"].dropna().unique()])
    sa_names     = ["All"] + sorted([safe_str(x) for x in DF["Assigned To Full Name"].dropna().unique()])
    ro_types     = ["All"] + sorted([safe_str(x) for x in DF["RO_TYPE_CLEAN"].dropna().unique()])
    visit_types  = ["All"] + sorted([safe_str(x) for x in DF["VISIT_TYPE_CLEAN"].dropna().unique()])

    age_order   = ["0-3 days", "4-10 days", "11-15 days", "16-30 days", "31-60 days", "Above 60"]
    present     = [x for x in age_order if x in set(DF["AGE_BUCKET"].astype(str).unique())]
    age_buckets = ["All"] + present

    return jsonify({
        "branches":    branch_objects,   # [{code, name}, ...]
        "statuses":    statuses,
        "age_buckets": age_buckets,
        "ro_types":    ro_types,
        "visit_types": visit_types,
        "hold_reasons": hold_reasons,
        "model_names": model_names,
        "sa_names":    sa_names,
    })


@app.route("/api/sa-names-by-branch")
def sa_names_by_branch():
    load_data(force=False)
    if DF is None or DF.empty:
        return jsonify({"sa_names": ["All"]})

    branches = _multi(request.args, "branch")
    subset   = DF[DF["Dealer Code"].astype(str).isin(branches)] if branches else DF

    sa_names = ["All"] + sorted([
        safe_str(x) for x in subset["Assigned To Full Name"].dropna().unique()
        if safe_str(x) not in ("-", "")
    ])
    return jsonify({"sa_names": sa_names})


@app.route("/api/stats")
def stats():
    load_data(force=False)
    if DF is None or DF.empty:
        return jsonify({"total_ros": 0, "total_ro_amount": 0.0,
                        "total_parts_amount": 0.0, "total_labor_amount": 0.0})

    filtered = apply_filters(DF, request.args)
    return jsonify({
        "total_ros":          int(len(filtered)),
        "total_ro_amount":    float(filtered["RO_AMOUNT_NUM"].sum())    if "RO_AMOUNT_NUM"    in filtered.columns else 0.0,
        "total_parts_amount": float(filtered["PARTS_AMOUNT_NUM"].sum()) if "PARTS_AMOUNT_NUM" in filtered.columns else 0.0,
        "total_labor_amount": float(filtered["LABOR_AMOUNT_NUM"].sum()) if "LABOR_AMOUNT_NUM" in filtered.columns else 0.0,
    })


@app.route("/api/rows")
def rows():
    load_data(force=False)
    if DF is None or DF.empty:
        return jsonify({"total_count": 0, "filtered_count": 0, "rows": []})

    limit = int(request.args.get("limit", "50"))
    skip  = int(request.args.get("skip",  "0"))

    filtered       = apply_filters(DF, request.args)
    total_count    = int(len(DF))
    filtered_count = int(len(filtered))

    page = filtered.iloc[skip: skip + limit] if limit > 0 else filtered
    out  = [json_row(r) for _, r in page.iterrows()]

    return jsonify({"total_count": total_count, "filtered_count": filtered_count, "rows": out})


@app.route("/api/export")
def export_excel():
    load_data(force=False)
    if DF is None or DF.empty:
        return jsonify({"error": "No data"})

    filtered = apply_filters(DF, request.args).copy()
    if filtered.empty:
        return jsonify({"error": "No data for filters"})

    export_df = pd.DataFrame([json_row(r) for _, r in filtered.iterrows()])

    # Use branch_name (city name) for the exported Branch column
    export_df["branch"] = export_df["branch_name"]
    export_df = export_df.drop(columns=["branch_name"], errors="ignore")

    export_df = export_df.rename(columns={
        "ro_id":            "RO ID",
        "ro_date":          "RO Date",
        "branch":           "Branch",
        "status":           "Status",
        "sr_type":          "SR Type",
        "ro_type":          "RO Type",
        "visit_type":       "Visit Type",
        "hold_reason":      "Hold Reason",
        "model_name":       "Model Name",
        "customer_name":    "Customer Name",
        "sa_name":          "SA Name",
        "reg_number":       "Reg Number",
        "km":               "KM",
        "age_bucket":       "Age Bucket",
        "days":             "Days",
        "total_ro_amount":    "Total RO Amount",
        "total_parts_amount": "Total Parts Amount",
        "total_labor_amount": "Total Labor Amount",
    })

    desired_order = [
        "RO ID", "RO Date", "Branch", "Status", "RO Type", "Visit Type", "SR Type", "Hold Reason",
        "SA Name", "Reg Number", "Customer Name", "Model Name", "KM",
        "Age Bucket", "Days", "Total RO Amount", "Total Parts Amount", "Total Labor Amount"
    ]
    existing  = [c for c in desired_order if c in export_df.columns]
    remaining = [c for c in export_df.columns if c not in existing]
    export_df = export_df[existing + remaining]

    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        export_df.to_excel(writer, index=False, sheet_name="Open_RO")
    bio.seek(0)

    filename = f"Open_RO_Export_{date.today().isoformat()}.xlsx"
    return send_file(bio, as_attachment=True, download_name=filename,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# =========================================================
# FRONTEND (embedded HTML)
# =========================================================
HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Unnati Vehicles Open RO Dashboard</title>
<style>
*{margin:0;padding:0;box-sizing:border-box;}
body{font-family:'Segoe UI',sans-serif;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);min-height:100vh;padding:18px;}
.container{max-width:1400px;margin:0 auto;}
header{background:#fff;padding:16px 18px;border-radius:12px;margin-bottom:16px;box-shadow:0 10px 30px rgba(0,0,0,.1);display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap;}
h1{font-size:26px;color:#111;}
.header-actions{display:flex;gap:10px;align-items:center;}
.btn{border:none;border-radius:10px;padding:10px 16px;font-weight:700;cursor:pointer;font-size:13px;transition:transform .15s;}
.btn:active{transform:scale(.97);}
.btn-clear{background:#e74c3c;color:#fff;}
.btn-clear:hover{background:#c0392b;}
.btn-theme{background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;width:44px;height:44px;display:flex;align-items:center;justify-content:center;font-size:18px;box-shadow:0 4px 15px rgba(102,126,234,.3);}

.stats-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin-bottom:14px;}
.card{background:#fff;border-radius:12px;padding:16px;box-shadow:0 5px 15px rgba(0,0,0,.1);text-align:center;}
.card .label{font-size:11px;letter-spacing:.6px;color:#666;font-weight:800;text-transform:uppercase;}
.card .value{margin-top:10px;font-size:28px;color:#667eea;font-weight:900;}
.card.grad{background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;}
.card.grad .label{color:rgba(255,255,255,.85);}
.card.grad .value{color:#fff;font-size:22px;}

.filters{background:#fff;border-radius:12px;padding:14px;box-shadow:0 5px 15px rgba(0,0,0,.1);margin-bottom:14px;}
.filters-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;}
.flabel{display:block;font-size:12px;font-weight:800;color:#111;margin-bottom:6px;}
input[type=date],input[type=text],select{width:100%;padding:10px;border-radius:10px;border:1px solid #ddd;font-size:13px;outline:none;background:#fff;color:#111;}

.ms-wrap{position:relative;}
.ms-trigger{width:100%;padding:10px 34px 10px 10px;border-radius:10px;border:1px solid #ddd;background:#fff;font-size:13px;font-weight:600;cursor:pointer;text-align:left;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;color:#111;position:relative;}
.ms-trigger::after{content:"▾";position:absolute;right:10px;top:50%;transform:translateY(-50%);font-size:11px;color:#888;pointer-events:none;}
.ms-trigger.active{border-color:#667eea;box-shadow:0 0 0 2px rgba(102,126,234,.2);}

.ms-panel{position:fixed;z-index:99999;background:#fff;border:1px solid #ddd;border-radius:12px;box-shadow:0 8px 30px rgba(0,0,0,.18);padding:10px;display:none;min-width:200px;max-height:280px;overflow:hidden;flex-direction:column;}
.ms-panel.open{display:flex;}
.ms-search{width:100%;padding:9px 10px;border-radius:8px;border:1px solid #e0e0e0;font-size:13px;outline:none;margin-bottom:8px;flex-shrink:0;}
.ms-actions{display:flex;gap:6px;margin-bottom:8px;flex-shrink:0;}
.ms-actions button{border:none;border-radius:8px;padding:6px 10px;font-size:12px;font-weight:700;cursor:pointer;background:#f3f3f3;color:#333;}
.ms-actions button:hover{background:#e8e8e8;}
.ms-list{overflow-y:auto;flex:1;}
.ms-item{display:flex;align-items:center;gap:8px;padding:7px 6px;border-radius:8px;cursor:pointer;user-select:none;}
.ms-item:hover{background:#f5f5ff;}
.ms-item input[type=checkbox]{width:15px;height:15px;cursor:pointer;accent-color:#667eea;flex-shrink:0;}
.ms-item .ms-txt{font-size:13px;color:#222;}
.ms-item.all-row .ms-txt{font-weight:700;color:#667eea;}

.table-wrap{background:#fff;border-radius:12px;box-shadow:0 5px 15px rgba(0,0,0,.1);overflow:hidden;}
.table-header{display:flex;align-items:center;justify-content:space-between;padding:12px 14px;background:#f7f7f7;border-bottom:1px solid #e7e7e7;gap:10px;flex-wrap:wrap;}
.info{font-size:12px;color:#444;font-weight:700;}
.btn-export{background:#27ae60;color:#fff;}
.btn-export:hover{background:#229954;}
.scroll{overflow:auto;max-height:560px;}
table{width:100%;border-collapse:collapse;min-width:1400px;}
thead th{position:sticky;top:0;background:#fff;z-index:10;border-bottom:2px solid #eee;padding:12px 10px;font-size:11px;text-transform:uppercase;letter-spacing:.5px;text-align:left;}
tbody td{border-bottom:1px solid #f0f0f0;padding:12px 10px;font-size:12px;vertical-align:top;}
tbody tr:hover{background:#fafafa;}
.badge{padding:5px 10px;border-radius:999px;font-weight:900;font-size:11px;display:inline-block;}
.badge-green{background:#dff4df;color:#0b7a28;}
.badge-amber{background:#fff0d9;color:#b85d00;}
.ro-id,.reg,.money{font-weight:900;}
.muted{color:#666;}

body.dark{background:linear-gradient(135deg,#1a1a2e,#16213e);color:#e0e0e0;}
body.dark header,body.dark .card,body.dark .filters,body.dark .table-wrap{background:#2d3561;color:#e0e0e0;box-shadow:0 5px 15px rgba(0,0,0,.3);}
body.dark h1{color:#e0e0e0;}
body.dark .table-header{background:#3a4575;border-bottom-color:#4a5585;}
body.dark input[type=date],body.dark input[type=text],body.dark select{background:#3a4575;color:#e0e0e0;border-color:#4a5585;}
body.dark thead th{background:#2d3561;color:#e0e0e0;border-bottom-color:#3a4575;}
body.dark tbody td{border-bottom-color:#3a4575;color:#e0e0e0;}
body.dark tbody tr:hover{background:#3a4575;}
body.dark .ms-trigger{background:#3a4575;border-color:#4a5585;color:#e0e0e0;}
body.dark .ms-trigger.active{border-color:#667eea;}
body.dark .ms-panel{background:#2d3561;border-color:#4a5585;}
body.dark .ms-search{background:#3a4575;border-color:#4a5585;color:#e0e0e0;}
body.dark .ms-actions button{background:#3a4575;color:#e0e0e0;}
body.dark .ms-actions button:hover{background:#4a5585;}
body.dark .ms-item:hover{background:#3a4575;}
body.dark .ms-item .ms-txt{color:#e0e0e0;}
body.dark .ms-item.all-row .ms-txt{color:#a0b4ff;}
body.dark .flabel{color:#ccc;}
</style>
</head>
<body>
<div class="container">
  <header>
    <h1>Unnati Vehicles Open RO Dashboard</h1>
    <div class="header-actions">
      <button class="btn btn-theme" id="themeBtn" title="Toggle Theme">🌙</button>
      <button class="btn btn-clear" id="clearBtn">Clear All</button>
    </div>
  </header>

  <div class="stats-grid">
    <div class="card"><div class="label">Total ROs</div><div class="value" id="kpi_total_ros">0</div></div>
    <div class="card grad"><div class="label">Total RO Amount</div><div class="value" id="kpi_ro_amt">₹0.00</div></div>
    <div class="card grad"><div class="label">Total Parts Amount</div><div class="value" id="kpi_parts_amt">₹0.00</div></div>
    <div class="card grad"><div class="label">Total Labor Amount</div><div class="value" id="kpi_labor_amt">₹0.00</div></div>
  </div>

  <div class="filters">
    <div class="filters-grid">
      <div><span class="flabel">Branch</span>         <div class="ms-wrap" id="ms_branch"></div></div>
      <div><span class="flabel">SA Name</span>        <div class="ms-wrap" id="ms_sa_name"></div></div>
      <div><span class="flabel">RO Status</span>      <div class="ms-wrap" id="ms_status"></div></div>
      <div><span class="flabel">RO Type</span>        <div class="ms-wrap" id="ms_ro_type"></div></div>
      <div><span class="flabel">Visit Type</span>     <div class="ms-wrap" id="ms_visit_type"></div></div>
      <div><span class="flabel">Age Bucket</span>     <div class="ms-wrap" id="ms_age_bucket"></div></div>
      <div><span class="flabel">Hold Reason</span>    <div class="ms-wrap" id="ms_hold_reason"></div></div>
      <div><span class="flabel">Model Name</span>     <div class="ms-wrap" id="ms_model_name"></div></div>
      <div><span class="flabel">From Date</span>      <input type="date" id="from_date"/></div>
      <div><span class="flabel">To Date</span>        <input type="date" id="to_date"/></div>
      <div><span class="flabel">Reg. Number</span>    <input type="text" id="reg_search" placeholder="Search registration..."/></div>
      <div><span class="flabel">Records</span>
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
        <thead><tr>
          <th>RO ID</th><th>RO Date</th><th>Branch</th><th>Status</th>
          <th>RO Type</th><th>Visit Type</th>
          <th>SA Name</th><th>Reg Number</th>
          <th>Customer Name</th><th>Model Name</th><th>KM</th>
          <th>Age Bucket</th><th>Days</th>
          <th>Total RO Amount</th><th>Total Parts Amount</th><th>Total Labor Amount</th>
        </tr></thead>
        <tbody id="tbody"><tr><td colspan="16" class="muted">Loading...</td></tr></tbody>
      </table>
    </div>
  </div>
</div>

<script>
const API = window.location.origin;
const _allWidgets = [];

/* ── Branch MultiSelect: stores codes internally, displays names ── */
function BranchMultiSelect(wrapperId, placeholder) {
  const wrap    = document.getElementById(wrapperId);
  const trigger = document.createElement("button");
  trigger.type  = "button";
  trigger.className = "ms-trigger";
  trigger.textContent = placeholder;

  const panel   = document.createElement("div");
  panel.className = "ms-panel";
  panel.innerHTML = `
    <input class="ms-search" type="text" placeholder="Search…"/>
    <div class="ms-actions">
      <button type="button" data-a="all">Select All</button>
      <button type="button" data-a="none">Clear</button>
    </div>
    <div class="ms-list"></div>`;

  document.body.appendChild(panel);
  wrap.appendChild(trigger);

  const search = panel.querySelector(".ms-search");
  const list   = panel.querySelector(".ms-list");
  let options  = [];   // [{code, name}]
  let selected = new Set();  // stores codes
  let onChange = null;

  function reposition() {
    const r = trigger.getBoundingClientRect();
    const vh = window.innerHeight;
    if (vh - r.bottom >= 280 || vh - r.bottom >= 120) {
      panel.style.top    = (r.bottom + 4) + "px";
      panel.style.bottom = "auto";
    } else {
      panel.style.bottom = (vh - r.top + 4) + "px";
      panel.style.top    = "auto";
    }
    panel.style.left  = r.left + "px";
    panel.style.width = Math.max(r.width, 220) + "px";
  }

  function updateTrigger() {
    if (selected.size === 0)      trigger.textContent = placeholder;
    else if (selected.size === 1) {
      const code = [...selected][0];
      const opt  = options.find(o => o.code === code);
      trigger.textContent = opt ? opt.name : code;
    }
    else trigger.textContent = selected.size + " selected";
    trigger.classList.toggle("active", selected.size > 0);
  }

  function render() {
    const q = search.value.trim().toLowerCase();
    list.innerHTML = "";
    const allRow = document.createElement("div");
    allRow.className = "ms-item all-row";
    allRow.innerHTML = `<input type="checkbox" ${selected.size===0?"checked":""}/><span class="ms-txt">${placeholder}</span>`;
    allRow.addEventListener("mousedown", e => { e.preventDefault(); selected.clear(); render(); fire(); });
    list.appendChild(allRow);
    options.forEach(opt => {
      if (q && !opt.name.toLowerCase().includes(q) && !opt.code.toLowerCase().includes(q)) return;
      const row = document.createElement("div");
      row.className = "ms-item";
      const chk = document.createElement("input"); chk.type = "checkbox"; chk.checked = selected.has(opt.code);
      const lbl = document.createElement("span"); lbl.className = "ms-txt"; lbl.textContent = opt.name;
      row.appendChild(chk); row.appendChild(lbl);
      row.addEventListener("mousedown", e => {
        e.preventDefault();
        selected.has(opt.code) ? selected.delete(opt.code) : selected.add(opt.code);
        render(); fire();
      });
      list.appendChild(row);
    });
    updateTrigger();
  }

  function fire() { if (onChange) onChange([...selected]); }
  function open() {
    closeAll(); reposition();
    panel.classList.add("open"); trigger.classList.add("active");
    search.value = ""; render(); search.focus();
  }
  function close() {
    panel.classList.remove("open");
    if (selected.size === 0) trigger.classList.remove("active");
  }

  _allWidgets.push({ close });
  trigger.addEventListener("click", e => { e.stopPropagation(); panel.classList.contains("open") ? close() : open(); });
  search.addEventListener("input", render);
  search.addEventListener("click", e => e.stopPropagation());
  panel.addEventListener("click",  e => e.stopPropagation());
  panel.querySelectorAll(".ms-actions button").forEach(btn => {
    btn.addEventListener("mousedown", e => {
      e.preventDefault();
      if (btn.dataset.a === "all") selected = new Set(options.map(o => o.code));
      else selected.clear();
      render(); fire();
    });
  });
  window.addEventListener("scroll", () => { if (panel.classList.contains("open")) reposition(); }, true);
  window.addEventListener("resize", () => { if (panel.classList.contains("open")) reposition(); });

  this.setOptions = arr => {
    // arr: [{code, name}, ...] — skip the {code:"All"} sentinel
    options  = (arr || []).filter(o => o.code !== "All");
    selected = new Set([...selected].filter(c => options.some(o => o.code === c)));
    render();
  };
  this.getValues = () => [...selected];   // returns codes
  this.clear     = () => { selected.clear(); render(); };
  this.onChange  = fn => { onChange = fn; };
}

/* ── Standard string MultiSelect ── */
function MultiSelect(wrapperId, placeholder) {
  const wrap    = document.getElementById(wrapperId);
  const trigger = document.createElement("button");
  trigger.type  = "button";
  trigger.className = "ms-trigger";
  trigger.textContent = placeholder;

  const panel   = document.createElement("div");
  panel.className = "ms-panel";
  panel.innerHTML = `
    <input class="ms-search" type="text" placeholder="Search…"/>
    <div class="ms-actions">
      <button type="button" data-a="all">Select All</button>
      <button type="button" data-a="none">Clear</button>
    </div>
    <div class="ms-list"></div>`;

  document.body.appendChild(panel);
  wrap.appendChild(trigger);

  const search = panel.querySelector(".ms-search");
  const list   = panel.querySelector(".ms-list");
  let options  = [];
  let selected = new Set();
  let onChange = null;

  function reposition() {
    const r = trigger.getBoundingClientRect();
    const vh = window.innerHeight;
    if (vh - r.bottom >= 280 || vh - r.bottom >= 120) {
      panel.style.top    = (r.bottom + 4) + "px";
      panel.style.bottom = "auto";
    } else {
      panel.style.bottom = (vh - r.top + 4) + "px";
      panel.style.top    = "auto";
    }
    panel.style.left  = r.left + "px";
    panel.style.width = Math.max(r.width, 200) + "px";
  }

  function updateTrigger() {
    if (selected.size === 0)       trigger.textContent = placeholder;
    else if (selected.size === 1)  trigger.textContent = [...selected][0];
    else                           trigger.textContent = selected.size + " selected";
    trigger.classList.toggle("active", selected.size > 0);
  }

  function render() {
    const q = search.value.trim().toLowerCase();
    list.innerHTML = "";
    const allRow = document.createElement("div");
    allRow.className = "ms-item all-row";
    allRow.innerHTML = `<input type="checkbox" ${selected.size===0?"checked":""}/><span class="ms-txt">${placeholder}</span>`;
    allRow.addEventListener("mousedown", e => { e.preventDefault(); selected.clear(); render(); fire(); });
    list.appendChild(allRow);
    options.filter(o => o !== "All").forEach(opt => {
      if (q && !opt.toLowerCase().includes(q)) return;
      const row = document.createElement("div");
      row.className = "ms-item";
      const chk = document.createElement("input"); chk.type = "checkbox"; chk.checked = selected.has(opt);
      const lbl = document.createElement("span"); lbl.className = "ms-txt"; lbl.textContent = opt;
      row.appendChild(chk); row.appendChild(lbl);
      row.addEventListener("mousedown", e => {
        e.preventDefault();
        selected.has(opt) ? selected.delete(opt) : selected.add(opt);
        render(); fire();
      });
      list.appendChild(row);
    });
    updateTrigger();
  }

  function fire() { if (onChange) onChange([...selected]); }
  function open() {
    closeAll(); reposition();
    panel.classList.add("open"); trigger.classList.add("active");
    search.value = ""; render(); search.focus();
  }
  function close() {
    panel.classList.remove("open");
    if (selected.size === 0) trigger.classList.remove("active");
  }

  _allWidgets.push({ close });
  trigger.addEventListener("click", e => { e.stopPropagation(); panel.classList.contains("open") ? close() : open(); });
  search.addEventListener("input", render);
  search.addEventListener("click", e => e.stopPropagation());
  panel.addEventListener("click",  e => e.stopPropagation());
  panel.querySelectorAll(".ms-actions button").forEach(btn => {
    btn.addEventListener("mousedown", e => {
      e.preventDefault();
      if (btn.dataset.a === "all") selected = new Set(options.filter(o => o !== "All"));
      else selected.clear();
      render(); fire();
    });
  });
  window.addEventListener("scroll", () => { if (panel.classList.contains("open")) reposition(); }, true);
  window.addEventListener("resize", () => { if (panel.classList.contains("open")) reposition(); });

  this.setOptions = arr => {
    options  = arr || [];
    selected = new Set([...selected].filter(v => options.includes(v)));
    render();
  };
  this.getValues = () => [...selected];
  this.clear     = () => { selected.clear(); render(); };
  this.onChange  = fn => { onChange = fn; };
}

function closeAll() { _allWidgets.forEach(w => w.close()); }
document.addEventListener("click", closeAll);

function inr(x) {
  const n = Number(x||0);
  if (isNaN(n)) return "₹0.00";
  return "₹" + n.toLocaleString("en-IN",{minimumFractionDigits:2,maximumFractionDigits:2});
}
function badgeClass(s) {
  s = (s||"").toLowerCase();
  if (s.includes("approved")||s.includes("ready")) return "badge badge-green";
  if (s.includes("hold")||s.includes("await")||s.includes("progress")) return "badge badge-amber";
  return "badge badge-green";
}

/* ── Widgets ── */
const MS = {
  branch:      new BranchMultiSelect("ms_branch",      "All Branches"),
  sa_name:     new MultiSelect("ms_sa_name",     "All SA Names"),
  status:      new MultiSelect("ms_status",      "All Statuses"),
  ro_type:     new MultiSelect("ms_ro_type",     "All RO Types"),
  visit_type:  new MultiSelect("ms_visit_type",  "All Visit Types"),
  age_bucket:  new MultiSelect("ms_age_bucket",  "All Age Buckets"),
  hold_reason: new MultiSelect("ms_hold_reason", "All Hold Reasons"),
  model_name:  new MultiSelect("ms_model_name",  "All Models"),
};

function getParams() {
  const p = new URLSearchParams();
  // Branch: getValues() returns dealer codes — send as-is so backend filter works correctly
  const branchCodes = MS.branch.getValues();
  if (branchCodes.length) p.append("branch", branchCodes.join(","));

  const add = (key, w) => { const v = w.getValues(); if (v.length) p.append(key, v.join(",")); };
  add("sa_name",     MS.sa_name);
  add("status",      MS.status);
  add("ro_type",     MS.ro_type);
  add("visit_type",  MS.visit_type);
  add("age_bucket",  MS.age_bucket);
  add("hold_reason", MS.hold_reason);
  add("model_name",  MS.model_name);
  const fd = document.getElementById("from_date").value;
  const td = document.getElementById("to_date").value;
  const rs = document.getElementById("reg_search").value.trim();
  if (fd) p.append("from_date", fd);
  if (td) p.append("to_date",   td);
  if (rs) p.append("reg_search", rs);
  return p;
}

async function reloadSaNames() {
  const branchCodes = MS.branch.getValues();
  const p = new URLSearchParams();
  if (branchCodes.length) p.append("branch", branchCodes.join(","));
  const res  = await fetch(`${API}/api/sa-names-by-branch?${p}`);
  const data = await res.json();
  MS.sa_name.setOptions(data.sa_names || ["All"]);
}

async function loadFilterOptions() {
  const res  = await fetch(`${API}/api/filter-options`);
  const data = await res.json();

  // Branches come as [{code, name}, ...] objects
  MS.branch.setOptions(data.branches || []);

  MS.sa_name.setOptions(data.sa_names      || ["All"]);
  MS.status.setOptions(data.statuses       || ["All"]);
  MS.ro_type.setOptions(data.ro_types      || ["All"]);
  MS.visit_type.setOptions(data.visit_types || ["All"]);
  MS.age_bucket.setOptions(data.age_buckets  || ["All"]);
  MS.hold_reason.setOptions(data.hold_reasons || ["All"]);
  MS.model_name.setOptions(data.model_names  || ["All"]);
}

async function loadStats() {
  const res = await fetch(`${API}/api/stats?${getParams()}`);
  const s   = await res.json();
  document.getElementById("kpi_total_ros").textContent = s.total_ros || 0;
  document.getElementById("kpi_ro_amt").textContent    = inr(s.total_ro_amount    || 0);
  document.getElementById("kpi_parts_amt").textContent = inr(s.total_parts_amount || 0);
  document.getElementById("kpi_labor_amt").textContent = inr(s.total_labor_amount || 0);
}

async function loadRows() {
  const limit = document.getElementById("limit").value;
  const p     = getParams();
  p.append("skip","0"); p.append("limit", limit);
  const res  = await fetch(`${API}/api/rows?${p}`);
  const data = await res.json();
  const rows = data.rows || [];
  document.getElementById("tableInfo").textContent =
    `Showing ${rows.length} of ${data.filtered_count} vehicles (Total: ${data.total_count})`;
  const tb = document.getElementById("tbody");
  tb.innerHTML = "";
  if (!rows.length) { tb.innerHTML=`<tr><td colspan="16" class="muted">No data found</td></tr>`; return; }
  rows.forEach(r => {
    // Display branch_name (city name) in the table; branch (code) used only for filtering
    tb.innerHTML += `<tr>
      <td class="ro-id">${r.ro_id||"-"}</td>
      <td>${r.ro_date||"-"}</td>
      <td>${r.branch_name||r.branch||"-"}</td>
      <td><span class="${badgeClass(r.status)}">${r.status||"-"}</span></td>
      <td>${r.ro_type||"-"}</td>
      <td>${r.visit_type||"-"}</td>
      <td>${r.sa_name||"-"}</td>
      <td class="reg">${r.reg_number||"-"}</td>
      <td>${r.customer_name||"-"}</td>
      <td>${r.model_name||"-"}</td>
      <td>${(r.km||0).toLocaleString("en-IN")}</td>
      <td>${r.age_bucket||"-"}</td>
      <td>${r.days||0}</td>
      <td class="money">${inr(r.total_ro_amount||0)}</td>
      <td class="money">${inr(r.total_parts_amount||0)}</td>
      <td class="money">${inr(r.total_labor_amount||0)}</td>
    </tr>`;
  });
}

async function refreshAll() { await loadStats(); await loadRows(); }

function clearAll() {
  Object.values(MS).forEach(w => w.clear());
  document.getElementById("from_date").value  = "";
  document.getElementById("to_date").value    = "";
  document.getElementById("reg_search").value = "";
  document.getElementById("limit").value      = "50";
  reloadSaNames().then(refreshAll);
}

function toggleTheme() {
  document.body.classList.toggle("dark");
  const dark = document.body.classList.contains("dark");
  localStorage.setItem("uv_openro_theme", dark?"dark":"light");
  document.getElementById("themeBtn").textContent = dark?"☀️":"🌙";
}
function initTheme() {
  if (localStorage.getItem("uv_openro_theme")==="dark") {
    document.body.classList.add("dark");
    document.getElementById("themeBtn").textContent = "☀️";
  }
}

(async function main() {
  initTheme();
  await loadFilterOptions();

  MS.branch.onChange(async () => { await reloadSaNames(); await refreshAll(); });
  MS.sa_name.onChange(refreshAll);
  MS.status.onChange(refreshAll);
  MS.ro_type.onChange(refreshAll);
  MS.visit_type.onChange(refreshAll);
  MS.age_bucket.onChange(refreshAll);
  MS.hold_reason.onChange(refreshAll);
  MS.model_name.onChange(refreshAll);

  document.getElementById("from_date").addEventListener("change", refreshAll);
  document.getElementById("to_date").addEventListener("change",   refreshAll);
  document.getElementById("limit").addEventListener("change",     refreshAll);
  document.getElementById("reg_search").addEventListener("keyup", () => {
    clearTimeout(window.__rt); window.__rt = setTimeout(refreshAll, 250);
  });
  document.getElementById("clearBtn").addEventListener("click",  clearAll);
  document.getElementById("themeBtn").addEventListener("click",  toggleTheme);
  document.getElementById("exportBtn").addEventListener("click", () => {
    window.location.href = `${API}/api/export?${getParams()}`;
  });
  await refreshAll();
})();
</script>
</body>
</html>
"""

@app.route("/")
def home():
    return Response(
        HTML.replace("<h1>Unnati Vehicles Open RO Dashboard</h1>", f"<h1>{APP_TITLE}</h1>")
            .replace("<title>Unnati Vehicles Open RO Dashboard</title>", f"<title>{APP_TITLE}</title>"),
        mimetype="text/html"
    )

# =========================================================
# MAIN
# =========================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)

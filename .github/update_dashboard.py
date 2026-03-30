"""
IDIS Americas Sales Dashboard — Local Update Script
====================================================
사용법:
  python update_dashboard.py

필요 파일 (같은 폴더에 놓거나 아래 PATHS 설정):
  - pipeline_dashboard_TEMPLATE.html  ← 대시보드 HTML 템플릿
  - Sales_YTD_*.xls
  - MTD_booking_*.xls
  - Pending_Fulfillment_*.xls
  - Sales_Activities_*.xls
  - Opps_and_Quotes_*.xlsx

필요 패키지:
  pip install pandas openpyxl lxml
"""

import os
import re
import json
import glob
import argparse
from datetime import datetime, date
from collections import defaultdict

# ── 패키지 체크 ──────────────────────────────────────────────────
try:
    import pandas as pd
    from lxml import etree
except ImportError:
    print("필요 패키지 설치: pip install pandas openpyxl lxml")
    raise


# ════════════════════════════════════════════════════════════════
#  설정 (필요 시 수정)
# ════════════════════════════════════════════════════════════════
class Config:
    # 파일 경로 — 같은 폴더에 있으면 자동 탐지, 아니면 직접 지정
    DATA_DIR   = "."           # 엑셀 파일들이 있는 폴더
    TEMPLATE   = "pipeline_dashboard_TEMPLATE.html"   # 템플릿 HTML
    OUTPUT_DIR = "."           # 출력 폴더

    # 담당자 → 그룹 매핑 (추가/수정 가능)
    REP_GROUP = {
        "Benjamin Barry":  "West",    "Bill Morgan":     "East",
        "Bobby Shiflett":  "West",    "Calan Bateman":   "West",
        "Chris Martinez":  "National","David Bachand":   "National",
        "David Bell":      "East",    "David Leiker":    "West",
        "Ed Snow":         "West",    "Gene Bayer":      "East",
        "Ivanhoe Martinez":"West",    "Jason Burrows":   "National",
        "Jason Morgan":    "East",    "Jesse Wood":      "National",
        "Jim Ball":        "East",    "John Norman":     "East",
        "Jon Turner":      "East",    "Keith Daulton":   "West",
        "Larry Lobue":     "West",    "Mark Bolton":     "West",
        "Michael Dolhan":  "East",    "Nick Giannakis":  "East",
        "Paul Pounds":     "West",    "Steve Ramputi":   "East",
        "Will Switzer":    "West",
    }

    # 담당자별 월간 Target (월 번호: 금액)
    REP_TARGETS = {
        "David Bell":      {1:256500,2:307800,3:461700,4:342000,5:410400,6:615600,7:456000,8:547200,9:820800,10:370500,11:444600,12:666900},
        "Jason Burrows":   {1:146250,2:175500,3:263250,4:195000,5:234000,6:351000,7:260000,8:312000,9:468000,10:211250,11:253500,12:380250},
        "Jesse Wood":      {1:243000,2:291600,3:437400,4:324000,5:388800,6:583200,7:432000,8:518400,9:777600,10:351000,11:421200,12:631800},
        "Benjamin Barry":  {1:56250, 2:67500, 3:101250,4:75000, 5:90000, 6:135000,7:100000,8:120000,9:180000,10:81250, 11:97500, 12:146250},
        "Ivanhoe Martinez":{1:36000, 2:43200, 3:64800, 4:48000, 5:57600, 6:86400, 7:64000, 8:76800, 9:115200,10:52000, 11:62400, 12:93600},
        "Chris Martinez":  {1:60750, 2:72900, 3:109350,4:81000, 5:97200, 6:145800,7:108000,8:129600,9:194400,10:87750, 11:105300,12:157950},
        "David Bachand":   {1:60750, 2:72900, 3:109350,4:81000, 5:97200, 6:145800,7:108000,8:129600,9:194400,10:87750, 11:105300,12:157950},
        # 나머지 담당자 — 기본값 (미설정 시 0)
    }
    # 기본 Target (위에 없는 담당자)
    DEFAULT_TARGET_BY_MONTH = {1:27000,2:32400,3:48600,4:36000,5:43200,6:64800,
                                7:48000,8:57600,9:86400,10:39000,11:46800,12:70200}

    # 팀 → 그룹 매핑
    TEAM_TO_GROUP = {
        "Security: East":          "East",
        "Security: Strategic East":"East",
        "Security: West":          "West",
        "Security: Strategic West":"West",
        "Security: Strategic Logo":"National",
        "Security: National Acct": "National",
    }

    # Active Pipeline 스테이지
    ACTIVE_STAGES = {"Issued Quote", "Open", "In Progress", "Processed"}

    # Closed 상태
    CLOSED_WON_STATUS  = {"Closed Won", "Closed Won (Converted to SO)", "Project Won - Pending Order"}
    CLOSED_LOST_STATUS = {"Closed Lost", "Cancelled"}

    # Prob% 매핑
    PROB_MAP = {
        "Early Discussion":0.1, "Future Strategic":0.1, "Unqualified":0.1,
        "Bid Submitted - Chances Low":0.3,
        "Bid Submitted - Chances Even":0.5,
        "Bid Submitted - Chances High":0.7,
        "Project Won - Pending Order":0.9,
        "Closed Won":1.0, "Closed Won (Converted to SO)":1.0,
        "Closed Lost":0.0, "Cancelled":0.0,
    }


# ════════════════════════════════════════════════════════════════
#  유틸 함수
# ════════════════════════════════════════════════════════════════
def parse_dt(s):
    if not s: return None
    try: return datetime.strptime(str(s)[:10], "%Y-%m-%d")
    except: return None

def parse_dt_pd(s):
    if pd is None or (hasattr(pd, "isna") and pd.isna(s)): return None
    try: return datetime.strptime(str(s)[:10], "%Y-%m-%d")
    except: return None

def parse_amt(s):
    try: return float(str(s).replace(",","") or 0)
    except: return 0.0

def get_week_tag(dt):
    return f"{dt.year}-W{dt.isocalendar()[1]:02d}"

def read_xls_xml(path):
    """SpreadsheetML (.xls) 파일 파싱"""
    with open(path, "rb") as f:
        content = f.read()
    tree = etree.fromstring(content)
    ns = {"ss": "urn:schemas-microsoft-com:office:spreadsheet"}
    result = {}
    for ws in tree.findall(".//ss:Worksheet", ns):
        name = ws.get("{urn:schemas-microsoft-com:office:spreadsheet}Name", "Sheet")
        rows = []
        for row in ws.findall(".//ss:Row", ns):
            cells = [
                (c.find("ss:Data", ns).text if c.find("ss:Data", ns) is not None else "")
                for c in row.findall(".//ss:Cell", ns)
            ]
            rows.append(cells)
        result[name] = rows
    return result

def to_records(rows):
    header = rows[0]
    return [dict(zip(header, r + [""] * max(0, len(header) - len(r)))) for r in rows[1:]]

def find_file(pattern, data_dir="."):
    """glob 패턴으로 가장 최근 파일 탐지"""
    matches = sorted(glob.glob(os.path.join(data_dir, pattern)))
    if not matches:
        return None
    return matches[-1]   # 가장 최근 (알파벳순 마지막)

def get_rep_target(rep, month_num):
    t = Config.REP_TARGETS.get(rep, Config.DEFAULT_TARGET_BY_MONTH)
    return t.get(month_num, 0)


# ════════════════════════════════════════════════════════════════
#  파일 탐지
# ════════════════════════════════════════════════════════════════
def find_input_files(data_dir):
    files = {}
    patterns = {
        "ytd":        "Sales_YTD_*.xls",
        "booking":    "MTD_booking_*.xls",
        "pf":         "Pending_Fulfillment_*.xls",
        "activities": "Sales_Activities_*.xls",
        "opps":       "Opps_and_Quotes_*.xlsx",
    }
    for key, pat in patterns.items():
        path = find_file(pat, data_dir)
        if path:
            print(f"  ✅ {key}: {os.path.basename(path)}")
            files[key] = path
        else:
            print(f"  ❌ {key}: {pat} 파일 없음")
    return files


# ════════════════════════════════════════════════════════════════
#  데이터 파싱
# ════════════════════════════════════════════════════════════════
def parse_ytd(path):
    """Sales YTD → company_monthly.sales, group_sales, so_lines"""
    rows = to_records(list(read_xls_xml(path).values())[0])
    company_sales = defaultdict(float)   # mk -> total
    group_sales   = defaultdict(lambda: defaultdict(float))  # group -> mk -> amt
    so_lines      = defaultdict(list)    # SO# -> [{item,qty,price,amount}]

    for r in rows:
        amt = parse_amt(r.get("Fulfilled Total ($) ", 0))
        dt  = parse_dt(r.get("Date", ""))
        if not dt or amt <= 0: continue
        mk   = dt.strftime("%Y-%m")
        team = r.get("Sales Team (Order)", "") or ""
        so   = r.get("Applied Document #", "") or ""
        item = r.get("Item", "") or ""
        try:
            qty   = float(r.get("Units", 0) or 0)
            price = float(r.get("SO Price", 0) or 0)
        except: qty = price = 0.0

        company_sales[mk] += amt
        grp = Config.TEAM_TO_GROUP.get(team)
        if grp:
            group_sales[grp][mk] += amt
        if so and item:
            so_lines[so].append({"item": item, "qty": qty, "price": price, "amount": amt})

    return dict(company_sales), dict(group_sales), dict(so_lines)


def parse_booking(path, so_lines):
    """MTD Booking → company_monthly.booking, booking_raw(by rep), qu_lookup"""
    rows = to_records(list(read_xls_xml(path).values())[0])
    company_booking = defaultdict(float)
    booking_by_rep  = defaultdict(list)

    for r in rows:
        amt = parse_amt(r.get("Amount", 0))
        if amt <= 0: continue
        dt  = parse_dt(r.get("Date Created", ""))
        if not dt: continue
        mk      = dt.strftime("%Y-%m")
        rep     = r.get("Sales Rep", "") or ""
        team    = r.get("Sales Team (from order)", "") or ""
        so      = r.get("Document Number", "") or ""
        company = r.get("Customer", "") or ""
        grp     = Config.TEAM_TO_GROUP.get(team, "")
        lines   = so_lines.get(so, [])

        company_booking[mk] += amt
        booking_by_rep[rep].append({
            "so": so, "company": company, "amount": amt,
            "date": dt.strftime("%Y-%m-%d"), "mo": dt.month, "month": mk,
            "team": team, "group": grp, "lines": lines,
        })

    return dict(company_booking), dict(booking_by_rep)


def parse_pf(path):
    """Pending Fulfillment → pf_raw list, rep_pf_mo"""
    rows = to_records(list(read_xls_xml(path).values())[0])
    pf_raw   = []
    rep_pf   = defaultdict(lambda: defaultdict(float))

    for r in rows:
        amt = parse_amt(r.get("Line Amount ($) Remaining", 0)) or parse_amt(r.get("Amount", 0))
        if amt <= 0: continue
        ship_dt = parse_dt(r.get("Ship Date", ""))
        if not ship_dt: continue
        mk      = ship_dt.strftime("%Y-%m")
        rep     = r.get("Sales Rep", "") or ""
        team    = r.get("Sales Team (Order)", "") or ""
        so      = r.get("Document Number", "") or ""
        company = r.get("Customer", "") or ""
        grp     = Config.TEAM_TO_GROUP.get(team, "")

        pf_raw.append({"so": so, "rep": rep, "group": grp or team,
                       "company": company, "amount": amt,
                       "ship_date": ship_dt.strftime("%Y-%m-%d"),
                       "month": mk, "status": r.get("Status", "")})
        if rep:
            rep_pf[rep][mk] += amt

    return pf_raw, dict(rep_pf)


def parse_activities(path):
    """Sales Activities → act_rep_mo, act_log"""
    rows = to_records(list(read_xls_xml(path).values())[0])
    act_mo  = defaultdict(lambda: defaultdict(int))
    act_log = defaultdict(list)

    for r in rows:
        rep = r.get("Attendee", "") or ""
        if not rep: continue
        dt  = parse_dt(r.get("Start Date", ""))
        if not dt: continue
        mk  = dt.strftime("%Y-%m")
        act_mo[rep][mk] += 1
        act_log[rep].append({
            "date": dt.strftime("%Y-%m-%d"),
            "company": r.get("Company", "") or "",
            "event":   r.get("Event _____________________________", "") or "",
            "month": dt.month, "year": dt.year,
        })

    return dict(act_mo), dict(act_log)


def parse_opps(path):
    """Opps & Quotes → pipeline, expired, qu_lines, qu_lookup"""
    df = pd.read_excel(path)

    CLOSED_STATUS = Config.CLOSED_WON_STATUS | Config.CLOSED_LOST_STATUS

    # QU line items
    qu_lines  = defaultdict(list)
    qu_meta   = {}

    for _, row in df.iterrows():
        deal   = str(row["Deal Name"]) if not pd.isna(row.get("Deal Name")) else ""
        item   = str(row["Item"])      if not pd.isna(row.get("Item"))      else ""
        if not deal or item == "nan": continue
        qty    = float(row["Quantity"]) if not pd.isna(row.get("Quantity")) else 0
        amt    = float(row["Amount"])   if not pd.isna(row.get("Amount"))   else 0
        stage  = str(row.get("Deal Stage",""))                if not pd.isna(row.get("Deal Stage"))  else ""
        status = str(row.get("Quote/Opportunity Status",""))  if not pd.isna(row.get("Quote/Opportunity Status")) else ""
        owner  = str(row.get("Deal Owner",""))                if not pd.isna(row.get("Deal Owner"))  else ""
        company= str(row.get("Company",""))                   if not pd.isna(row.get("Company"))     else ""
        pf     = str(row.get("Product Family",""))            if not pd.isna(row.get("Product Family")) else ""
        ps     = str(row.get("Product Series",""))            if not pd.isna(row.get("Product Series")) else ""
        exp_c  = parse_dt_pd(row.get("Expected Close"))
        cl_d   = parse_dt_pd(row.get("Close Date"))
        tp     = str(row.get("Type","Quote"))                 if not pd.isna(row.get("Type"))        else "Quote"

        if item:
            qu_lines[deal].append({"item": item, "qty": qty, "amount": amt,
                                   "product_family": pf, "product_series": ps})
        if deal not in qu_meta:
            qu_meta[deal] = {
                "stage": stage, "status": status, "owner": owner,
                "company": company, "type": tp,
                "exp_close": exp_c.strftime("%Y-%m-%d") if exp_c else "",
                "close_date": cl_d.strftime("%Y-%m-%d") if cl_d else "",
            }

    # QU lookup (rep+company -> [QU#, amount]) for SO-QU fuzzy match
    closed_df  = df[df["Quote/Opportunity Status"].isin(Config.CLOSED_WON_STATUS)].copy()
    closed_agg = closed_df.groupby(["Deal Name","Deal Owner","Company"])["Amount"].sum().reset_index()
    qu_lookup  = {}
    for _, row in closed_agg.iterrows():
        owner   = str(row["Deal Owner"]) if not pd.isna(row["Deal Owner"]) else ""
        company = str(row["Company"])    if not pd.isna(row["Company"])    else ""
        key     = f"{owner}|||{company}"
        amt     = float(row["Amount"])   if not pd.isna(row["Amount"])     else 0
        qu_lookup.setdefault(key, []).append({"qu": str(row["Deal Name"]), "amount": amt})

    # Active pipeline (deduplicated by deal)
    deal_agg = df.groupby(
        ["Deal Name","Type","Deal Stage","Quote/Opportunity Status",
         "Company","Deal Owner","Expected Close","Create Date","Expire Date","Forecast Type"]
    )["Amount"].sum().reset_index()

    pipeline = []
    expired  = []

    for _, row in deal_agg.iterrows():
        stage  = str(row["Deal Stage"])                if not pd.isna(row.get("Deal Stage"))  else ""
        status = str(row["Quote/Opportunity Status"])  if not pd.isna(row.get("Quote/Opportunity Status")) else ""
        rep    = str(row["Deal Owner"])                if not pd.isna(row.get("Deal Owner"))  else ""
        grp    = Config.REP_GROUP.get(rep, "")
        amt    = float(row["Amount"])                  if not pd.isna(row.get("Amount"))      else 0
        if amt <= 0: continue
        doc    = str(row["Deal Name"])                 if not pd.isna(row.get("Deal Name"))   else ""
        tp     = str(row["Type"])                      if not pd.isna(row.get("Type"))        else "Quote"
        prob   = Config.PROB_MAP.get(status, 0.1)
        exp_c  = parse_dt_pd(row.get("Expected Close"))
        cr_dt  = parse_dt_pd(row.get("Create Date"))
        exp_d  = parse_dt_pd(row.get("Expire Date"))
        is_won  = status in Config.CLOSED_WON_STATUS
        is_lost = status in Config.CLOSED_LOST_STATUS
        meta    = {"doc": doc, "type": tp, "status": status, "rep": rep, "group": grp,
                   "company": str(row["Company"]) if not pd.isna(row.get("Company")) else "",
                   "amount": round(amt, 2), "prob": prob,
                   "exp_close": exp_c.strftime("%Y-%m-%d") if exp_c else "",
                   "created":   cr_dt.strftime("%Y-%m-%d") if cr_dt else "",
                   "opp_ref": "", "closed_won": is_won, "closed_lost": is_lost,
                   "lines": qu_lines.get(doc, [])}

        if stage in Config.ACTIVE_STAGES:
            pipeline.append(meta)
        elif stage == "Expired":
            meta["expire_date"] = exp_d.strftime("%Y-%m-%d") if exp_d else ""
            expired.append(meta)

    return pipeline, expired, dict(qu_lines), dict(qu_meta), qu_lookup


# ════════════════════════════════════════════════════════════════
#  대시보드 데이터 조립
# ════════════════════════════════════════════════════════════════
def build_dashboard_data(files, as_of_str, week_tag):
    """모든 소스를 파싱해서 대시보드 D 객체 생성"""

    ALL_MONTHS = [f"2026-{m:02d}" for m in range(1, 10)]

    print("\n[1/5] Sales YTD 파싱...")
    company_sales, group_sales, so_lines = parse_ytd(files["ytd"])

    print("[2/5] MTD Booking 파싱...")
    company_booking, booking_raw = parse_booking(files["booking"], so_lines)

    print("[3/5] Pending Fulfillment 파싱...")
    pf_raw, rep_pf_mo = parse_pf(files["pf"])

    print("[4/5] Sales Activities 파싱...")
    act_mo, act_log = parse_activities(files["activities"])

    print("[5/5] Opps & Quotes 파싱...")
    pipeline, expired, qu_lines, qu_meta, qu_lookup = parse_opps(files["opps"])

    # ── company_monthly ─────────────────────────────────────────
    company_monthly = {}
    all_mk = set(list(company_sales.keys()) + list(company_booking.keys()) + ALL_MONTHS)
    for mk in sorted(all_mk):
        company_monthly[mk] = {
            "booking": round(company_booking.get(mk, 0), 2),
            "sales":   round(company_sales.get(mk, 0),   2),
        }

    # ── rep_monthly ──────────────────────────────────────────────
    # sales_raw는 기존 데이터 유지 (YTD에 rep 정보 없음)
    # → 담당자별 sales는 이전 데이터를 그대로 쓰거나 0으로 시작
    rep_monthly = {}
    for rep, grp in Config.REP_GROUP.items():
        monthly = {}
        for mk in ALL_MONTHS:
            m_num = int(mk.split("-")[1])
            monthly[mk] = {
                "target":   get_rep_target(rep, m_num),
                "booking":  round(sum(e["amount"] for e in booking_raw.get(rep, [])
                                      if e["month"] == mk), 2),
                "sales":    0,      # YTD에 rep 미포함 — 이전값 유지
                "pf":       round(rep_pf_mo.get(rep, {}).get(mk, 0), 2),
                "bo":       0,
                "activity": act_mo.get(rep, {}).get(mk, 0),
            }
        # activity log (history 포함)
        log = act_log.get(rep, [])
        rep_monthly[rep] = {"group": grp, "monthly": monthly, "activity_log": log}

    # ── sales_group_monthly ──────────────────────────────────────
    sales_group_monthly = {}
    for grp in ["East", "West", "National"]:
        monthly = {}
        for mk in ALL_MONTHS:
            m_num = int(mk.split("-")[1])
            tgt   = sum(get_rep_target(r, m_num) for r, g in Config.REP_GROUP.items() if g == grp)
            bk    = sum(e["amount"] for rep in booking_raw
                        for e in booking_raw[rep]
                        if e.get("group") == grp and e["month"] == mk)
            pf_g  = sum(p["amount"] for p in pf_raw
                        if p.get("group") == grp and p.get("month") == mk)
            monthly[mk] = {
                "target":  tgt,
                "booking": round(bk, 2),
                "sales":   round(group_sales.get(grp, {}).get(mk, 0), 2),
                "pf":      round(pf_g, 2),
            }
        sales_group_monthly[grp] = monthly

    # ── rep_pipeline_summary ─────────────────────────────────────
    rep_pipeline_summary = {}
    for rep in Config.REP_GROUP:
        deals = [p for p in pipeline if p["rep"] == rep]
        rep_pipeline_summary[rep] = {
            "total": round(sum(p["amount"] for p in deals), 2),
            "count": len(deals),
        }

    # ── group_pipeline ───────────────────────────────────────────
    group_pipeline = {}
    for grp in ["East", "West", "National"]:
        gp = [p for p in pipeline if p["group"] == grp]
        group_pipeline[grp] = {
            "total": round(sum(p["amount"] for p in gp), 2),
            "count": len(gp),
        }

    # ── prob_detail ──────────────────────────────────────────────
    prob_detail = {}
    for pv in [10, 30, 50, 70, 90]:
        deals = [p for p in pipeline if round(p["prob"] * 100) == pv]
        prob_detail[str(pv)] = {
            "count":  len(deals),
            "amount": round(sum(p["amount"] for p in deals), 2),
            "deals":  sorted(deals, key=lambda x: -x["amount"])[:50],
        }

    # ── pipeline_created_weeks ───────────────────────────────────
    pcw = defaultdict(lambda: {"week": "", "total": 0, "count": 0, "deals": []})
    for p in pipeline:
        dt = parse_dt(p.get("created", ""))
        if not dt: continue
        wk = get_week_tag(dt)
        pcw[wk]["week"]  = wk
        pcw[wk]["total"] = round(pcw[wk]["total"] + p["amount"], 2)
        pcw[wk]["count"] += 1
        pcw[wk]["deals"].append({
            "doc": p["doc"], "type": p["type"], "rep": p["rep"],
            "group": p["group"], "company": p["company"],
            "amount": p["amount"], "status": p["status"],
            "created": p.get("created", ""), "exp_close": p.get("exp_close", ""),
            "lines": p.get("lines", []),
            "closed_won": p.get("closed_won", False),
            "closed_lost": p.get("closed_lost", False),
        })

    # ── activity dict ────────────────────────────────────────────
    activity = {}
    for rep in Config.REP_GROUP:
        monthly_act = {mk: act_mo.get(rep, {}).get(mk, 0) for mk in ALL_MONTHS}
        activity[rep] = {"monthly": monthly_act, "log": act_log.get(rep, [])}

    # ── Assemble D ───────────────────────────────────────────────
    D = {
        "as_of":       as_of_str,
        "week_tag":    week_tag,
        "all_months":  ALL_MONTHS,
        "rep_group":   Config.REP_GROUP,
        "rep_targets": {r: {str(m): get_rep_target(r, m) for m in range(1, 13)}
                        for r in Config.REP_GROUP},
        "rep_monthly":            rep_monthly,
        "sales_group_monthly":    sales_group_monthly,
        "pipeline":               pipeline,
        "expired":                expired,
        "expired_count":          len(expired),
        "expired_total":          round(sum(p["amount"] for p in expired), 2),
        "rep_pipeline_summary":   rep_pipeline_summary,
        "group_pipeline":         group_pipeline,
        "prob_detail":            prob_detail,
        "activity":               activity,
        "snapshots":              {},
        "company_monthly":        company_monthly,
        "unassigned_monthly":     {},
        "booking_raw":            booking_raw,
        "sales_raw":              {},          # YTD에 rep 정보 없음 — 추후 추가 가능
        "pf_raw":                 pf_raw,
        "pipeline_created_weeks": dict(pcw),
        "pc_cat_map":             {},
        "sales_rep_monthly_available": False,
        "so_lines":               so_lines,
        "qu_lookup":              qu_lookup,
    }
    return D


# ════════════════════════════════════════════════════════════════
#  HTML 주입
# ════════════════════════════════════════════════════════════════
def inject_data(template_path, D, output_path):
    """템플릿 HTML의 const D={...}; 를 새 데이터로 교체"""
    with open(template_path, "r", encoding="utf-8") as f:
        html = f.read()

    # const D= 위치 찾기
    marker = "const D="
    idx = html.find(marker)
    if idx == -1:
        raise ValueError("템플릿에서 'const D=' 를 찾을 수 없습니다.")

    # 기존 D 객체 끝(중괄호 매칭)
    depth = 0
    i     = idx + len(marker)
    while i < len(html):
        c = html[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                data_end = i + 1
                break
        elif c == '"':
            i += 1
            while i < len(html) and html[i] != '"':
                if html[i] == "\\": i += 1
                i += 1
        i += 1

    new_data_str = json.dumps(D, ensure_ascii=False, separators=(",", ":"))
    new_html = html[:idx + len(marker)] + new_data_str + html[data_end:]

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(new_html)

    size_mb = len(new_html) / 1024 / 1024
    print(f"\n✅ 저장 완료: {output_path} ({size_mb:.1f} MB)")


# ════════════════════════════════════════════════════════════════
#  메인
# ════════════════════════════════════════════════════════════════
def main():
    parser = argparse.ArgumentParser(description="IDIS Dashboard Updater")
    parser.add_argument("--data-dir",  default=Config.DATA_DIR,
                        help="엑셀 파일 폴더 (기본: 현재 폴더)")
    parser.add_argument("--template",  default=Config.TEMPLATE,
                        help="템플릿 HTML 파일 경로")
    parser.add_argument("--output-dir",default=Config.OUTPUT_DIR,
                        help="출력 폴더 (기본: 현재 폴더)")
    parser.add_argument("--as-of",    default=None,
                        help="기준일 YYYY-MM-DD (기본: 오늘)")
    parser.add_argument("--week",     default=None,
                        help="주차 태그 예: 2026-W13 (기본: 자동)")
    args = parser.parse_args()

    # 날짜 결정
    today      = date.today()
    as_of_str  = args.as_of or today.strftime("%Y-%m-%d")
    week_num   = today.isocalendar()[1]
    week_tag   = args.week  or f"{today.year}-W{week_num:02d}"

    print("=" * 55)
    print("  IDIS Americas Dashboard Updater")
    print(f"  기준일: {as_of_str}  |  주차: {week_tag}")
    print("=" * 55)

    # 파일 탐지
    print(f"\n📂 파일 탐색: {os.path.abspath(args.data_dir)}")
    files = find_input_files(args.data_dir)
    missing = [k for k in ["ytd","booking","pf","activities","opps"] if k not in files]
    if missing:
        print(f"\n❌ 필수 파일 없음: {missing}")
        print("파일 이름 패턴을 확인하세요.")
        return

    # 템플릿 확인
    template = args.template
    if not os.path.exists(template):
        print(f"\n❌ 템플릿 파일 없음: {template}")
        return

    # 데이터 빌드
    D = build_dashboard_data(files, as_of_str, week_tag)

    # 요약 출력
    print(f"\n📊 빌드 완료 요약:")
    print(f"  Pipeline:     {len(D['pipeline']):,} 건")
    print(f"  Expired:      {D['expired_count']:,} 건")
    print(f"  PF:           {len(D['pf_raw']):,} 건")
    print(f"  Booking reps: {len(D['booking_raw'])} 명")
    months = ["2026-01","2026-02","2026-03"]
    for mk in months:
        cm = D["company_monthly"].get(mk, {})
        print(f"  {mk}: booking=${cm.get('booking',0):>12,.0f}  "
              f"sales=${cm.get('sales',0):>12,.0f}")

    # HTML 출력 경로
    out_name   = f"pipeline_dashboard_{as_of_str}_{week_tag}.html"
    out_path   = os.path.join(args.output_dir, out_name)

    inject_data(template, D, out_path)

    print(f"\n🎉 완료! 브라우저에서 열어보세요:")
    print(f"   {os.path.abspath(out_path)}")


if __name__ == "__main__":
    main()

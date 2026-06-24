"""
pdf_parser.py
Extracts structured data from Safaricom Results Booklets using pdfplumber.
Handles the structured template format (FY20+ booklets).

Returns a dict of {table_name: [row_dict, ...]} for all 4 raw tables.
"""

import re
import pdfplumber
from pathlib import Path


# ── Regex patterns (validated against real FY26 booklet text) ─────────────────

# Standard KPI row: <label>  <FY_current>  <FY_prior>  <%change>
KPI_ROW = re.compile(
    r"^\s*(?P<label>.+?)\s+"
    r"(?P<current>[\d,]+\.?\d*)\s+"
    r"(?P<prior>[\d,]+\.?\d*)\s+"
    r"(?P<change>\(?[\d,]+\.?\d*\)?%?p?p?t?)\s*$"
)

# Income statement: <label> [note_ref] <group_fy> <group_py> <company_fy> <company_py>
INCOME_ROW = re.compile(
    r"^\s*(?P<label>.+?)\s+"
    r"(?:(?P<note>\d[A-Za-z]?\.\w+)\s+)?"
    r"(?P<g_fy>[\d,]+\.?\d*)\s+"
    r"(?P<g_py>[\d,]+\.?\d*)\s+"
    r"(?P<c_fy>[\d,]+\.?\d*)\s+"
    r"(?P<c_py>[\d,]+\.?\d*)\s*$"
)


def _clean_num(s: str) -> float | None:
    """Strip commas, parens (negatives), return float. None if empty/non-numeric."""
    if not s:
        return None
    s = s.strip()
    negative = s.startswith("(") and s.endswith(")")
    s = s.strip("()").replace(",", "").rstrip("%").rstrip("ppt")
    try:
        val = float(s)
        return -val if negative else val
    except ValueError:
        return None


def _extract_text(pdf_path: str, page_num: int) -> str:
    """Extract layout-aware text from a single page (1-indexed)."""
    with pdfplumber.open(pdf_path) as pdf:
        if page_num > len(pdf.pages):
            return ""
        return pdf.pages[page_num - 1].extract_text(layout=True) or ""


def _detect_period(pdf_path: str) -> dict:
    """
    Detect reporting period from the booklet cover/title page.
    Returns dict with period_label, period_type, fiscal_year, period_end_date.
    """
    with pdfplumber.open(pdf_path) as pdf:
        cover_text = pdf.pages[0].extract_text() or ""

    # Match e.g. "AUDITED RESULTS FOR THE YEAR ENDED 31 MARCH 2026"
    fy_match = re.search(r"YEAR ENDED (\d{1,2}) (\w+) (\d{4})", cover_text, re.IGNORECASE)
    hy_match = re.search(r"SIX MONTHS.*?(\d{1,2}) (\w+) (\d{4})", cover_text, re.IGNORECASE)

    months = {"JANUARY":"01","FEBRUARY":"02","MARCH":"03","APRIL":"04",
              "MAY":"05","JUNE":"06","JULY":"07","AUGUST":"08",
              "SEPTEMBER":"09","OCTOBER":"10","NOVEMBER":"11","DECEMBER":"12"}

    if fy_match:
        day, month_str, year = fy_match.groups()
        month = months.get(month_str.upper(), "03")
        period_end = f"{year}-{month}-{int(day):02d}"
        fiscal_year = int(year)
        return {"period_label": f"FY{str(fiscal_year)[2:]}",
                "period_type": "FY",
                "fiscal_year": fiscal_year,
                "period_end_date": period_end}
    elif hy_match:
        day, month_str, year = hy_match.groups()
        month = months.get(month_str.upper(), "09")
        period_end = f"{year}-{month}-{int(day):02d}"
        fiscal_year = int(year) + 1 if month == "09" else int(year)
        return {"period_label": f"HY{str(fiscal_year)[2:]}",
                "period_type": "HY",
                "fiscal_year": fiscal_year,
                "period_end_date": period_end}

    raise ValueError(f"Could not detect period from PDF: {pdf_path}")


def _parse_kpi_page(pdf_path: str, page_num: int, target_labels: dict) -> dict:
    """
    Parse a KPI page — returns {col_name: value} for matched labels.
    target_labels: {partial_label_string: col_name}
    """
    text = _extract_text(pdf_path, page_num)
    results = {}
    for line in text.splitlines():
        m = KPI_ROW.match(line)
        if not m: continue
        label = m.group("label").strip()
        val   = _clean_num(m.group("current"))
        for partial, col_name in target_labels.items():
            if partial.lower() in label.lower():
                results[col_name] = val
                break
    return results


def _parse_income_page(pdf_path: str, page_num: int, target_labels: dict,
                       use_group: bool = True) -> dict:
    """
    Parse income statement page (4 numeric columns: Group FY, Group PY, Company FY, Company PY).
    use_group=True takes Group column; False takes Company column.
    """
    text = _extract_text(pdf_path, page_num)
    results = {}
    for line in text.splitlines():
        m = INCOME_ROW.match(line)
        if not m: continue
        label = m.group("label").strip()
        val   = _clean_num(m.group("g_fy") if use_group else m.group("c_fy"))
        # Convert Millions to Billions (income statement is in KShs Mns)
        if val is not None:
            val = round(val / 1000, 3)
        for partial, col_name in target_labels.items():
            if partial.lower() in label.lower():
                results[col_name] = val
                break
    return results


def parse_booklet(pdf_path: str) -> dict:
    """
    Main entry point. Parse a Safaricom Results Booklet PDF.
    Returns {table_name: [row_dict]} for all 4 raw tables.
    """
    period = _detect_period(pdf_path)
    print(f"Detected period: {period}")

    # ── company_overview — Group income statement, page 14 ─────────────────────
    # Values in KShs Mns → divide by 1000 for Bns
    overview_row = dict(period)
    income_labels = {
        "Service revenue":          "service_revenue_kes_bn",
        "Operating profit":         "ebit_kes_bn",
        "Profit for the year":      "net_income_kes_bn",
    }
    overview_row.update(_parse_income_page(pdf_path, 14, income_labels, use_group=True))

    # Total revenue from Group KPI page 11
    kpi_labels_group = {
        "One month active customers":  "active_customers_mn",
    }
    overview_row.update(_parse_kpi_page(pdf_path, 11, kpi_labels_group))
    # Note: total_revenue and capex require cash flow page — left for manual review if missing

    # ── mpesa_metrics — Section 4Aa (page 29) + 4Ab (page 31) + 4Ad (page 33) ─
    mpesa_row = dict(period)
    mpesa_kpi_labels = {
        "M-PESA revenue":              "mpesa_revenue_kes_bn",
        "One month active M-PESA":     "mpesa_customers_1m_mn",
        "Merchants":                    "merchants_mn",
    }
    # M-PESA revenue from Kenya KPI page 12
    mpesa_row.update(_parse_kpi_page(pdf_path, 12, mpesa_kpi_labels))

    # ── revenue_segments — Kenya income statement page 20 ──────────────────────
    seg_row = dict(period)
    seg_labels = {
        "Connectivity revenue":         "connectivity_total_kes_bn",
        "M-PESA revenue":               "mpesa_kes_bn",
        "Fixed Service and IoT":        "fixed_service_iot_kes_bn",
        "Service revenue":              "total_service_revenue_kes_bn",
    }
    seg_row.update(_parse_income_page(pdf_path, 20, seg_labels, use_group=False))

    # ── kenya_ethiopia — Kenya (page 20) and Ethiopia income statement (page 37) ─
    ke_row = dict(period)
    ke_row["geography"] = "KE"
    ke_labels = {"Service revenue": "service_revenue_kes_bn", "Operating profit": "ebit_kes_bn"}
    ke_row.update(_parse_income_page(pdf_path, 20, ke_labels, use_group=False))

    et_row = dict(period)
    et_row["geography"] = "ET"
    et_labels = {"Service revenue": "service_revenue_kes_bn", "Operating loss": "ebit_kes_bn"}
    et_row.update(_parse_income_page(pdf_path, 37, et_labels, use_group=True))

    return {
        "company_overview":  [overview_row],
        "mpesa_metrics":     [mpesa_row],
        "revenue_segments":  [seg_row],
        "kenya_ethiopia":    [ke_row, et_row],
    }


if __name__ == "__main__":
    import sys, json
    if len(sys.argv) < 2:
        print("Usage: python3 pdf_parser.py <path_to_booklet.pdf>")
        sys.exit(1)
    result = parse_booklet(sys.argv[1])
    print(json.dumps(result, indent=2, default=str))
"""
Auto-detect chart-worthy data in LLM answers and generate Plotly chart specs.
Parses markdown tables and bullet lists — no LLM dependency.
"""

import re
from typing import Optional

# ─── Helpers ────────────────────────────────────────────────────

def _parse_number(text: str) -> Optional[float]:
    """Extract a numeric value from text like '$1,177,956.44' or '66.8%'."""
    text = text.strip().replace("**", "").replace("\\", "")
    # Remove leading $ or trailing %
    is_pct = text.endswith("%")
    cleaned = text.replace("$", "").replace("%", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _is_numeric_column(values: list[str]) -> bool:
    """Check if most values in a column are numeric."""
    nums = sum(1 for v in values if _parse_number(v) is not None)
    return nums >= len(values) * 0.6 and nums >= 2


def _choose_chart_type(labels: list[str], values: list[float], header: str) -> str:
    """Pick the best chart type based on data characteristics."""
    header_lower = header.lower()
    labels_lower = " ".join(labels).lower()

    # Time-series → line chart
    time_patterns = [r'\b(month|quarter|year|date|period|q[1-4]|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|199[0-9]|200[0-9])\b']
    if any(re.search(p, labels_lower) for p in time_patterns):
        return "line"
    if any(re.search(p, header_lower) for p in time_patterns):
        return "line"

    # Few categories (2) → pie chart (good for binary splits like gender, marital)
    if len(labels) <= 2 and all(v > 0 for v in values):
        return "pie"

    # 3-6 categories with % data summing to ~100 → pie
    if len(labels) <= 6 and all(0 <= v <= 100 for v in values) and abs(sum(values) - 100) < 5:
        return "pie"

    # Many categories or long labels → horizontal bar
    avg_label_len = sum(len(l) for l in labels) / max(len(labels), 1)
    if avg_label_len > 15 or len(labels) > 8:
        return "horizontal_bar"

    return "bar"


# ─── Markdown Table Parser ──────────────────────────────────────

def _extract_tables(text: str) -> list[dict]:
    """Parse markdown tables from text. Returns list of {headers, rows}."""
    tables = []
    lines = text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        # Detect a table header row (has pipes)
        if "|" in line and i + 1 < len(lines) and re.match(r'^[\s|:\-]+$', lines[i + 1].strip()):
            headers = [h.strip().replace("**", "") for h in line.split("|") if h.strip()]
            i += 2  # skip separator
            rows = []
            while i < len(lines) and "|" in lines[i]:
                cells = [c.strip().replace("**", "") for c in lines[i].split("|") if c.strip()]
                if len(cells) == len(headers):
                    rows.append(cells)
                i += 1
            if rows:
                tables.append({"headers": headers, "rows": rows})
            continue
        i += 1
    return tables


def _chart_from_table(table: dict) -> Optional[dict]:
    """Try to build a chart spec from a parsed markdown table."""
    headers = table["headers"]
    rows = table["rows"]

    if len(rows) < 2 or len(headers) < 2:
        return None

    # First column is usually labels
    labels = [row[0].replace("**", "").strip() for row in rows]

    # Filter out summary/total rows
    TOTAL_WORDS = {"total", "sum", "all", "overall", "grand total"}
    valid_indices = [i for i, l in enumerate(labels) if l.lower().strip() not in TOTAL_WORDS]
    if len(valid_indices) < 2:
        return None
    labels = [labels[i] for i in valid_indices]
    rows = [rows[i] for i in valid_indices]

    # Parse all numeric columns
    columns = []  # list of (col_idx, header, values, is_pct)
    for col_idx in range(1, len(headers)):
        col_values_str = [row[col_idx] if col_idx < len(row) else "" for row in rows]
        if not _is_numeric_column(col_values_str):
            continue
        values = []
        for v in col_values_str:
            parsed = _parse_number(v)
            values.append(parsed if parsed is not None else 0)
        h = headers[col_idx]
        is_pct = "%" in h or "percent" in h.lower() or (
            all(0 <= v <= 100 for v in values) and abs(sum(values) - 100) < 5
        )
        columns.append((col_idx, h, values, is_pct))

    if not columns:
        return None

    # Determine chart type first from labels
    chart_type = _choose_chart_type(labels, columns[0][2], columns[0][1])

    # Pick the best column based on chart type
    if chart_type == "pie":
        # For pie charts, prefer percentage columns
        pct_cols = [c for c in columns if c[3]]
        best = pct_cols[0] if pct_cols else columns[0]
    else:
        # For bar/line, prefer absolute value columns
        abs_cols = [c for c in columns if not c[3]]
        best = abs_cols[0] if abs_cols else columns[0]

    _, best_header, best_values, _ = best

    title = f"{best_header} by {headers[0]}"

    return {
        "chart_type": chart_type,
        "title": title,
        "x_label": headers[0],
        "y_label": best_header,
        "data": [{"label": l, "value": v} for l, v in zip(labels, best_values)],
    }


# ─── Bullet List Parser ─────────────────────────────────────────

def _extract_list_data(text: str) -> Optional[dict]:
    """Parse bullet/numbered lists with 'Label: $value' or 'Label — value' patterns."""
    # Match patterns like: - **USA**: $1,177,956.44 or • USA — $1,177  or 1. USA: $1,177
    pattern = r'(?:^|\n)\s*(?:[-•*]|\d+[.)]) \s*\**([^*:\n]+?)\**\s*(?::|—|–|\|)\s*\**\$?([\d,]+\.?\d*)\**'
    matches = re.findall(pattern, text)

    if len(matches) < 2:
        return None

    labels = [m[0].strip() for m in matches]
    values = []
    for m in matches:
        parsed = _parse_number(m[1])
        if parsed is not None:
            values.append(parsed)
        else:
            return None

    chart_type = _choose_chart_type(labels, values, "")
    return {
        "chart_type": chart_type,
        "title": "Comparison",
        "x_label": "",
        "y_label": "",
        "data": [{"label": l, "value": v} for l, v in zip(labels, values)],
    }


# ─── User Chart Preference Detector ──────────────────────────────

_USER_CHART_PREFS = [
    (r'\b(pie\s*chart|pie\s*graph|donut\s*chart|as\s*a?\s*pie)\b', "pie"),
    (r'\b(bar\s*chart|bar\s*graph|as\s*a?\s*bar|column\s*chart)\b', "bar"),
    (r'\b(horizontal\s*bar|hbar|as\s*horizontal)\b', "horizontal_bar"),
    (r'\b(line\s*chart|line\s*graph|as\s*a?\s*line|trend\s*line)\b', "line"),
]


def _detect_user_chart_preference(question: str) -> Optional[str]:
    """Check if the user explicitly asked for a specific chart type."""
    q = question.lower()
    for pattern, chart_type in _USER_CHART_PREFS:
        if re.search(pattern, q):
            return chart_type
    return None


# ─── Main Entry Point ───────────────────────────────────────────

# Questions that typically warrant charts
CHART_TRIGGER_PATTERNS = [
    r'\b(break\s*down|breakdown|by country|by store|by brand|by region|by product)\b',
    r'\b(compar|versus|vs\.?|rank|top \d|bottom \d)\b',
    r'\b(trend|monthly|quarterly|yearly|over time|growth)\b',
    r'\b(distribution|split|proportion|composition)\b',
    r'\b(show me|chart|graph|plot|visuali)\b',
]


def detect_chart(answer: str, question: str = "") -> Optional[dict]:
    """
    Auto-detect if the answer contains chart-worthy data.
    Returns a chart spec dict or None.
    """
    # Check if user explicitly requested a chart type
    user_pref = _detect_user_chart_preference(question)

    # Quick check: does the question/answer suggest a chart?
    combined = f"{question} {answer}".lower()
    should_chart = user_pref or any(re.search(p, combined) for p in CHART_TRIGGER_PATTERNS)

    # Also chart if the answer contains a markdown table with 2+ data rows
    tables = _extract_tables(answer)
    has_table = any(len(t["rows"]) >= 2 for t in tables)

    if not should_chart and not has_table:
        return None

    # Try tables first (most reliable)
    for table in tables:
        chart = _chart_from_table(table)
        if chart:
            if user_pref:
                chart["chart_type"] = user_pref
            return chart

    # Try bullet list extraction
    chart = _extract_list_data(answer)
    if chart:
        if user_pref:
            chart["chart_type"] = user_pref
        return chart

    return None

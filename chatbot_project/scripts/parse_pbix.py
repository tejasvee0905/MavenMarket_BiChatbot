import json, os, re, struct

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
_REPO_ROOT = os.path.dirname(_PROJECT_ROOT)
BASE = os.path.join(_REPO_ROOT, "data", "pbix", "pbix_extracted", "contents")
OUT = os.path.join(_PROJECT_ROOT, "knowledge", "raw")

# ── 1. Parse Report Layout (visuals, pages, fields) ──
with open(os.path.join(BASE, "Report", "Layout"), "r", encoding="utf-16-le") as f:
    layout = json.load(f)

pages = []
for sec in layout.get("sections", []):
    page = {
        "name": sec.get("displayName"),
        "ordinal": sec.get("ordinal"),
        "visuals": []
    }
    for vc in sec.get("visualContainers", []):
        try:
            cfg = json.loads(vc.get("config", "{}"))
            sv = cfg.get("singleVisual", {})
            vtype = sv.get("visualType", "unknown")
            projs = sv.get("projections", {})
            fields = {}
            for role, items in projs.items():
                fields[role] = [item.get("queryRef", "") for item in items]
            # Extract filters if present
            filters = []
            for filt_str in [vc.get("filters", "[]")]:
                try:
                    flist = json.loads(filt_str) if isinstance(filt_str, str) else []
                    for fi in flist:
                        filters.append(fi.get("name", ""))
                except:
                    pass
            page["visuals"].append({
                "type": vtype,
                "fields": fields,
                "filters": filters
            })
        except:
            pass
    pages.append(page)

with open(os.path.join(OUT, "dashboard_visuals.json"), "w") as f:
    json.dump(pages, f, indent=2)
print(f"Saved dashboard_visuals.json — {len(pages)} pages")
for p in pages:
    print(f"  Page '{p['name']}': {len(p['visuals'])} visuals")
    for v in p["visuals"]:
        all_fields = [f for flist in v["fields"].values() for f in flist]
        print(f"    - {v['type']}: {all_fields}")

# ── 2. Parse DataModel (binary, extract readable strings) ──
dm_path = os.path.join(BASE, "DataModel")
with open(dm_path, "rb") as f:
    raw = f.read()

# The DataModel is an ABF (Analysis Services Backup Format) file.
# Try to find JSON metadata within it.
# Look for common patterns: table names, measure definitions, relationships

text_chunks = []
# Find all UTF-16LE strings that look like JSON or DAX
i = 0
current = []
while i < len(raw) - 1:
    # UTF-16LE: low byte, high byte
    lo, hi = raw[i], raw[i+1]
    if hi == 0 and 0x20 <= lo <= 0x7e:
        current.append(chr(lo))
    else:
        if len(current) > 20:
            text_chunks.append("".join(current))
        current = []
    i += 2

if current and len(current) > 20:
    text_chunks.append("".join(current))

# Extract DAX measures
measures = []
dax_pattern = re.compile(r'(CALCULATE|SUMX|COUNTROWS|DIVIDE|RELATED|DISTINCTCOUNT|DATEADD|SUM|AVERAGE|FILTER|ALL|VALUES|IF|SWITCH|TOTALYTD|SAMEPERIODLASTYEAR|FORMAT)', re.IGNORECASE)

for chunk in text_chunks:
    if dax_pattern.search(chunk) and len(chunk) < 2000:
        measures.append(chunk.strip())

# Extract table/column names (look for patterns like Table[Column])
table_col_pattern = re.compile(r"(\w+)\[(\w+)\]")
table_columns = {}
for chunk in text_chunks:
    for match in table_col_pattern.finditer(chunk):
        tbl, col = match.group(1), match.group(2)
        if tbl not in table_columns:
            table_columns[tbl] = set()
        table_columns[tbl].add(col)

# Look for JSON metadata blocks
json_blocks = []
for chunk in text_chunks:
    if chunk.strip().startswith("{") and len(chunk) > 100:
        try:
            obj = json.loads(chunk)
            json_blocks.append(obj)
        except:
            pass

# Save extracted measures
with open(os.path.join(OUT, "dax_measures_raw.json"), "w") as f:
    json.dump(measures, f, indent=2)
print(f"\nExtracted {len(measures)} potential DAX expressions")

# Save table-column map
tc_serializable = {k: sorted(list(v)) for k, v in table_columns.items()}
with open(os.path.join(OUT, "table_columns.json"), "w") as f:
    json.dump(tc_serializable, f, indent=2)
print(f"Extracted {len(tc_serializable)} tables with columns")
for tbl, cols in tc_serializable.items():
    print(f"  {tbl}: {cols[:5]}{'...' if len(cols)>5 else ''}")

# Save JSON metadata blocks
with open(os.path.join(OUT, "model_metadata.json"), "w") as f:
    json.dump(json_blocks, f, indent=2)
print(f"Extracted {len(json_blocks)} JSON metadata blocks")

# ── 3. Look for measure names specifically ──
measure_names = []
for chunk in text_chunks:
    # Measures often appear as "MeasureName = DAX_EXPRESSION"
    m = re.match(r'^([A-Za-z_][\w\s]*?)\s*[:=]\s*(CALCULATE|SUMX|COUNTROWS|DIVIDE|SUM|AVERAGE|DISTINCTCOUNT|IF|SWITCH|RELATED|FILTER|TOTALYTD|DATEADD|FORMAT|SAMEPERIODLASTYEAR)', chunk, re.IGNORECASE)
    if m:
        measure_names.append({"name": m.group(1).strip(), "expression_start": chunk[:200]})

with open(os.path.join(OUT, "measure_definitions.json"), "w") as f:
    json.dump(measure_names, f, indent=2)
print(f"\nFound {len(measure_names)} named measure definitions")
for m in measure_names:
    print(f"  {m['name']}: {m['expression_start'][:80]}...")

# ── 4. Parse DiagramLayout ──
with open(os.path.join(BASE, "DiagramLayout"), "r", encoding="utf-16-le") as f:
    diagram = json.load(f)

with open(os.path.join(OUT, "diagram_layout.json"), "w") as f:
    json.dump(diagram, f, indent=2)
print(f"\nDiagramLayout saved")

print("\n=== DONE ===")

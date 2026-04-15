import struct, re, json, os

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
_REPO_ROOT = os.path.dirname(_PROJECT_ROOT)
dm_path = os.path.join(_REPO_ROOT, "data", "pbix", "pbix_extracted", "contents", "DataModel")
OUT = os.path.join(_PROJECT_ROOT, "knowledge", "raw")

with open(dm_path, "rb") as f:
    data = f.read()

print(f"DataModel size: {len(data)} bytes")
print(f"Header hex: {data[:40].hex()}")

# Extract ASCII strings (30+ chars)
ascii_strings = re.findall(b"[\x20-\x7e]{30,}", data)
print(f"Found {len(ascii_strings)} ASCII strings (30+ chars)")

# Extract UTF-16LE strings (15+ chars)
utf16_strings = []
i = 0
current = []
while i < len(data) - 1:
    lo, hi = data[i], data[i + 1]
    if hi == 0 and 0x20 <= lo <= 0x7e:
        current.append(chr(lo))
    else:
        if len(current) >= 15:
            utf16_strings.append("".join(current))
        current = []
    i += 2
if len(current) >= 15:
    utf16_strings.append("".join(current))

print(f"Found {len(utf16_strings)} UTF-16LE strings (15+ chars)")

# Look for DAX keywords
dax_kw = re.compile(r"CALCULATE|SUMX|COUNTROWS|DIVIDE|RELATED|DISTINCTCOUNT|DATEADD|SUM\b|AVERAGE|FILTER|ALL\b|VALUES|TOTALYTD|SAMEPERIODLASTYEAR|FORMAT|SWITCH|IF\b|SELECTEDVALUE|HASONEVALUE|ISBLANK|BLANK|WEEKDAY|DATESYTD|LASTDATE|MAX\b|MIN\b", re.IGNORECASE)

# Check UTF-16 strings for DAX
dax_expressions = []
for s in utf16_strings:
    if dax_kw.search(s):
        dax_expressions.append(s)

# Check ASCII strings for DAX
for s in ascii_strings:
    decoded = s.decode("ascii", errors="ignore")
    if dax_kw.search(decoded):
        dax_expressions.append(decoded)

print(f"\n=== DAX EXPRESSIONS ({len(dax_expressions)}) ===")
for expr in dax_expressions:
    print(f"  {expr[:200]}")

# Look for table names and measure names
all_strings = [s.decode("ascii", errors="ignore") for s in ascii_strings] + utf16_strings

# Find strings that look like measure assignments
measure_defs = []
for s in all_strings:
    # Pattern: something = DAX_FUNCTION(...)
    m = re.match(r"^([A-Za-z_][\w\s]*?)\s*=\s*(.+)", s)
    if m and dax_kw.search(m.group(2)):
        measure_defs.append({"name": m.group(1).strip(), "expression": s.strip()})

print(f"\n=== MEASURE DEFINITIONS ({len(measure_defs)}) ===")
for md in measure_defs:
    print(f"  {md['name']}: {md['expression'][:150]}")

# Find JSON blocks
json_blocks = []
for s in all_strings:
    s = s.strip()
    if s.startswith("{") and len(s) > 50:
        try:
            obj = json.loads(s)
            json_blocks.append(obj)
        except:
            pass
    elif s.startswith("[") and len(s) > 50:
        try:
            obj = json.loads(s)
            json_blocks.append(obj)
        except:
            pass

print(f"\n=== JSON BLOCKS ({len(json_blocks)}) ===")
for jb in json_blocks:
    print(f"  {str(jb)[:200]}")

# Save everything
with open(os.path.join(OUT, "dax_expressions.json"), "w") as f:
    json.dump(dax_expressions, f, indent=2)

with open(os.path.join(OUT, "measure_definitions.json"), "w") as f:
    json.dump(measure_defs, f, indent=2)

with open(os.path.join(OUT, "model_json_blocks.json"), "w") as f:
    json.dump(json_blocks, f, indent=2, default=str)

# Also dump ALL interesting strings for manual review
interesting = [s for s in all_strings if any(kw in s.lower() for kw in ["maven", "measure", "table", "column", "relationship", "total", "revenue", "profit", "customer", "store", "product", "return", "calendar", "transaction", "region"])]
with open(os.path.join(OUT, "model_strings.json"), "w") as f:
    json.dump(interesting, f, indent=2)
print(f"\n=== INTERESTING STRINGS ({len(interesting)}) ===")
for s in interesting[:50]:
    print(f"  {s[:200]}")

print("\n=== DONE ===")

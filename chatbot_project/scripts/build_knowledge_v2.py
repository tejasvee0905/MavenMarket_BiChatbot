"""
Expanded Knowledge Base Builder v2
Generates 100+ insights from CSV data including counts, cross-tabs, and detailed breakdowns.
"""
import json, os
import pandas as pd
import numpy as np

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
_REPO_ROOT = os.path.dirname(_PROJECT_ROOT)
OUT = os.path.join(_PROJECT_ROOT, "knowledge", "active")
DATA = os.path.join(_REPO_ROOT, "data", "csvs")

# Load all CSVs
cal = pd.read_csv(os.path.join(DATA, "MavenMarket_Calendar.csv"))
cust = pd.read_csv(os.path.join(DATA, "MavenMarket_Customers.csv"))
prod = pd.read_csv(os.path.join(DATA, "MavenMarket_Products.csv"))
regions = pd.read_csv(os.path.join(DATA, "MavenMarket_Regions.csv"))
ret = pd.read_csv(os.path.join(DATA, "MavenMarket_Returns.csv"))
stores = pd.read_csv(os.path.join(DATA, "MavenMarket_Stores.csv"))
txn = pd.read_csv(os.path.join(DATA, "MavenMarket_Transactions.csv"))

# Merge transactions with dimensions
txn_full = txn.merge(prod, on="product_id").merge(cust, on="customer_id").merge(stores, on="store_id", suffixes=("_cust", "_store"))
txn_full["revenue"] = txn_full["quantity"] * txn_full["product_retail_price"]
txn_full["cost"] = txn_full["quantity"] * txn_full["product_cost"]
txn_full["profit"] = txn_full["revenue"] - txn_full["cost"]

# Parse dates
txn_full["transaction_date"] = pd.to_datetime(txn_full["transaction_date"], dayfirst=True)
txn_full["year"] = txn_full["transaction_date"].dt.year
txn_full["month"] = txn_full["transaction_date"].dt.month
txn_full["month_name"] = txn_full["transaction_date"].dt.month_name()
txn_full["quarter"] = txn_full["transaction_date"].dt.quarter
txn_full["day_name"] = txn_full["transaction_date"].dt.day_name()
txn_full["is_weekend"] = txn_full["day_name"].isin(["Saturday", "Sunday"])

# Returns merged
ret_full = ret.merge(prod, on="product_id").merge(stores, on="store_id")
ret_full["return_date"] = pd.to_datetime(ret_full["return_date"], dayfirst=True)

insights = []

def add(topic, insight, data=None):
    entry = {"topic": topic, "insight": insight}
    if data:
        entry["data"] = data
    insights.append(entry)

fmt = lambda x: f"${x:,.2f}"
fmti = lambda x: f"{x:,}"

# ═══════════════════════════════════════════════════════════════
# SECTION 1: OVERALL KPIs
# ═══════════════════════════════════════════════════════════════
total_rev = txn_full["revenue"].sum()
total_cost = txn_full["cost"].sum()
total_profit = txn_full["profit"].sum()
total_txns = len(txn)
total_qty = txn_full["quantity"].sum()
total_customers = cust["customer_id"].nunique()
total_products = prod["product_id"].nunique()
total_stores = stores["store_id"].nunique()
profit_margin = total_profit / total_rev * 100
total_returns = ret["quantity"].sum()
return_rate = total_returns / total_qty * 100

add("Overall KPIs", f"MavenMarket overall performance: Total Revenue: {fmt(total_rev)}, Total Cost: {fmt(total_cost)}, Total Profit: {fmt(total_profit)}, Profit Margin: {profit_margin:.1f}%, Total Transactions: {fmti(total_txns)}, Total Quantity Sold: {fmti(total_qty)}, Total Returns: {fmti(total_returns)}, Return Rate: {return_rate:.1f}%, Total Customers: {fmti(total_customers)}, Total Products: {fmti(total_products)}, Total Stores: {fmti(total_stores)}.")

# ═══════════════════════════════════════════════════════════════
# SECTION 2: CUSTOMER DEMOGRAPHICS (COUNTS, not just revenue)
# ═══════════════════════════════════════════════════════════════

# Gender counts
gender_counts = cust["gender"].value_counts()
add("Customer Count by Gender", f"Customer count by gender: Female (F): {fmti(gender_counts.get('F', 0))}, Male (M): {fmti(gender_counts.get('M', 0))}. Total customers: {fmti(total_customers)}.", {"F": int(gender_counts.get('F', 0)), "M": int(gender_counts.get('M', 0))})

# Marital status counts
marital_counts = cust["marital_status"].value_counts()
add("Customer Count by Marital Status", f"Customer count by marital status: Married (M): {fmti(marital_counts.get('M', 0))}, Single (S): {fmti(marital_counts.get('S', 0))}. Total customers: {fmti(total_customers)}.", {"Married": int(marital_counts.get('M', 0)), "Single": int(marital_counts.get('S', 0))})

# Country counts
country_counts = cust["customer_country"].value_counts()
for c in country_counts.index:
    add(f"Customer Count in {c}", f"Number of customers in {c}: {fmti(country_counts[c])}.", {"country": c, "count": int(country_counts[c])})
add("Customer Count by Country", f"Customer count by country: " + ", ".join([f"{c}: {fmti(v)}" for c, v in country_counts.items()]) + f". Total: {fmti(total_customers)}.")

# Education counts
edu_counts = cust["education"].value_counts()
add("Customer Count by Education", f"Customer count by education level: " + ", ".join([f"{e}: {fmti(v)}" for e, v in edu_counts.items()]) + ".")

# Occupation counts
occ_counts = cust["occupation"].value_counts()
add("Customer Count by Occupation", f"Customer count by occupation: " + ", ".join([f"{o}: {fmti(v)}" for o, v in occ_counts.items()]) + ".")

# Income bracket counts
income_counts = cust["yearly_income"].value_counts()
add("Customer Count by Income Bracket", f"Customer count by yearly income: " + ", ".join([f"{i}: {fmti(v)}" for i, v in income_counts.items()]) + ".")

# Member card counts
member_counts = cust["member_card"].value_counts()
add("Customer Count by Member Card", f"Customer count by member card tier: " + ", ".join([f"{m}: {fmti(v)}" for m, v in member_counts.items()]) + ".")

# Homeowner counts
home_counts = cust["homeowner"].value_counts()
add("Customer Count by Homeowner Status", f"Customer count by homeowner status: Homeowner (Y): {fmti(home_counts.get('Y', 0))}, Non-homeowner (N): {fmti(home_counts.get('N', 0))}.")

# Children stats
children_stats = cust["total_children"].describe()
add("Customer Children Statistics", f"Customer children statistics: Average children per customer: {children_stats['mean']:.1f}, Max children: {int(children_stats['max'])}, Customers with 0 children: {fmti((cust['total_children']==0).sum())}, Customers with 1+ children: {fmti((cust['total_children']>0).sum())}.")

# ═══════════════════════════════════════════════════════════════
# SECTION 3: CROSS-TABS (Marital×Country, Gender×Country, etc.)
# ═══════════════════════════════════════════════════════════════

# Marital status × Country (COUNTS)
for country in cust["customer_country"].unique():
    sub = cust[cust["customer_country"] == country]
    ms = sub["marital_status"].value_counts()
    add(f"Marital Status in {country}", f"In {country}: Married customers: {fmti(ms.get('M', 0))}, Single customers: {fmti(ms.get('S', 0))}, Total: {fmti(len(sub))}.", {"country": country, "Married": int(ms.get('M', 0)), "Single": int(ms.get('S', 0))})

# Gender × Country (COUNTS)
for country in cust["customer_country"].unique():
    sub = cust[cust["customer_country"] == country]
    gc = sub["gender"].value_counts()
    add(f"Gender Distribution in {country}", f"In {country}: Female customers: {fmti(gc.get('F', 0))}, Male customers: {fmti(gc.get('M', 0))}, Total: {fmti(len(sub))}.", {"country": country, "Female": int(gc.get('F', 0)), "Male": int(gc.get('M', 0))})

# Marital × Gender (COUNTS)
mg = cust.groupby(["marital_status", "gender"]).size()
add("Customer Count by Marital Status and Gender", f"Married Female: {fmti(mg.get(('M','F'), 0))}, Married Male: {fmti(mg.get(('M','M'), 0))}, Single Female: {fmti(mg.get(('S','F'), 0))}, Single Male: {fmti(mg.get(('S','M'), 0))}.")

# ═══════════════════════════════════════════════════════════════
# SECTION 4: REVENUE BREAKDOWNS (existing + new cross-tabs)
# ═══════════════════════════════════════════════════════════════

# Revenue by Country
rev_country = txn_full.groupby("customer_country")["revenue"].sum().sort_values(ascending=False)
add("Revenue by Country", f"Revenue by country: " + ", ".join([f"{c}: {fmt(v)}" for c, v in rev_country.items()]) + f". Total: {fmt(total_rev)}.")

# Revenue by country + year
for year in [1997, 1998]:
    ydata = txn_full[txn_full["year"] == year]
    rev_cy = ydata.groupby("customer_country")["revenue"].sum().sort_values(ascending=False)
    add(f"Revenue by Country in {year}", f"Revenue by country in {year}: " + ", ".join([f"{c}: {fmt(v)}" for c, v in rev_cy.items()]) + f". Total for {year}: {fmt(ydata['revenue'].sum())}.")

# Revenue by Year
for year in txn_full["year"].unique():
    yrev = txn_full[txn_full["year"] == year]["revenue"].sum()
    add(f"Revenue in {year}", f"Total revenue in {year}: {fmt(yrev)}.")

# YoY Growth
rev_97 = txn_full[txn_full["year"] == 1997]["revenue"].sum()
rev_98 = txn_full[txn_full["year"] == 1998]["revenue"].sum()
growth = (rev_98 - rev_97) / rev_97 * 100
add("Year-over-Year Revenue Growth", f"Revenue grew {growth:.1f}% from 1997 ({fmt(rev_97)}) to 1998 ({fmt(rev_98)}).")

# Revenue by Gender
rev_gender = txn_full.groupby("gender")["revenue"].sum()
add("Revenue by Gender", f"Revenue by gender: Female (F) {fmt(rev_gender.get('F', 0))}, Male (M) {fmt(rev_gender.get('M', 0))}.")

# Revenue by Marital Status
rev_marital = txn_full.groupby("marital_status")["revenue"].sum()
add("Revenue by Marital Status", f"Revenue by marital status: Married (M) {fmt(rev_marital.get('M', 0))}, Single (S) {fmt(rev_marital.get('S', 0))}.")

# Revenue by Gender × Country
for country in txn_full["customer_country"].unique():
    sub = txn_full[txn_full["customer_country"] == country]
    rg = sub.groupby("gender")["revenue"].sum()
    add(f"Revenue by Gender in {country}", f"Revenue by gender in {country}: Female: {fmt(rg.get('F', 0))}, Male: {fmt(rg.get('M', 0))}.")

# Revenue by Marital Status × Country
for country in txn_full["customer_country"].unique():
    sub = txn_full[txn_full["customer_country"] == country]
    rm = sub.groupby("marital_status")["revenue"].sum()
    add(f"Revenue by Marital Status in {country}", f"Revenue by marital status in {country}: Married: {fmt(rm.get('M', 0))}, Single: {fmt(rm.get('S', 0))}.")

# Revenue by Store Type
rev_stype = txn_full.groupby("store_type")["revenue"].sum().sort_values(ascending=False)
add("Revenue by Store Type", f"Revenue by store type: " + ", ".join([f"{t}: {fmt(v)}" for t, v in rev_stype.items()]) + ".")

# Revenue by Income Bracket
rev_income = txn_full.groupby("yearly_income")["revenue"].sum().sort_values(ascending=False)
add("Revenue by Income Bracket", f"Revenue by customer income: " + ", ".join([f"{i}: {fmt(v)}" for i, v in rev_income.items()]) + ".")

# Revenue by Education
rev_edu = txn_full.groupby("education")["revenue"].sum().sort_values(ascending=False)
add("Revenue by Education Level", f"Revenue by education: " + ", ".join([f"{e}: {fmt(v)}" for e, v in rev_edu.items()]) + ".")

# Revenue by Occupation
rev_occ = txn_full.groupby("occupation")["revenue"].sum().sort_values(ascending=False)
add("Revenue by Occupation", f"Revenue by occupation: " + ", ".join([f"{o}: {fmt(v)}" for o, v in rev_occ.items()]) + ".")

# Revenue by Member Card
rev_member = txn_full.groupby("member_card")["revenue"].sum().sort_values(ascending=False)
add("Revenue by Member Card Tier", f"Revenue by member card: " + ", ".join([f"{m}: {fmt(v)}" for m, v in rev_member.items()]) + ".")

# Revenue per Customer
rev_per_cust = total_rev / total_customers
add("Revenue per Customer", f"Average revenue per customer: {fmt(rev_per_cust)}. Total customers who made purchases: {fmti(txn_full['customer_id'].nunique())}.")

# Weekend vs Weekday
wknd_rev = txn_full[txn_full["is_weekend"]]["revenue"].sum()
wkday_rev = txn_full[~txn_full["is_weekend"]]["revenue"].sum()
add("Weekend vs Weekday Revenue", f"Weekend revenue: {fmt(wknd_rev)} ({wknd_rev/total_rev*100:.1f}% of total). Weekday revenue: {fmt(wkday_rev)} ({wkday_rev/total_rev*100:.1f}% of total).")

# ═══════════════════════════════════════════════════════════════
# SECTION 5: STORE PERFORMANCE
# ═══════════════════════════════════════════════════════════════

# Revenue by Store (all stores)
store_rev = txn_full.groupby("store_name").agg(
    revenue=("revenue", "sum"),
    profit=("profit", "sum"),
    transactions=("quantity", "count"),
    qty_sold=("quantity", "sum")
).sort_values("revenue", ascending=False)
store_rev["profit_margin"] = (store_rev["profit"] / store_rev["revenue"] * 100).round(1)

# Top 5 and Bottom 5 stores
top5 = store_rev.head(5)
add("Top 5 Stores by Revenue", f"Top 5 stores by revenue: " + ", ".join([f"{n} (Rev: {fmt(r['revenue'])}, Profit: {fmt(r['profit'])}, Margin: {r['profit_margin']}%)" for n, r in top5.iterrows()]) + ".")

bot5 = store_rev.tail(5)
add("Bottom 5 Stores by Revenue", f"Bottom 5 stores by revenue: " + ", ".join([f"{n} (Rev: {fmt(r['revenue'])})" for n, r in bot5.iterrows()]) + ".")

# All stores with details
for sname, sdata in store_rev.iterrows():
    add(f"Store Performance: {sname}", f"{sname}: Revenue {fmt(sdata['revenue'])}, Profit {fmt(sdata['profit'])}, Profit Margin {sdata['profit_margin']}%, Transactions: {fmti(int(sdata['transactions']))}, Quantity Sold: {fmti(int(sdata['qty_sold']))}.")

# Store revenue by country
store_country = txn_full.groupby(["store_country", "store_name"])["revenue"].sum().sort_values(ascending=False)
for country in txn_full["store_country"].unique():
    sc = store_country.get(country, pd.Series())
    if len(sc) > 0:
        items = ", ".join([f"{name}: {fmt(rev)}" for name, rev in sc.items()])
        add(f"Stores in {country}", f"Store revenue in {country}: {items}.")

# Sales by Region
rev_region = txn_full.merge(regions, left_on="region_id", right_on="region_id")
rev_by_region = rev_region.groupby("sales_region")["revenue"].sum().sort_values(ascending=False)
add("Revenue by Sales Region", f"Revenue by sales region: " + ", ".join([f"{r}: {fmt(v)}" for r, v in rev_by_region.items()]) + ".")

# Sales by District
rev_by_district = rev_region.groupby("sales_district")["revenue"].sum().sort_values(ascending=False)
add("Revenue by Sales District", f"Revenue by sales district: " + ", ".join([f"{d}: {fmt(v)}" for d, v in rev_by_district.head(15).items()]) + ".")

# Return rate by store
ret_by_store = ret_full.groupby("store_name")["quantity"].sum()
txn_qty_store = txn_full.groupby("store_name")["quantity"].sum()
store_return_rate = (ret_by_store / txn_qty_store * 100).sort_values(ascending=False)
add("Return Rate by Store", f"Stores with highest return rates: " + ", ".join([f"{s}: {r:.1f}%" for s, r in store_return_rate.head(10).items()]) + ".")

# ═══════════════════════════════════════════════════════════════
# SECTION 6: PRODUCT ANALYSIS
# ═══════════════════════════════════════════════════════════════

# Top/bottom products
prod_rev = txn_full.groupby("product_name").agg(revenue=("revenue","sum"), profit=("profit","sum"), qty=("quantity","sum")).sort_values("revenue", ascending=False)

add("Top 10 Products by Revenue", "Top 10 products by revenue: " + ", ".join([f"{n}: {fmt(r['revenue'])}" for n, r in prod_rev.head(10).iterrows()]) + ".")
add("Top 10 Products by Profit", "Top 10 products by profit: " + ", ".join([f"{n}: {fmt(r['profit'])}" for n, r in prod_rev.sort_values("profit", ascending=False).head(10).iterrows()]) + ".")
add("Bottom 10 Products by Profit", "Lowest profit products: " + ", ".join([f"{n}: {fmt(r['profit'])}" for n, r in prod_rev.sort_values("profit").head(10).iterrows()]) + ".")

# Top brands
brand_rev = txn_full.groupby("product_brand").agg(revenue=("revenue","sum"), profit=("profit","sum"), qty=("quantity","sum")).sort_values("revenue", ascending=False)
add("Top 10 Brands by Revenue", "Top 10 brands: " + ", ".join([f"{b}: {fmt(r['revenue'])}" for b, r in brand_rev.head(10).iterrows()]) + ".")

# Product counts
add("Product Statistics", f"Total products: {fmti(total_products)}. Recyclable products: {fmti((prod['recyclable']==1).sum())}. Low-fat products: {fmti((prod['low_fat']==1).sum())}. Average retail price: {fmt(prod['product_retail_price'].mean())}. Average cost: {fmt(prod['product_cost'].mean())}.")

# Brand count
brand_count = prod["product_brand"].nunique()
add("Brand Count", f"Total unique brands: {fmti(brand_count)}.")

# Most returned products
ret_prod = ret_full.groupby("product_name")["quantity"].sum().sort_values(ascending=False)
add("Most Returned Products", "Top 10 most returned products: " + ", ".join([f"{p}: {fmti(q)} units returned" for p, q in ret_prod.head(10).items()]) + ".")

# ═══════════════════════════════════════════════════════════════
# SECTION 7: TIME ANALYSIS
# ═══════════════════════════════════════════════════════════════

# Monthly revenue
monthly = txn_full.groupby(["year", "month", "month_name"])["revenue"].sum().reset_index()
monthly["label"] = monthly["month_name"] + " " + monthly["year"].astype(str)
best_month = monthly.loc[monthly["revenue"].idxmax()]
worst_month = monthly.loc[monthly["revenue"].idxmin()]
add("Monthly Revenue Trend", f"Best revenue month: {best_month['label']} ({fmt(best_month['revenue'])}). Worst revenue month: {worst_month['label']} ({fmt(worst_month['revenue'])}).")

# Quarterly revenue
quarterly = txn_full.groupby(["year", "quarter"])["revenue"].sum()
q_lines = ", ".join([f"{y} Q{q}: {fmt(v)}" for (y, q), v in quarterly.items()])
add("Quarterly Revenue", f"Quarterly revenue: {q_lines}.")

# Revenue by day of week
dow_rev = txn_full.groupby("day_name")["revenue"].sum().sort_values(ascending=False)
add("Revenue by Day of Week", f"Revenue by day of week: " + ", ".join([f"{d}: {fmt(v)}" for d, v in dow_rev.items()]) + ".")

# Monthly revenue 1998 details
m98 = txn_full[txn_full["year"]==1998].groupby("month_name")["revenue"].sum()
add("Monthly Revenue 1998", f"Monthly revenue in 1998: " + ", ".join([f"{m}: {fmt(v)}" for m, v in m98.items()]) + ".")

# Monthly revenue 1997 details
m97 = txn_full[txn_full["year"]==1997].groupby("month_name")["revenue"].sum()
add("Monthly Revenue 1997", f"Monthly revenue in 1997: " + ", ".join([f"{m}: {fmt(v)}" for m, v in m97.items()]) + ".")

# ═══════════════════════════════════════════════════════════════
# SECTION 8: PROFIT BREAKDOWNS
# ═══════════════════════════════════════════════════════════════

# Profit by Country
profit_country = txn_full.groupby("customer_country")["profit"].sum().sort_values(ascending=False)
add("Profit by Country", f"Profit by country: " + ", ".join([f"{c}: {fmt(v)}" for c, v in profit_country.items()]) + f". Total profit: {fmt(total_profit)}.")

# Profit by Store Type
profit_stype = txn_full.groupby("store_type")["profit"].sum().sort_values(ascending=False)
add("Profit by Store Type", f"Profit by store type: " + ", ".join([f"{t}: {fmt(v)}" for t, v in profit_stype.items()]) + ".")

# Profit by Year
for year in [1997, 1998]:
    yp = txn_full[txn_full["year"]==year]["profit"].sum()
    add(f"Profit in {year}", f"Total profit in {year}: {fmt(yp)}.")

# Profit by Gender
profit_gender = txn_full.groupby("gender")["profit"].sum()
add("Profit by Gender", f"Profit by gender: Female: {fmt(profit_gender.get('F',0))}, Male: {fmt(profit_gender.get('M',0))}.")

# ═══════════════════════════════════════════════════════════════
# SECTION 9: TRANSACTION COUNTS
# ═══════════════════════════════════════════════════════════════

# Transactions by country
txn_country = txn_full.groupby("customer_country").size().sort_values(ascending=False)
add("Transactions by Country", f"Transaction count by country: " + ", ".join([f"{c}: {fmti(v)}" for c, v in txn_country.items()]) + ".")

# Transactions by year
txn_year = txn_full.groupby("year").size()
add("Transactions by Year", f"Transaction count by year: " + ", ".join([f"{y}: {fmti(v)}" for y, v in txn_year.items()]) + ".")

# Quantity sold by country
qty_country = txn_full.groupby("customer_country")["quantity"].sum().sort_values(ascending=False)
add("Quantity Sold by Country", f"Quantity sold by country: " + ", ".join([f"{c}: {fmti(v)}" for c, v in qty_country.items()]) + ".")

# ═══════════════════════════════════════════════════════════════
# SECTION 10: RETURNS ANALYSIS
# ═══════════════════════════════════════════════════════════════

# Returns by country
ret_full2 = ret.merge(stores, on="store_id")
ret_country = ret_full2.groupby("store_country")["quantity"].sum().sort_values(ascending=False)
add("Returns by Country", f"Returns by country: " + ", ".join([f"{c}: {fmti(v)} units" for c, v in ret_country.items()]) + f". Total returns: {fmti(total_returns)}.")

# Returns by year
ret["return_date"] = pd.to_datetime(ret["return_date"], dayfirst=True)
ret_year = ret.groupby(ret["return_date"].dt.year)["quantity"].sum()
add("Returns by Year", f"Returns by year: " + ", ".join([f"{y}: {fmti(v)} units" for y, v in ret_year.items()]) + ".")

# ═══════════════════════════════════════════════════════════════
# SECTION 11: STORE DETAILS
# ═══════════════════════════════════════════════════════════════

for _, s in stores.iterrows():
    add(f"Store Details: {s['store_name']}", f"{s['store_name']}: Type: {s['store_type']}, City: {s['store_city']}, State: {s['store_state']}, Country: {s['store_country']}, Total sqft: {fmti(s['total_sqft'])}, Grocery sqft: {fmti(s['grocery_sqft'])}.")

# ═══════════════════════════════════════════════════════════════
# SAVE
# ═══════════════════════════════════════════════════════════════

os.makedirs(OUT, exist_ok=True)
output_path = os.path.join(OUT, "insights.jsonl")
with open(output_path, "w") as f:
    for ins in insights:
        f.write(json.dumps(ins) + "\n")

print(f"\nDone! Generated {len(insights)} insights → {output_path}")

# Print summary by section
topics = {}
for ins in insights:
    prefix = ins["topic"].split(":")[0].split(" in ")[0].split(" by ")[0].strip()
    topics[prefix] = topics.get(prefix, 0) + 1
print("\nBreakdown:")
for t, c in sorted(topics.items(), key=lambda x: -x[1]):
    print(f"  {t}: {c}")

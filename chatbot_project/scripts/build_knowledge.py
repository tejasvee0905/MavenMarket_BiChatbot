import json, os
import pandas as pd

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
_REPO_ROOT = os.path.dirname(_PROJECT_ROOT)
OUT = os.path.join(_PROJECT_ROOT, "knowledge", "active")
DATA = os.path.join(_REPO_ROOT, "data", "csvs")

# ═══════════════════════════════════════════════════════════════
# 1. SCHEMA — Table definitions extracted from CSVs
# ═══════════════════════════════════════════════════════════════

schema = {
    "model_name": "MavenMarket",
    "tables": {
        "MavenMarket_Calendar": {
            "role": "Dimension",
            "grain": "One row per date (1997-1998)",
            "row_count": 730,
            "columns": {
                "transaction_date": {"type": "date", "description": "Calendar date (DD-MM-YYYY), primary key"},
                "year": {"type": "int", "description": "Year (1997 or 1998)"},
                "month_number": {"type": "int", "description": "Month number 1-12"},
                "month_name": {"type": "string", "description": "Month name (January, February, ...)"},
                "quarter": {"type": "int", "description": "Quarter number 1-4"},
                "day_of_week": {"type": "string", "description": "Day name (Monday, Tuesday, ...)"},
                "day_of_month": {"type": "int", "description": "Day of month 1-31"},
                "week_of_year": {"type": "int", "description": "Week number 1-52"},
                "start_of_week": {"type": "date", "description": "First date of the week"},
                "start_of_month": {"type": "date", "description": "First date of the month"}
            }
        },
        "MavenMarket_Customers": {
            "role": "Dimension",
            "grain": "One row per customer",
            "row_count": 10281,
            "columns": {
                "customer_id": {"type": "int", "description": "Unique customer identifier, primary key"},
                "customer_acct_num": {"type": "string", "description": "Customer account number"},
                "first_name": {"type": "string", "description": "Customer first name"},
                "last_name": {"type": "string", "description": "Customer last name"},
                "full_name": {"type": "string", "description": "Full name (first + last)"},
                "customer_address": {"type": "string", "description": "Street address"},
                "customer_city": {"type": "string", "description": "City"},
                "customer_state_province": {"type": "string", "description": "State or province"},
                "customer_postal_code": {"type": "string", "description": "Postal/zip code"},
                "customer_country": {"type": "string", "description": "Country (USA, Canada, Mexico)"},
                "birthdate": {"type": "date", "description": "Customer birth date"},
                "marital_status": {"type": "string", "description": "Marital status (M=Married, S=Single)"},
                "yearly_income": {"type": "string", "description": "Income bracket ($10K-$30K, $30K-$50K, etc.)"},
                "gender": {"type": "string", "description": "Gender (M=Male, F=Female)"},
                "total_children": {"type": "int", "description": "Total number of children"},
                "num_children_at_home": {"type": "int", "description": "Number of children living at home"},
                "education": {"type": "string", "description": "Education level (Partial High School, High School Degree, Partial College, Bachelors Degree, Graduate Degree)"},
                "acct_open_date": {"type": "date", "description": "Account opening date"},
                "member_card": {"type": "string", "description": "Loyalty card tier (Normal, Bronze, Silver, Gold)"},
                "occupation": {"type": "string", "description": "Occupation (Professional, Skilled Manual, Manual, Clerical, Management)"},
                "homeowner": {"type": "string", "description": "Homeowner status (Y/N)"}
            }
        },
        "MavenMarket_Products": {
            "role": "Dimension",
            "grain": "One row per product",
            "row_count": 1560,
            "columns": {
                "product_id": {"type": "int", "description": "Unique product identifier, primary key"},
                "product_brand": {"type": "string", "description": "Brand name"},
                "product_name": {"type": "string", "description": "Full product name"},
                "product_sku": {"type": "string", "description": "Stock keeping unit"},
                "product_retail_price": {"type": "float", "description": "Retail price in dollars"},
                "product_cost": {"type": "float", "description": "Cost price in dollars"},
                "product_weight": {"type": "float", "description": "Product weight"},
                "recyclable": {"type": "int", "description": "Is recyclable (0=No, 1=Yes)"},
                "low_fat": {"type": "int", "description": "Is low fat (0=No, 1=Yes)"},
                "price_margin": {"type": "float", "description": "Price margin (retail - cost)"}
            },
            "calculated_columns": {
                "Price Tier": "Categorizes products into price tiers (Low, Mid, High) based on product_retail_price"
            }
        },
        "MavenMarket_Stores": {
            "role": "Dimension",
            "grain": "One row per store",
            "row_count": 24,
            "columns": {
                "store_id": {"type": "int", "description": "Unique store identifier, primary key"},
                "region_id": {"type": "int", "description": "Foreign key to Regions table"},
                "store_type": {"type": "string", "description": "Store type (Supermarket, Deluxe Supermarket, Gourmet Supermarket, Small Grocery, Mid-Size Grocery)"},
                "store_name": {"type": "string", "description": "Store name (Store 1, Store 2, ...)"},
                "store_street_address": {"type": "string", "description": "Street address"},
                "store_city": {"type": "string", "description": "City"},
                "store_state": {"type": "string", "description": "State"},
                "store_country": {"type": "string", "description": "Country (USA, Canada, Mexico)"},
                "store_phone": {"type": "string", "description": "Phone number"},
                "first_opened_date": {"type": "date", "description": "Date store first opened"},
                "last_remodel_date": {"type": "date", "description": "Date of last remodel"},
                "total_sqft": {"type": "int", "description": "Total square footage"},
                "grocery_sqft": {"type": "int", "description": "Grocery section square footage"}
            }
        },
        "MavenMarket_Regions": {
            "role": "Dimension",
            "grain": "One row per region",
            "row_count": 109,
            "columns": {
                "region_id": {"type": "int", "description": "Unique region identifier, primary key"},
                "sales_district": {"type": "string", "description": "Sales district name"},
                "sales_region": {"type": "string", "description": "Sales region name"}
            }
        },
        "MavenMarket_Transactions": {
            "role": "Fact",
            "grain": "One row per transaction line item",
            "row_count": 269720,
            "columns": {
                "transaction_date": {"type": "date", "description": "Transaction date, foreign key to Calendar"},
                "stock_date": {"type": "date", "description": "Stock/inventory date"},
                "product_id": {"type": "int", "description": "Foreign key to Products"},
                "customer_id": {"type": "int", "description": "Foreign key to Customers"},
                "store_id": {"type": "int", "description": "Foreign key to Stores"},
                "quantity": {"type": "int", "description": "Quantity purchased"}
            }
        },
        "MavenMarket_Returns": {
            "role": "Fact",
            "grain": "One row per return line item",
            "row_count": 7087,
            "columns": {
                "return_date": {"type": "date", "description": "Return date, foreign key to Calendar"},
                "product_id": {"type": "int", "description": "Foreign key to Products"},
                "store_id": {"type": "int", "description": "Foreign key to Stores"},
                "quantity": {"type": "int", "description": "Quantity returned"}
            }
        },
        "MavenMarket_Measures": {
            "role": "Measure Table",
            "grain": "DAX measures only, no rows",
            "row_count": 0,
            "description": "Dedicated table for storing all DAX measures"
        }
    },
    "relationships": [
        {"from_table": "MavenMarket_Transactions", "from_column": "transaction_date", "to_table": "MavenMarket_Calendar", "to_column": "transaction_date", "cardinality": "Many-to-One", "active": True},
        {"from_table": "MavenMarket_Transactions", "from_column": "customer_id", "to_table": "MavenMarket_Customers", "to_column": "customer_id", "cardinality": "Many-to-One", "active": True},
        {"from_table": "MavenMarket_Transactions", "from_column": "product_id", "to_table": "MavenMarket_Products", "to_column": "product_id", "cardinality": "Many-to-One", "active": True},
        {"from_table": "MavenMarket_Transactions", "from_column": "store_id", "to_table": "MavenMarket_Stores", "to_column": "store_id", "cardinality": "Many-to-One", "active": True},
        {"from_table": "MavenMarket_Returns", "from_column": "return_date", "to_table": "MavenMarket_Calendar", "to_column": "transaction_date", "cardinality": "Many-to-One", "active": False, "note": "Inactive relationship, activated via USERELATIONSHIP in DAX"},
        {"from_table": "MavenMarket_Returns", "from_column": "product_id", "to_table": "MavenMarket_Products", "to_column": "product_id", "cardinality": "Many-to-One", "active": True},
        {"from_table": "MavenMarket_Returns", "from_column": "store_id", "to_table": "MavenMarket_Stores", "to_column": "store_id", "cardinality": "Many-to-One", "active": True},
        {"from_table": "MavenMarket_Stores", "from_column": "region_id", "to_table": "MavenMarket_Regions", "to_column": "region_id", "cardinality": "Many-to-One", "active": True}
    ]
}

with open(os.path.join(OUT, "schema.json"), "w") as f:
    json.dump(schema, f, indent=2)
print("Saved schema.json")

# ═══════════════════════════════════════════════════════════════
# 2. MEASURES — Reconstructed from dashboard visual analysis
# ═══════════════════════════════════════════════════════════════

measures = [
    {
        "name": "Total Revenue",
        "table": "MavenMarket_Measures",
        "expression": "Total Revenue = SUMX(MavenMarket_Transactions, MavenMarket_Transactions[quantity] * RELATED(MavenMarket_Products[product_retail_price]))",
        "description": "Total revenue calculated as sum of (quantity * retail price) across all transactions",
        "format": "Currency",
        "used_in_pages": ["Page 1", "Page 2", "Page 3"],
        "used_in_visuals": ["cardVisual", "areaChart", "azureMap", "columnChart", "donutChart", "pieChart", "tableEx", "barChart"]
    },
    {
        "name": "Total Cost",
        "table": "MavenMarket_Measures",
        "expression": "Total Cost = SUMX(MavenMarket_Transactions, MavenMarket_Transactions[quantity] * RELATED(MavenMarket_Products[product_cost]))",
        "description": "Total cost calculated as sum of (quantity * cost price) across all transactions",
        "format": "Currency",
        "used_in_pages": ["Page 2"],
        "used_in_visuals": ["tableEx"]
    },
    {
        "name": "Total Profit",
        "table": "MavenMarket_Measures",
        "expression": "Total Profit = [Total Revenue] - [Total Cost]",
        "description": "Total profit = revenue minus cost",
        "format": "Currency",
        "used_in_pages": ["Page 1", "Page 2"],
        "used_in_visuals": ["cardVisual", "tableEx"]
    },
    {
        "name": "Profit Margin",
        "table": "MavenMarket_Measures",
        "expression": "Profit Margin = DIVIDE([Total Profit], [Total Revenue], 0)",
        "description": "Profit margin percentage = total profit / total revenue",
        "format": "Percentage",
        "used_in_pages": ["Page 1", "Page 2"],
        "used_in_visuals": ["cardVisual", "tableEx"]
    },
    {
        "name": "Total Transactions",
        "table": "MavenMarket_Measures",
        "expression": "Total Transactions = COUNTROWS(MavenMarket_Transactions)",
        "description": "Count of all transaction rows",
        "format": "Whole Number",
        "used_in_pages": ["Page 1", "Page 2"],
        "used_in_visuals": ["cardVisual", "lineChart"]
    },
    {
        "name": "Total Returns",
        "table": "MavenMarket_Measures",
        "expression": "Total Returns = SUM(MavenMarket_Returns[quantity])",
        "description": "Total quantity of returned items",
        "format": "Whole Number",
        "used_in_pages": ["Page 2"],
        "used_in_visuals": ["cardVisual"]
    },
    {
        "name": "Return Rate",
        "table": "MavenMarket_Measures",
        "expression": "Return Rate = DIVIDE([Total Returns], [Total Quantity Sold], 0)",
        "description": "Return rate = total returns / total quantity sold",
        "format": "Percentage",
        "used_in_pages": ["Page 1"],
        "used_in_visuals": ["cardVisual"]
    },
    {
        "name": "Total Quantity Sold",
        "table": "MavenMarket_Measures",
        "expression": "Total Quantity Sold = SUM(MavenMarket_Transactions[quantity])",
        "description": "Total quantity of items sold across all transactions",
        "format": "Whole Number",
        "used_in_pages": ["Page 2", "Page 3"],
        "used_in_visuals": ["cardVisual"]
    },
    {
        "name": "Total Customers",
        "table": "MavenMarket_Measures",
        "expression": "Total Customers = DISTINCTCOUNT(MavenMarket_Transactions[customer_id])",
        "description": "Count of distinct customers who made at least one transaction",
        "format": "Whole Number",
        "used_in_pages": ["Page 3"],
        "used_in_visuals": ["cardVisual", "tableEx", "donutChart"]
    },
    {
        "name": "Total Products Sold",
        "table": "MavenMarket_Measures",
        "expression": "Total Products Sold = DISTINCTCOUNT(MavenMarket_Transactions[product_id])",
        "description": "Count of distinct products sold",
        "format": "Whole Number",
        "used_in_pages": ["Page 2"],
        "used_in_visuals": ["cardVisual"]
    },
    {
        "name": "Store Count",
        "table": "MavenMarket_Measures",
        "expression": "Store Count = DISTINCTCOUNT(MavenMarket_Transactions[store_id])",
        "description": "Count of distinct stores with transactions",
        "format": "Whole Number",
        "used_in_pages": ["Page 2"],
        "used_in_visuals": ["cardVisual"]
    },
    {
        "name": "Revenue per Customer",
        "table": "MavenMarket_Measures",
        "expression": "Revenue per Customer = DIVIDE([Total Revenue], [Total Customers], 0)",
        "description": "Average revenue per customer",
        "format": "Currency",
        "used_in_pages": ["Page 3"],
        "used_in_visuals": ["cardVisual"]
    },
    {
        "name": "Transactions per Customer",
        "table": "MavenMarket_Measures",
        "expression": "Transactions per Customer = DIVIDE([Total Transactions], [Total Customers], 0)",
        "description": "Average number of transactions per customer",
        "format": "Decimal Number",
        "used_in_pages": ["Page 3"],
        "used_in_visuals": ["cardVisual", "pieChart"]
    },
    {
        "name": "Weekend Revenue",
        "table": "MavenMarket_Measures",
        "expression": "Weekend Revenue = CALCULATE([Total Revenue], MavenMarket_Calendar[day_of_week] IN {\"Saturday\", \"Sunday\"})",
        "description": "Total revenue from weekend transactions (Saturday and Sunday)",
        "format": "Currency",
        "used_in_pages": ["Page 2"],
        "used_in_visuals": ["cardVisual"]
    },
    {
        "name": "Revenue per Sqft",
        "table": "MavenMarket_Measures",
        "expression": "Revenue per Sqft = DIVIDE([Total Revenue], SUM(MavenMarket_Stores[total_sqft]), 0)",
        "description": "Revenue per square foot of store space",
        "format": "Currency",
        "used_in_pages": ["Page 2"],
        "used_in_visuals": ["cardVisual"]
    },
    {
        "name": "Revenue Last 60 Days",
        "table": "MavenMarket_Measures",
        "expression": "Revenue Last 60 Days = CALCULATE([Total Revenue], DATESINPERIOD(MavenMarket_Calendar[transaction_date], MAX(MavenMarket_Calendar[transaction_date]), -60, DAY))",
        "description": "Rolling 60-day revenue from the last date in context",
        "format": "Currency",
        "used_in_pages": ["Page 1"],
        "used_in_visuals": ["donutChart"]
    }
]

with open(os.path.join(OUT, "measures.json"), "w") as f:
    json.dump(measures, f, indent=2)
print(f"Saved measures.json — {len(measures)} measures")

# ═══════════════════════════════════════════════════════════════
# 3. DASHBOARD PAGES — from visual extraction
# ═══════════════════════════════════════════════════════════════

dashboard_summary = {
    "report_name": "MavenMarket Dashboard (tej.pbix)",
    "theme": "AccessibleCityPark",
    "total_pages": 3,
    "total_visuals": 31,
    "pages": [
        {
            "name": "Page 1 — Executive Overview",
            "ordinal": 0,
            "visual_count": 9,
            "purpose": "High-level KPIs, revenue trends, geographic performance, and product analysis",
            "visuals": [
                {"type": "cardVisual", "shows": "KPI cards: Total Profit, Total Revenue, Profit Margin, Total Transactions, Return Rate"},
                {"type": "areaChart", "shows": "Monthly revenue trend by year (1997 vs 1998)"},
                {"type": "azureMap", "shows": "Revenue by country on a map, filterable by year"},
                {"type": "columnChart", "shows": "Top products by revenue"},
                {"type": "donutChart", "shows": "Revenue by Price Tier (Low/Mid/High)"},
                {"type": "pieChart", "shows": "Revenue by country (USA, Canada, Mexico)"},
                {"type": "donutChart", "shows": "Revenue Last 60 Days by month"},
                {"type": "slicer", "shows": "Year filter"},
                {"type": "slicer", "shows": "Country filter"}
            ],
            "slicers": ["Year", "Country"]
        },
        {
            "name": "Page 2 — Store Performance",
            "ordinal": 1,
            "visual_count": 9,
            "purpose": "Store-level analysis: store-by-store metrics, regional comparison, store types",
            "visuals": [
                {"type": "cardVisual", "shows": "KPI cards: Store Count, Weekend Revenue, Revenue per Sqft, Total Quantity Sold, Total Returns, Total Products Sold"},
                {"type": "tableEx", "shows": "Store detail table: store name, revenue, profit, cost, profit margin"},
                {"type": "barChart", "shows": "Revenue by sales region"},
                {"type": "donutChart", "shows": "Revenue by store type"},
                {"type": "lineChart", "shows": "Transactions by store"},
                {"type": "azureMap", "shows": "Store locations on map"},
                {"type": "slicer", "shows": "Country filter"},
                {"type": "slicer", "shows": "Sales region filter"},
                {"type": "slicer", "shows": "Store type filter"}
            ],
            "slicers": ["Country", "Sales Region", "Store Type"]
        },
        {
            "name": "Page 3 — Customer Analysis",
            "ordinal": 2,
            "visual_count": 13,
            "purpose": "Customer demographics and behavior: gender, income, education, occupation, member cards",
            "visuals": [
                {"type": "cardVisual", "shows": "KPI cards: Total Customers, Transactions per Customer, Revenue per Customer, Total Quantity Sold"},
                {"type": "pieChart", "shows": "Transactions per customer by gender"},
                {"type": "tableEx", "shows": "Customer count by gender"},
                {"type": "pieChart", "shows": "Revenue by gender"},
                {"type": "barChart", "shows": "Revenue by yearly income bracket, split by gender"},
                {"type": "donutChart", "shows": "Revenue by education level"},
                {"type": "donutChart", "shows": "Revenue by occupation"},
                {"type": "donutChart", "shows": "Customer count by country"},
                {"type": "donutChart", "shows": "Revenue by marital status"},
                {"type": "cardVisual", "shows": "Transactions per Customer highlight"},
                {"type": "slicer", "shows": "Education filter"},
                {"type": "slicer", "shows": "Member card filter"},
                {"type": "slicer", "shows": "Occupation filter"}
            ],
            "slicers": ["Education", "Member Card", "Occupation"]
        }
    ]
}

with open(os.path.join(OUT, "dashboard_summary.json"), "w") as f:
    json.dump(dashboard_summary, f, indent=2)
print("Saved dashboard_summary.json")

# ═══════════════════════════════════════════════════════════════
# 4. PRE-COMPUTED INSIGHTS — from actual CSV data
# ═══════════════════════════════════════════════════════════════

print("\nComputing insights from CSV data...")

txn = pd.read_csv(os.path.join(DATA, "MavenMarket_Transactions.csv"), parse_dates=["transaction_date"], dayfirst=True)
products = pd.read_csv(os.path.join(DATA, "MavenMarket_Products.csv"))
stores = pd.read_csv(os.path.join(DATA, "MavenMarket_Stores.csv"))
customers = pd.read_csv(os.path.join(DATA, "MavenMarket_Customers.csv"))
regions = pd.read_csv(os.path.join(DATA, "MavenMarket_Regions.csv"))
returns = pd.read_csv(os.path.join(DATA, "MavenMarket_Returns.csv"), parse_dates=["return_date"], dayfirst=True)
calendar = pd.read_csv(os.path.join(DATA, "MavenMarket_Calendar.csv"), parse_dates=["transaction_date"], dayfirst=True)

# Enrich transactions
txn_e = txn.merge(products, on="product_id").merge(stores, on="store_id").merge(regions, on="region_id")
txn_e["revenue"] = txn_e["quantity"] * txn_e["product_retail_price"]
txn_e["cost"] = txn_e["quantity"] * txn_e["product_cost"]
txn_e["profit"] = txn_e["revenue"] - txn_e["cost"]
txn_e["year"] = txn_e["transaction_date"].dt.year
txn_e["month"] = txn_e["transaction_date"].dt.to_period("M")
txn_e["month_name"] = txn_e["transaction_date"].dt.strftime("%B %Y")

insights = []

# --- Overall KPIs ---
total_rev = txn_e["revenue"].sum()
total_cost = txn_e["cost"].sum()
total_profit = txn_e["profit"].sum()
total_txn = len(txn)
total_qty = txn["quantity"].sum()
total_returns = returns["quantity"].sum()
return_rate = total_returns / total_qty * 100
total_customers = txn["customer_id"].nunique()
profit_margin = total_profit / total_rev * 100

insights.append({
    "topic": "Overall KPIs",
    "insight": f"MavenMarket overall performance: Total Revenue ${total_rev:,.2f}, Total Cost ${total_cost:,.2f}, Total Profit ${total_profit:,.2f}, Profit Margin {profit_margin:.1f}%, Total Transactions {total_txn:,}, Total Quantity Sold {total_qty:,}, Total Returns {total_returns:,}, Return Rate {return_rate:.1f}%, Total Customers {total_customers:,}.",
    "data": {"total_revenue": round(total_rev, 2), "total_cost": round(total_cost, 2), "total_profit": round(total_profit, 2), "profit_margin_pct": round(profit_margin, 1), "total_transactions": total_txn, "total_quantity_sold": int(total_qty), "total_returns": int(total_returns), "return_rate_pct": round(return_rate, 1), "total_customers": total_customers}
})

# --- Revenue by Year ---
rev_by_year = txn_e.groupby("year")["revenue"].sum()
for yr, rev in rev_by_year.items():
    insights.append({
        "topic": "Revenue by Year",
        "insight": f"In {yr}, total revenue was ${rev:,.2f}.",
        "data": {"year": int(yr), "revenue": round(rev, 2)}
    })
if len(rev_by_year) == 2:
    yoy = (rev_by_year.iloc[1] - rev_by_year.iloc[0]) / rev_by_year.iloc[0] * 100
    insights.append({
        "topic": "Year-over-Year Growth",
        "insight": f"Revenue grew {yoy:.1f}% from {rev_by_year.index[0]} to {rev_by_year.index[1]} (${rev_by_year.iloc[0]:,.2f} → ${rev_by_year.iloc[1]:,.2f}).",
        "data": {"yoy_growth_pct": round(yoy, 1)}
    })

# --- Monthly Revenue Trend ---
monthly_rev = txn_e.groupby("month_name")["revenue"].sum().reset_index()
monthly_rev.columns = ["month", "revenue"]
best_month = monthly_rev.loc[monthly_rev["revenue"].idxmax()]
worst_month = monthly_rev.loc[monthly_rev["revenue"].idxmin()]
insights.append({
    "topic": "Monthly Revenue Trend",
    "insight": f"Best revenue month: {best_month['month']} (${best_month['revenue']:,.2f}). Lowest revenue month: {worst_month['month']} (${worst_month['revenue']:,.2f}).",
    "data": {"best_month": best_month["month"], "best_revenue": round(best_month["revenue"], 2), "worst_month": worst_month["month"], "worst_revenue": round(worst_month["revenue"], 2)}
})

# --- Top 10 Products by Revenue ---
top_products = txn_e.groupby("product_name")["revenue"].sum().sort_values(ascending=False).head(10)
insights.append({
    "topic": "Top 10 Products by Revenue",
    "insight": "Top 10 products by revenue: " + ", ".join([f"{name} (${rev:,.2f})" for name, rev in top_products.items()]),
    "data": {name: round(rev, 2) for name, rev in top_products.items()}
})

# --- Top 10 Products by Profit ---
top_profit_products = txn_e.groupby("product_name")["profit"].sum().sort_values(ascending=False).head(10)
insights.append({
    "topic": "Top 10 Products by Profit",
    "insight": "Top 10 products by profit: " + ", ".join([f"{name} (${p:,.2f})" for name, p in top_profit_products.items()]),
    "data": {name: round(p, 2) for name, p in top_profit_products.items()}
})

# --- Bottom 10 Products by Profit (loss leaders) ---
bottom_products = txn_e.groupby("product_name")["profit"].sum().sort_values().head(10)
insights.append({
    "topic": "Bottom 10 Products by Profit",
    "insight": "Lowest profit products: " + ", ".join([f"{name} (${p:,.2f})" for name, p in bottom_products.items()]),
    "data": {name: round(p, 2) for name, p in bottom_products.items()}
})

# --- Revenue by Country ---
rev_country = txn_e.groupby("store_country")["revenue"].sum().sort_values(ascending=False)
insights.append({
    "topic": "Revenue by Country",
    "insight": "Revenue by country: " + ", ".join([f"{c}: ${r:,.2f}" for c, r in rev_country.items()]),
    "data": {c: round(r, 2) for c, r in rev_country.items()}
})

# --- Revenue by Store Type ---
rev_store_type = txn_e.groupby("store_type")["revenue"].sum().sort_values(ascending=False)
insights.append({
    "topic": "Revenue by Store Type",
    "insight": "Revenue by store type: " + ", ".join([f"{t}: ${r:,.2f}" for t, r in rev_store_type.items()]),
    "data": {t: round(r, 2) for t, r in rev_store_type.items()}
})

# --- Top Stores by Revenue ---
top_stores = txn_e.groupby("store_name").agg(
    revenue=("revenue", "sum"),
    profit=("profit", "sum"),
    transactions=("quantity", "count")
).sort_values("revenue", ascending=False)
insights.append({
    "topic": "Top Stores by Revenue",
    "insight": "Top 5 stores: " + ", ".join([f"{name} (Rev: ${row['revenue']:,.2f}, Profit: ${row['profit']:,.2f})" for name, row in top_stores.head(5).iterrows()]),
    "data": {name: {"revenue": round(row["revenue"], 2), "profit": round(row["profit"], 2)} for name, row in top_stores.head(10).iterrows()}
})

# --- Revenue by Region ---
rev_region = txn_e.groupby("sales_region")["revenue"].sum().sort_values(ascending=False)
insights.append({
    "topic": "Revenue by Sales Region",
    "insight": "Revenue by sales region: " + ", ".join([f"{r}: ${v:,.2f}" for r, v in rev_region.items()]),
    "data": {r: round(v, 2) for r, v in rev_region.items()}
})

# --- Customer Demographics: Gender ---
cust_txn = txn_e.merge(customers[["customer_id", "gender", "yearly_income", "education", "occupation", "member_card", "marital_status", "customer_country"]], on="customer_id")
rev_gender = cust_txn.groupby("gender")["revenue"].sum()
insights.append({
    "topic": "Revenue by Gender",
    "insight": f"Revenue by gender: Female (F) ${rev_gender.get('F', 0):,.2f}, Male (M) ${rev_gender.get('M', 0):,.2f}.",
    "data": {g: round(r, 2) for g, r in rev_gender.items()}
})

# --- Customer Demographics: Income ---
rev_income = cust_txn.groupby("yearly_income")["revenue"].sum().sort_values(ascending=False)
insights.append({
    "topic": "Revenue by Income Bracket",
    "insight": "Revenue by customer income: " + ", ".join([f"{inc}: ${r:,.2f}" for inc, r in rev_income.items()]),
    "data": {inc: round(r, 2) for inc, r in rev_income.items()}
})

# --- Customer Demographics: Education ---
rev_education = cust_txn.groupby("education")["revenue"].sum().sort_values(ascending=False)
insights.append({
    "topic": "Revenue by Education Level",
    "insight": "Revenue by education: " + ", ".join([f"{e}: ${r:,.2f}" for e, r in rev_education.items()]),
    "data": {e: round(r, 2) for e, r in rev_education.items()}
})

# --- Customer Demographics: Occupation ---
rev_occupation = cust_txn.groupby("occupation")["revenue"].sum().sort_values(ascending=False)
insights.append({
    "topic": "Revenue by Occupation",
    "insight": "Revenue by occupation: " + ", ".join([f"{o}: ${r:,.2f}" for o, r in rev_occupation.items()]),
    "data": {o: round(r, 2) for o, r in rev_occupation.items()}
})

# --- Member Card Analysis ---
rev_member = cust_txn.groupby("member_card")["revenue"].sum().sort_values(ascending=False)
insights.append({
    "topic": "Revenue by Member Card Tier",
    "insight": "Revenue by member card: " + ", ".join([f"{m}: ${r:,.2f}" for m, r in rev_member.items()]),
    "data": {m: round(r, 2) for m, r in rev_member.items()}
})

# --- Return Analysis by Store ---
ret_store = returns.merge(stores, on="store_id")
ret_by_store = ret_store.groupby("store_name")["quantity"].sum().sort_values(ascending=False)
sales_by_store = txn.merge(stores, on="store_id").groupby("store_name")["quantity"].sum()
rr_by_store = (ret_by_store / sales_by_store * 100).sort_values(ascending=False).dropna()
insights.append({
    "topic": "Return Rate by Store",
    "insight": "Stores with highest return rates: " + ", ".join([f"{s}: {r:.1f}%" for s, r in rr_by_store.head(5).items()]),
    "data": {s: round(r, 1) for s, r in rr_by_store.items()}
})

# --- Return Analysis by Product (top returned) ---
ret_prod = returns.merge(products, on="product_id")
ret_by_prod = ret_prod.groupby("product_name")["quantity"].sum().sort_values(ascending=False).head(10)
insights.append({
    "topic": "Most Returned Products",
    "insight": "Top 10 most returned products: " + ", ".join([f"{p} ({q} units)" for p, q in ret_by_prod.items()]),
    "data": {p: int(q) for p, q in ret_by_prod.items()}
})

# --- Brand Analysis ---
rev_brand = txn_e.groupby("product_brand")["revenue"].sum().sort_values(ascending=False).head(10)
insights.append({
    "topic": "Top 10 Brands by Revenue",
    "insight": "Top 10 brands: " + ", ".join([f"{b}: ${r:,.2f}" for b, r in rev_brand.items()]),
    "data": {b: round(r, 2) for b, r in rev_brand.items()}
})

# --- Weekend vs Weekday ---
txn_e["day_of_week"] = txn_e["transaction_date"].dt.day_name()
txn_e["is_weekend"] = txn_e["day_of_week"].isin(["Saturday", "Sunday"])
weekend_rev = txn_e[txn_e["is_weekend"]]["revenue"].sum()
weekday_rev = txn_e[~txn_e["is_weekend"]]["revenue"].sum()
insights.append({
    "topic": "Weekend vs Weekday Revenue",
    "insight": f"Weekend revenue: ${weekend_rev:,.2f} ({weekend_rev/total_rev*100:.1f}% of total). Weekday revenue: ${weekday_rev:,.2f} ({weekday_rev/total_rev*100:.1f}% of total).",
    "data": {"weekend_revenue": round(weekend_rev, 2), "weekday_revenue": round(weekday_rev, 2), "weekend_pct": round(weekend_rev/total_rev*100, 1)}
})

# --- Revenue per Customer Segment ---
rev_per_cust = cust_txn.groupby("customer_id")["revenue"].sum()
avg_rev_per_cust = rev_per_cust.mean()
median_rev_per_cust = rev_per_cust.median()
insights.append({
    "topic": "Revenue per Customer Distribution",
    "insight": f"Average revenue per customer: ${avg_rev_per_cust:,.2f}. Median: ${median_rev_per_cust:,.2f}. Top customer spent ${rev_per_cust.max():,.2f}. {(rev_per_cust > avg_rev_per_cust).sum()} customers are above average.",
    "data": {"avg_revenue_per_customer": round(avg_rev_per_cust, 2), "median": round(median_rev_per_cust, 2), "max": round(rev_per_cust.max(), 2), "above_avg_count": int((rev_per_cust > avg_rev_per_cust).sum())}
})

# --- Marital Status ---
rev_marital = cust_txn.groupby("marital_status")["revenue"].sum()
insights.append({
    "topic": "Revenue by Marital Status",
    "insight": f"Revenue by marital status: Married (M) ${rev_marital.get('M', 0):,.2f}, Single (S) ${rev_marital.get('S', 0):,.2f}.",
    "data": {m: round(r, 2) for m, r in rev_marital.items()}
})

# --- Quarterly Revenue ---
txn_e["quarter"] = txn_e["transaction_date"].dt.quarter
txn_e["year_quarter"] = txn_e["year"].astype(str) + " Q" + txn_e["quarter"].astype(str)
rev_quarter = txn_e.groupby("year_quarter")["revenue"].sum().sort_index()
insights.append({
    "topic": "Quarterly Revenue",
    "insight": "Quarterly revenue: " + ", ".join([f"{q}: ${r:,.2f}" for q, r in rev_quarter.items()]),
    "data": {q: round(r, 2) for q, r in rev_quarter.items()}
})

# Save all insights
with open(os.path.join(OUT, "insights.jsonl"), "w") as f:
    for ins in insights:
        f.write(json.dumps(ins) + "\n")

print(f"Saved insights.jsonl — {len(insights)} insights")

print(f"\n=== KNOWLEDGE BASE COMPLETE ===")
print(f"Files in {OUT}:")
for fname in os.listdir(OUT):
    fpath = os.path.join(OUT, fname)
    size_kb = os.path.getsize(fpath) / 1024
    print(f"  {fname} ({size_kb:.1f} KB)")

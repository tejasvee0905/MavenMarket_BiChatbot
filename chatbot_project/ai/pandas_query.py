"""
Pandas Query Engine — Lets the LLM generate and execute pandas code against CSVs.
Used as a fallback when the knowledge base doesn't have a pre-computed answer.
"""
import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data", "csvs")

# Pre-load all dataframes at import time
_dataframes = {}

def get_dataframes() -> dict[str, pd.DataFrame]:
    """Load all CSVs into a dict of DataFrames, cached after first call."""
    global _dataframes
    if _dataframes:
        return _dataframes

    _dataframes["customers"] = pd.read_csv(os.path.join(DATA_DIR, "MavenMarket_Customers.csv"))
    _dataframes["products"] = pd.read_csv(os.path.join(DATA_DIR, "MavenMarket_Products.csv"))
    _dataframes["stores"] = pd.read_csv(os.path.join(DATA_DIR, "MavenMarket_Stores.csv"))
    _dataframes["regions"] = pd.read_csv(os.path.join(DATA_DIR, "MavenMarket_Regions.csv"))
    _dataframes["transactions"] = pd.read_csv(os.path.join(DATA_DIR, "MavenMarket_Transactions.csv"))
    _dataframes["returns"] = pd.read_csv(os.path.join(DATA_DIR, "MavenMarket_Returns.csv"))
    _dataframes["calendar"] = pd.read_csv(os.path.join(DATA_DIR, "MavenMarket_Calendar.csv"))

    # Pre-compute common joins
    txn = _dataframes["transactions"].merge(_dataframes["products"], on="product_id")
    txn = txn.merge(_dataframes["customers"], on="customer_id")
    txn = txn.merge(_dataframes["stores"], on="store_id", suffixes=("_cust", "_store"))
    txn["revenue"] = txn["quantity"] * txn["product_retail_price"]
    txn["cost"] = txn["quantity"] * txn["product_cost"]
    txn["profit"] = txn["revenue"] - txn["cost"]
    txn["transaction_date"] = pd.to_datetime(txn["transaction_date"], dayfirst=True)
    txn["year"] = txn["transaction_date"].dt.year
    txn["month_name"] = txn["transaction_date"].dt.month_name()
    _dataframes["txn_full"] = txn

    return _dataframes


def get_schema_summary() -> str:
    """Return a concise schema summary for the LLM to use when generating pandas code."""
    return """Available DataFrames:
- customers: customer_id, customer_country, marital_status (M=Married, S=Single), gender (M=Male, F=Female), yearly_income, education, occupation, member_card, homeowner (Y/N), total_children, num_children_at_home, customer_city, customer_state_province
- products: product_id, product_brand, product_name, product_retail_price, product_cost, recyclable (0/1), low_fat (0/1), product_weight
- stores: store_id, region_id, store_type, store_name, store_city, store_state, store_country, total_sqft, grocery_sqft
- regions: region_id, sales_district, sales_region
- transactions: transaction_date, product_id, customer_id, store_id, quantity
- returns: return_date, product_id, store_id, quantity
- calendar: transaction_date, year, month_number, month_name, quarter, day_of_week
- txn_full: Pre-joined transactions+products+customers+stores with computed columns: revenue, cost, profit, year, month_name. Columns from customers have no suffix, store columns have _store suffix where conflicts exist (e.g., store_country vs customer_country)."""


ALLOWED_MODULES = {"pd", "pandas", "np", "numpy"}


def execute_pandas_query(code: str) -> str:
    """
    Safely execute a pandas query and return the result as a string.
    Only allows access to the loaded dataframes — no file I/O, imports, or system calls.
    """
    # Security: block dangerous operations
    dangerous = ["import ", "exec(", "eval(", "open(", "__", "os.", "sys.", "subprocess",
                  "shutil", "pathlib", "glob", "requests", "urllib", "socket",
                  "compile(", "getattr(", "setattr(", "delattr(", "globals(", "locals(",
                  "breakpoint(", "input(", "print("]  # print blocked to force return
    code_lower = code.lower()
    for d in dangerous:
        if d.lower() in code_lower:
            return f"Error: Blocked operation '{d.strip()}' — not allowed for safety."

    dfs = get_dataframes()

    # Execute in restricted namespace
    namespace = {
        "pd": pd,
        "customers": dfs["customers"],
        "products": dfs["products"],
        "stores": dfs["stores"],
        "regions": dfs["regions"],
        "transactions": dfs["transactions"],
        "returns": dfs["returns"],
        "calendar": dfs["calendar"],
        "txn_full": dfs["txn_full"],
    }

    try:
        # Execute the code
        exec(code, {"__builtins__": {}}, namespace)
        # Look for a 'result' variable
        if "result" in namespace:
            res = namespace["result"]
            if isinstance(res, pd.DataFrame):
                return res.to_string(max_rows=50)
            elif isinstance(res, pd.Series):
                return res.to_string()
            else:
                return str(res)
        return "Code executed but no 'result' variable was set. Assign your answer to 'result'."
    except Exception as e:
        return f"Error executing query: {type(e).__name__}: {e}"


if __name__ == "__main__":
    # Quick test
    result = execute_pandas_query("""
result = customers.groupby(['customer_country', 'marital_status']).size().unstack(fill_value=0)
""")
    print(result)

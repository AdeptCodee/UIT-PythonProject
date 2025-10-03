import pandas as pd

def _lower(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase all column names to standardize schema."""
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df

def clean_transactions(tx: pd.DataFrame, pro: pd.DataFrame) -> pd.DataFrame:
    """
    Minimal cleaning + schema mapping for the dunnhumby raw tables.
    - Rename columns to a stable schema
    - Remove bad rows (quantity <= 0 or sales_value < 0)
    - Compute unit_price, discount_value, gross_value, discount_rate
    - Attach optional department/category from products
    - Derive order_date from 'day' (1 -> 2017-01-01)
    """
    tx = _lower(tx).rename(columns={
        "household_key": "household_id",
        "basket_id": "basket_id",
        "product_id": "product_id",
        "quantity": "quantity",
        "sales_value": "sales_value",
        "store_id": "store_id",
        "retail_disc": "retail_disc",
        "coupon_disc": "coupon_disc",
        "coupon_match_disc": "coupon_match_disc",
        "day": "day",
        "trans_time": "trans_time",
        "week_no": "week",
    })
    pro = _lower(pro)

    # Ensure numeric types and fill missing discounts with 0
    for c in ["quantity", "sales_value", "retail_disc", "coupon_disc", "coupon_match_disc"]:
        if c in tx.columns:
            tx[c] = pd.to_numeric(tx[c], errors="coerce")
    tx["retail_disc"]       = tx.get("retail_disc", 0).fillna(0)
    tx["coupon_disc"]       = tx.get("coupon_disc", 0).fillna(0)
    tx["coupon_match_disc"] = tx.get("coupon_match_disc", 0).fillna(0)

    # Basic filters
    tx = tx[(tx["quantity"] > 0) & (tx["sales_value"] >= 0)].copy()

    # Price/discount metrics
    tx["unit_price"] = (tx["sales_value"] / tx["quantity"]).replace([float("inf")], pd.NA)
    tx.loc[tx["unit_price"] <= 0, "unit_price"] = pd.NA

    tx["discount_value"] = tx["retail_disc"] + tx["coupon_disc"] + tx["coupon_match_disc"]
    tx["gross_value"]    = tx["sales_value"] + tx["discount_value"]
    tx["discount_rate"]  = (tx["discount_value"] / tx["gross_value"]).fillna(0)

    # Attach department/category from products if available
    dept_col = next((c for c in pro.columns if c in ["department", "dept"]), None)
    cat_col  = next((c for c in pro.columns if c in ["product_category", "category", "cat"]), None)
    keep = ["product_id"]
    if cat_col:  keep.append(cat_col)
    if dept_col: keep.append(dept_col)
    if all(k in pro.columns for k in keep):
        tx = tx.merge(pro[keep].drop_duplicates(), on="product_id", how="left")

    # Derive order_date from 'day'
    base = pd.Timestamp("2017-01-01")
    tx["order_date"] = (base + pd.to_timedelta(tx["day"].astype(int) - 1, unit="D")).dt.date

    return tx

def flag_coupon_with_discount(tx: pd.DataFrame) -> pd.DataFrame:
    """
    Treatment flag (Method B):
    - Mark a line as coupon_applied if discount_value > 0
    - Basket-level flag will be the max over the basket
    """
    tx = tx.copy()
    tx["coupon_applied_line"] = (tx["discount_value"] > 0).astype(int)
    return tx

def build_baskets(tx: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate to basket level:
    - One row per (basket_id, household_id, order_date)
    - items, sales, discount, coupon_applied (max over lines)
    """
    g = (tx.groupby(["basket_id", "household_id", "order_date"], as_index=False)
           .agg(items=("quantity", "sum"),
                sales=("sales_value", "sum"),
                discount=("discount_value", "sum"),
                coupon_applied=("coupon_applied_line", "max")))
    return g

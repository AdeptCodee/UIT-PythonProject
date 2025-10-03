from src.load_dunnhumby import load_all
from src.build_baskets import clean_transactions, flag_coupon_with_discount, build_baskets

# 1) Load raw tables
tx, pro, hh, cps, red, cam, cdesc, causal = load_all()

# 2) Minimal cleaning + mapping
txc = clean_transactions(tx, pro)
print("After clean, tx shape:", txc.shape)
print(txc[["sales_value", "discount_value", "discount_rate", "unit_price", "order_date"]].head())

# 3) Flag coupon (Method B) and build basket-level
txc = flag_coupon_with_discount(txc)
baskets = build_baskets(txc)
print("Baskets:", baskets.shape)
print(baskets.head())
print("Coupon rate:", round(baskets["coupon_applied"].mean(), 3))

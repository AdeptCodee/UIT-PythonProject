# test_load.py
from src.load_dunnhumby import load_all

tx, pro, hh, cps, red, cam, cdesc, causal = load_all()
print("Transactions:", tx.shape)
print("Products    :", pro.shape)
print("Households  :", hh.shape)
print("Coupons     :", cps.shape)
print("Redemptions :", red.shape)
print("Campaigns   :", cam.shape)
print("Camp desc   :", cdesc.shape)
print("Causal      :", causal.shape)

print("\nSample transactions columns:")
print(tx.columns.tolist()[:20])
print(tx.head())

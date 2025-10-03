# src/load_dunnhumby.py
from __future__ import annotations
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, Dict

# Project root: .../STAT3013.Q12_Group01
BASE_DIR = Path(__file__).resolve().parents[1]
RAW = BASE_DIR / "data" / "raw"

# Các tên file khả dĩ cho từng bảng
CANDIDATES: Dict[str, Tuple[str, ...]] = {
    "transactions": (
        "transactions", "transaction_data", "transaction", "trans"
    ),
    "products": (
        "products", "product"
    ),
    "households": (
        "households", "hh_demographic", "household", "hh"
    ),
    "coupons": (
        "coupons", "coupon"
    ),
    "redemptions": (
        "redemptions", "coupon_redempt", "redemption"
    ),
    "campaigns": (
        "campaigns", "campaign_table", "campaign"
    ),
    "campaign_descriptions": (
        "campaign_descriptions", "campaign_desc", "campaign_description"
    ),
    # tuỳ dataset có/không bảng này; nhiều bản dùng causal_data để track coupon usage chi tiết
    "coupon_redemptions": (
        "coupon_redemptions", "causal_data"
    ),
}

EXTS = (".csv", ".xlsx", ".xls")


def _find_path(stem_candidates: Tuple[str, ...]) -> Optional[Path]:
    """Tìm file theo nhiều tên gốc (stem) và phần mở rộng hợp lệ trong RAW."""
    for stem in stem_candidates:
        for ext in EXTS:
            p = RAW / f"{stem}{ext}"
            if p.exists():
                return p
    return None


def _read_any(path: Path, parse_dates=None) -> pd.DataFrame:
    """Đọc CSV/XLSX/XLS. parse_dates là danh sách cột ngày nếu tồn tại."""
    parse_dates = parse_dates or []
    if path.suffix.lower() == ".csv":
        # parse_dates chỉ áp dụng cho cột nào thực sự có
        use_parse = [c for c in parse_dates if _has_column(path, c)]
        return pd.read_csv(path, parse_dates=use_parse or None)
    else:
        # Excel: cần openpyxl
        df = pd.read_excel(path)
        # tự ép kiểu ngày nếu cột có mặt
        for c in parse_dates:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors="coerce")
        return df


def _has_column(csv_path: Path, col: str) -> bool:
    """Kiểm tra nhanh header CSV để tránh parse_dates sai cột."""
    try:
        with csv_path.open("r", encoding="utf-8") as f:
            header = f.readline().strip()
        return col in [h.strip().strip('"') for h in header.split(",")]
    except Exception:
        return False


def _load_table(key: str, parse_date_cols: Tuple[str, ...] = ()) -> pd.DataFrame:
    p = _find_path(CANDIDATES[key])
    if p is None:
        raise FileNotFoundError(
            f"Không tìm thấy bảng '{key}' trong {RAW}. "
            f"Thử các tên: {CANDIDATES[key]} với đuôi {EXTS}"
        )
    df = _read_any(p, parse_dates=list(parse_date_cols))
    return df


def load_all():
    """
    Trả về: tx, pro, hh, cps, red, cam, cdesc
    (và cố gắng nạp 'coupon_redemptions' nếu có – trả thêm ở vị trí cuối cùng)
    """
    # transactions: cố gắng parse cột 'transaction_timestamp' nếu hiện diện
    tx = _load_table("transactions", parse_date_cols=("transaction_timestamp",))

    pro = _load_table("products")
    hh = _load_table("households")
    cps = _load_table("coupons")

    # redemptions: nhiều bản không có cột timestamp → không ép ngày ở đây
    red = _load_table("redemptions")

    cam = _load_table("campaigns")
    cdesc = _load_table("campaign_descriptions")

    # optional
    try:
        cred = _load_table("coupon_redemptions")
        return tx, pro, hh, cps, red, cam, cdesc, cred
    except FileNotFoundError:
        # nếu không có cũng OK
        return tx, pro, hh, cps, red, cam, cdesc

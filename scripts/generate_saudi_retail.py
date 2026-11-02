import numpy as np
import pandas as pd
from pathlib import Path

rng = np.random.default_rng(2030)
OUT = Path("landing")
DAYS = pd.date_range("2023-01-01", "2025-12-31", freq="D")

# city: (region, share of customers). Shares are rough, informed by SAMA's weekly
# point-of-sale reports, where Riyadh is about a third of card spending.
CITIES = {
    "Riyadh": ("Riyadh", 0.32), "Jeddah": ("Makkah", 0.16), "Makkah": ("Makkah", 0.06),
    "Madinah": ("Madinah", 0.06), "Dammam": ("Eastern Province", 0.07),
    "Khobar": ("Eastern Province", 0.05), "Taif": ("Makkah", 0.04), "Buraydah": ("Qassim", 0.05),
    "Abha": ("Asir", 0.04), "Tabuk": ("Tabuk", 0.04), "Hail": ("Hail", 0.03),
    "Jazan": ("Jazan", 0.04), "Najran": ("Najran", 0.04),
}
CITY_NAMES = list(CITIES)
CITY_W = np.array([w for _, w in CITIES.values()])

# How the e-commerce platform and the branch systems spell the same city
VARIANTS = {
    "Riyadh": ["Riyadh", "الرياض", "RIYADH ", "Riyadh City"],
    "Jeddah": ["Jeddah", "جدة", "Jiddah"],
    "Makkah": ["Makkah", "Mecca", "مكة المكرمة"],
    "Madinah": ["Madinah", "Medina", "المدينة المنورة"],
    "Dammam": ["Dammam", "الدمام"],
}

# category: (SAMA sector, min price, max price, share of lines)
CATEGORIES = {
    "Electronics": ("Electronic and Electric Devices", 150, 6000, 0.18),
    "Clothing & Footwear": ("Clothing and Footwear", 40, 600, 0.25),
    "Furniture": ("Furniture", 300, 8000, 0.07),
    "Food & Beverages": ("Beverage and Food", 5, 150, 0.30),
    "Jewelry": ("Jewelry", 200, 15000, 0.05),
    "Health & Beauty": ("Health", 15, 400, 0.15),
}
CATS = list(CATEGORIES)

# Approximate Gregorian dates; Hijri dates depend on moon sighting
EVENTS = [
    ("Ramadan", "2023-03-23", "2023-04-20"), ("Ramadan", "2024-03-11", "2024-04-09"),
    ("Ramadan", "2025-03-01", "2025-03-29"),
    ("Eid al-Fitr", "2023-04-21", "2023-04-24"), ("Eid al-Fitr", "2024-04-10", "2024-04-13"),
    ("Eid al-Fitr", "2025-03-30", "2025-04-02"),
    ("Eid al-Adha", "2023-06-28", "2023-07-01"), ("Eid al-Adha", "2024-06-16", "2024-06-19"),
    ("Eid al-Adha", "2025-06-06", "2025-06-09"),
    ("White Friday", "2023-11-20", "2023-11-27"), ("White Friday", "2024-11-25", "2024-12-02"),
    ("White Friday", "2025-11-24", "2025-12-01"),
    ("National Day", "2023-09-20", "2023-09-26"), ("National Day", "2024-09-20", "2024-09-26"),
    ("National Day", "2025-09-20", "2025-09-26"),
    ("Back to School", "2023-08-06", "2023-08-19"), ("Back to School", "2024-08-04", "2024-08-17"),
    ("Back to School", "2025-08-10", "2025-08-23"),
]
EVENTS = [(n, pd.Timestamp(a), pd.Timestamp(b)) for n, a, b in EVENTS]

def day_factors(day):
    total = {3: 1.15, 4: 1.2, 5: 1.1}.get(day.weekday(), 0.95)   # Thu, Fri, Sat busiest
    if day.day >= 27 or day.day <= 2:
        total *= 1.25                                              # salaries paid around the 27th
    cat = {c: 1.0 for c in CATS}
    for name, a, b in EVENTS:
        if not (a <= day <= b):
            continue
        if name == "Ramadan":
            total *= 1.3
            cat["Food & Beverages"] *= 1.6
            if day >= b - pd.Timedelta(days=10):                   # pre-Eid shopping
                cat["Clothing & Footwear"] *= 2.0
                cat["Jewelry"] *= 1.8
        elif name == "White Friday":
            total *= 1.8
            cat["Electronics"] *= 2.5
        elif name == "National Day":
            total *= 1.4
        elif name == "Back to School":
            cat["Clothing & Footwear"] *= 1.5
    return total, cat

# Reference data
customers = pd.DataFrame({
    "customer_id": [f"C{i:05d}" for i in range(1, 20001)],
    "home_city": rng.choice(CITY_NAMES, 20000, p=CITY_W),
})

product_rows = []
for i in range(1, 401):
    c = rng.choice(CATS, p=[CATEGORIES[k][3] for k in CATS])
    lo, hi = CATEGORIES[c][1], CATEGORIES[c][2]
    product_rows.append({"product_id": f"P{i:04d}", "category": c,
                         "brand": f"Brand {rng.integers(1, 41)}",
                         "list_price_sar": round(float(rng.uniform(lo, hi)), 2)})
products = pd.DataFrame(product_rows)
PROD_IDS = {c: products.loc[products.category == c, "product_id"].to_numpy() for c in CATS}
PROD_PRICE = {c: products.loc[products.category == c, "list_price_sar"].to_numpy() for c in CATS}

branch_cities = ["Riyadh"] * 6 + ["Jeddah"] * 4 + ["Dammam", "Khobar", "Makkah", "Madinah",
                 "Taif", "Buraydah", "Abha", "Tabuk", "Hail", "Jazan", "Najran"]
branches = pd.DataFrame({"branch_id": [f"B{i:02d}" for i in range(1, len(branch_cities) + 1)],
                         "city": branch_cities})
branches["region"] = branches.city.map(lambda c: CITIES[c][0])
BRANCHES_IN = {c: branches.loc[branches.city == c, "branch_id"].to_numpy() for c in CITY_NAMES}
branches = pd.concat([branches, pd.DataFrame([{"branch_id": "WEB", "city": "Online", "region": "Online"}])])

HOUR_W = np.array([2, 1, 1, .5, .5, .5, .5, 1, 2, 3, 4, 5, 5, 4, 4, 5, 6, 7, 8, 9, 10, 10, 8, 5])
HOUR_W = HOUR_W / HOUR_W.sum()                                    # evening peak
METHODS = ["mada", "Apple Pay", "Visa/Mastercard", "STC Pay", "Tabby", "Tamara", "Cash on Delivery"]
PAY_ONLINE = [.45, .20, .12, .04, .08, .05, .06]
PAY_STORE = [.55, .22, .15, .04, .02, .02, .00]

# Orders
orders, items, payments = [], [], []
cust_ids, cust_city = customers.customer_id.to_numpy(), customers.home_city.to_numpy()
n = 0
for d, day in enumerate(DAYS):
    total, cat_mult = day_factors(day)
    w = np.array([CATEGORIES[c][3] * cat_mult[c] for c in CATS])
    w = w / w.sum()
    for _ in range(rng.poisson(110 * total * 1.01 ** (d / 30))):   # ~1% monthly growth
        n += 1
        oid, ci = f"O{n:07d}", rng.integers(len(cust_ids))
        city, online = cust_city[ci], rng.random() < 0.4
        local = BRANCHES_IN.get(city, np.array([]))
        branch = "WEB" if online or len(local) == 0 else rng.choice(local)
        ts = day + pd.Timedelta(hours=int(rng.choice(24, p=HOUR_W)), minutes=int(rng.integers(60)))
        status = rng.choice(["delivered", "cancelled", "returned"], p=[.93, .04, .03])
        gross = 0.0
        for line in range(1, int(rng.choice([1, 2, 3, 4], p=[.55, .25, .13, .07])) + 1):
            c = rng.choice(CATS, p=w)
            k = rng.integers(len(PROD_IDS[c]))
            qty, price = int(rng.choice([1, 1, 1, 2, 3])), PROD_PRICE[c][k]
            discount = round(price * qty * rng.choice([0, 0, 0, .1, .2]), 2)
            vat = round((price * qty - discount) * 0.15, 2)              # 15% VAT
            items.append((oid, line, PROD_IDS[c][k], qty, price, discount, vat))
            gross += price * qty - discount + vat
        method = rng.choice(METHODS, p=PAY_ONLINE if online else PAY_STORE)
        payments.append((oid, 1, method, 4 if method in ("Tabby", "Tamara") else 1, round(gross, 2)))
        orders.append((oid, cust_ids[ci], branch, "Online" if online else "Store",
                       rng.choice(VARIANTS.get(city, [city])), ts, status))

orders = pd.DataFrame(orders, columns=["order_id", "customer_id", "branch_id", "channel",
                                       "city", "order_ts", "status"])
items = pd.DataFrame(items, columns=["order_id", "line_no", "product_id", "quantity",
                                     "unit_price_sar", "discount_sar", "vat_sar"])
payments = pd.DataFrame(payments, columns=["order_id", "payment_seq", "payment_method",
                                           "installments", "amount_sar"])

# Monthly files, with the mess real exports have
month = orders.order_ts.dt.strftime("%Y-%m")
for ym, mo in orders.groupby(month):
    folder = OUT / "sales" / ym
    folder.mkdir(parents=True, exist_ok=True)
    mo = mo.copy()
    fmt = "%d/%m/%Y %H:%M" if ym == "2024-07" else "%Y-%m-%d %H:%M:%S"   # a system upgrade
    mo["order_ts"] = mo.order_ts.dt.strftime(fmt)
    ids = set(mo.order_id)
    month_items = items[items.order_id.isin(ids)]
    month_items = pd.concat([month_items, month_items.sample(frac=0.005, random_state=1)])  # duplicates
    mo.to_csv(folder / "orders.csv", index=False, encoding="utf-8-sig")
    month_items.to_csv(folder / "order_items.csv", index=False, encoding="utf-8-sig")
    payments[payments.order_id.isin(ids)].to_csv(folder / "payments.csv", index=False,
                                                encoding="utf-8-sig")

ref = OUT / "reference"
ref.mkdir(parents=True, exist_ok=True)
customers.to_csv(ref / "customers.csv", index=False, encoding="utf-8-sig")
products.to_csv(ref / "products.csv", index=False, encoding="utf-8-sig")
branches.to_csv(ref / "branches.csv", index=False, encoding="utf-8-sig")
pd.DataFrame([(c, v[0]) for c, v in CATEGORIES.items()], columns=["category", "sama_sector"]) \
  .to_csv(ref / "category_sector_map.csv", index=False, encoding="utf-8-sig")
pd.DataFrame([(nm, a.date(), b.date()) for nm, a, b in EVENTS],
             columns=["event_name", "start_date", "end_date"]) \
  .to_csv(ref / "events.csv", index=False, encoding="utf-8-sig")

print(f"{len(orders):,} orders, {len(items):,} lines, {month.nunique()} months")
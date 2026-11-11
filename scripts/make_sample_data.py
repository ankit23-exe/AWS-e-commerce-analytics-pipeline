"""Generate a tiny SYNTHETIC dataset that mimics the Olist file layout.

It is not real Olist data. It exists so the pipeline and the tests can run on a laptop,
and it contains deliberate defects so you can see the quality checks working:

  customers   1 exact duplicate row
  orders      1 duplicate, 1 unknown customer, 1 invalid status, 1 missing purchase timestamp
  order_items 1 duplicate, 1 missing product_id, 1 negative price, 1 unknown seller,
              1 item for a non-existent order, 3 items whose parent order is quarantined
  products    1 missing category (-> "unknown"), 1 category without a translation

Run:  python scripts/make_sample_data.py
"""
import csv
from datetime import datetime, timedelta
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "data" / "sample" / "raw"
FMT = "%Y-%m-%d %H:%M:%S"


def write(table, filename, header, rows):
    folder = OUT / table
    folder.mkdir(parents=True, exist_ok=True)
    with open(folder / filename, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def main():
    # ---- customers: c001..c030 (+1 exact duplicate) ----
    states = ["SP", "RJ", "MG", "BA", "RS"]
    customers = [
        [f"c{i:03d}", f"u{i:03d}", f"{1000 + i * 37:05d}", f"city_{i % 7}", states[i % 5]]
        for i in range(1, 31)
    ]
    customers.append(list(customers[0]))
    write("customers", "olist_customers_dataset.csv",
          ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"],
          customers)

    # ---- sellers: s01..s05 ----
    sellers = [[f"s{i:02d}", f"{2000 + i * 11:05d}", f"seller_city_{i}", states[i % 5]] for i in range(1, 6)]
    write("sellers", "olist_sellers_dataset.csv",
          ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"], sellers)

    # ---- products: p01..p10 (header keeps the real dataset's "lenght" typo) ----
    cats = ["beleza_saude", "informatica_acessorios", "esporte_lazer", "moveis_decoracao", "brinquedos"]
    products = []
    for i in range(1, 9):
        products.append([f"p{i:02d}", cats[i % 5], 40 + i, 300 + i * 10, 1 + i % 4,
                         200 + i * 50, 10 + i, 5 + i, 8 + i])
    products.append(["p09", "", 45, 350, 2, 500, 20, 10, 15])               # missing category
    products.append(["p10", "categoria_sem_traducao", 50, 400, 3, 700, 25, 12, 18])  # no translation
    write("products", "olist_products_dataset.csv",
          ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght",
           "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm",
           "product_width_cm"], products)

    english = {"beleza_saude": "health_beauty", "informatica_acessorios": "computers_accessories",
               "esporte_lazer": "sports_leisure", "moveis_decoracao": "furniture_decor",
               "brinquedos": "toys"}
    write("category_translation", "product_category_name_translation.csv",
          ["product_category_name", "product_category_name_english"], [[k, v] for k, v in english.items()])

    # ---- orders: o0001..o0040 spread over ~4 months ----
    start = datetime(2017, 11, 1, 10, 0, 0)
    orders, items = [], []
    for i in range(40):
        oid = f"o{i + 1:04d}"
        purchase = start + timedelta(days=3 * i)
        approved = purchase + timedelta(hours=1)
        estimated = purchase + timedelta(days=15)
        if i % 13 == 12:
            status, carrier, delivered = "canceled", "", ""
        elif i % 8 == 7:
            status, carrier, delivered = "shipped", (purchase + timedelta(days=2)).strftime(FMT), ""
        else:
            status = "delivered"
            carrier = (purchase + timedelta(days=2)).strftime(FMT)
            delivered = (purchase + timedelta(days=8 + i % 12)).strftime(FMT)  # some are late
        orders.append([oid, f"c{i % 30 + 1:03d}", status, purchase.strftime(FMT), approved.strftime(FMT),
                       carrier, delivered, estimated.strftime(FMT)])
        for k in range(1 + i % 3):
            items.append([oid, k + 1, f"p{(i + k) % 10 + 1:02d}", f"s{(i + k) % 5 + 1:02d}",
                          (purchase + timedelta(days=3)).strftime(FMT),
                          f"{20 + (i * 7 + k) % 50}.90", f"{5 + (i * 3 + k) % 10}.25"])

    ts = (start + timedelta(days=200)).strftime(FMT)
    # order defects
    orders.append(["o0041", "c999", "delivered", ts, ts, ts, ts, ts])      # unknown customer
    orders.append(["o0042", "c001", "weird_status", ts, ts, ts, ts, ts])   # invalid status
    orders.append(["o0043", "c002", "delivered", "", ts, ts, ts, ts])      # missing purchase timestamp
    orders.append(list(orders[0]))                                          # exact duplicate
    # item defects
    items.append(list(items[0]))                                            # exact duplicate
    items.append(["o0002", 9, "", "s01", ts, "10.00", "5.00"])              # missing product_id
    items.append(["o0003", 9, "p01", "s01", ts, "-5.00", "5.00"])           # negative price
    items.append(["o0004", 9, "p01", "s99", ts, "10.00", "5.00"])           # unknown seller
    items.append(["o9999", 1, "p01", "s01", ts, "10.00", "5.00"])           # order does not exist
    for oid in ("o0041", "o0042", "o0043"):                                 # parent order quarantined
        items.append([oid, 1, "p01", "s01", ts, "10.00", "5.00"])

    write("orders", "olist_orders_dataset.csv",
          ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at",
           "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"],
          orders)
    write("order_items", "olist_order_items_dataset.csv",
          ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price",
           "freight_value"], items)
    print(f"Sample data written to {OUT}  (orders: {len(orders)}, order_items: {len(items)})")


if __name__ == "__main__":
    main()

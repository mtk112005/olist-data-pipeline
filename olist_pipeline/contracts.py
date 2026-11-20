"""Data contracts for the nine Olist source files.

Each contract declares column types, the primary key, required columns and
allowed values. The silver stage enforces them; rows that violate a contract
go to quarantine instead of silently flowing downstream.
"""
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Contract:
    file: str
    columns: dict            # column -> one of: str, zip, int, float, ts
    primary_key: tuple
    required: tuple = ()
    allowed: dict = field(default_factory=dict)
    kind: str = "reference"   # reference (full refresh) | transaction (loaded by month)


ORDER_STATUSES = ("created", "approved", "invoiced", "processing", "shipped",
                  "delivered", "canceled", "unavailable")
PAYMENT_TYPES = ("credit_card", "boleto", "voucher", "debit_card", "not_defined")

CONTRACTS = {
    "customers": Contract(
        "olist_customers_dataset.csv",
        {"customer_id": "str", "customer_unique_id": "str", "customer_zip_code_prefix": "zip",
         "customer_city": "str", "customer_state": "str"},
        primary_key=("customer_id",), required=("customer_unique_id",)),
    "sellers": Contract(
        "olist_sellers_dataset.csv",
        {"seller_id": "str", "seller_zip_code_prefix": "zip", "seller_city": "str", "seller_state": "str"},
        primary_key=("seller_id",)),
    "products": Contract(
        "olist_products_dataset.csv",
        {"product_id": "str", "product_category_name": "str", "product_name_lenght": "int",
         "product_description_lenght": "int", "product_photos_qty": "int", "product_weight_g": "int",
         "product_length_cm": "int", "product_height_cm": "int", "product_width_cm": "int"},
        primary_key=("product_id",)),
    "category_translation": Contract(
        "product_category_name_translation.csv",
        {"product_category_name": "str", "product_category_name_english": "str"},
        primary_key=("product_category_name",)),
    "geolocation": Contract(
        "olist_geolocation_dataset.csv",
        {"geolocation_zip_code_prefix": "zip", "geolocation_lat": "float", "geolocation_lng": "float",
         "geolocation_city": "str", "geolocation_state": "str"},
        # ~1M rows with many exact duplicates; no natural key, so dedupe on all columns
        primary_key=("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng",
                     "geolocation_city", "geolocation_state")),
    "orders": Contract(
        "olist_orders_dataset.csv",
        {"order_id": "str", "customer_id": "str", "order_status": "str",
         "order_purchase_timestamp": "ts", "order_approved_at": "ts",
         "order_delivered_carrier_date": "ts", "order_delivered_customer_date": "ts",
         "order_estimated_delivery_date": "ts"},
        primary_key=("order_id",), required=("customer_id", "order_purchase_timestamp"),
        allowed={"order_status": ORDER_STATUSES}, kind="transaction"),
    "order_items": Contract(
        "olist_order_items_dataset.csv",
        {"order_id": "str", "order_item_id": "int", "product_id": "str", "seller_id": "str",
         "shipping_limit_date": "ts", "price": "float", "freight_value": "float"},
        primary_key=("order_id", "order_item_id"), required=("product_id", "seller_id", "price"),
        kind="transaction"),
    "order_payments": Contract(
        "olist_order_payments_dataset.csv",
        {"order_id": "str", "payment_sequential": "int", "payment_type": "str",
         "payment_installments": "int", "payment_value": "float"},
        primary_key=("order_id", "payment_sequential"), allowed={"payment_type": PAYMENT_TYPES},
        kind="transaction"),
    "order_reviews": Contract(
        "olist_order_reviews_dataset.csv",
        {"review_id": "str", "order_id": "str", "review_score": "int", "review_comment_title": "str",
         "review_comment_message": "str", "review_creation_date": "ts", "review_answer_timestamp": "ts"},
        # review_id alone is NOT unique in the source (same review attached to several orders)
        primary_key=("review_id", "order_id"), required=("review_score",),
        allowed={"review_score": (1, 2, 3, 4, 5)}, kind="transaction"),
}

PG_TYPES = {"str": "text", "zip": "char(5)", "int": "integer", "float": "numeric(12,2)",
            "ts": "timestamp"}
# lat/lng need more precision than money columns
PG_OVERRIDES = {"geolocation_lat": "double precision", "geolocation_lng": "double precision"}


def raw_ddl(table: str) -> str:
    c = CONTRACTS[table]
    cols = [f'    {name} {PG_OVERRIDES.get(name, PG_TYPES[t])}' for name, t in c.columns.items()]
    cols += ["    _batch_id text not null", "    _loaded_at timestamptz not null default now()"]
    return f"create table if not exists raw.{table} (\n" + ",\n".join(cols) + "\n);"

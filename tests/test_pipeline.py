"""Unit tests for contracts, casting, validation and idempotent landing.
They run without a database (the warehouse is exercised in CI by dbt build)."""
import pandas as pd
import pytest

from olist_pipeline import lake
from olist_pipeline.contracts import CONTRACTS, raw_ddl

FIXTURES = __import__("pathlib").Path(__file__).parent / "fixtures" / "landing"


def test_every_contract_has_fixture_file_with_all_columns():
    for table, c in CONTRACTS.items():
        cols = pd.read_csv(FIXTURES / c.file, nrows=0, encoding="utf-8-sig").columns
        assert set(c.columns) == set(cols), table


def test_zip_codes_are_zero_padded_strings():
    df = pd.DataFrame({"seller_id": ["a"], "seller_zip_code_prefix": ["1234"],
                       "seller_city": [" Sao Paulo "], "seller_state": ["SP"]})
    out = lake.cast(df, CONTRACTS["sellers"])
    assert out.seller_zip_code_prefix.iloc[0] == "01234"
    assert out.seller_city.iloc[0] == "sao paulo"


def test_validation_quarantines_bad_rows_and_drops_duplicates():
    c = CONTRACTS["orders"]
    raw = pd.DataFrame({
        "order_id": ["o1", "o1", None, "o3"],
        "customer_id": ["c1", "c1", "c2", "c3"],
        "order_status": ["delivered", "delivered", "delivered", "teleported"],
        "order_purchase_timestamp": ["2017-01-01 10:00:00"] * 4,
        "order_approved_at": [""] * 4, "order_delivered_carrier_date": [""] * 4,
        "order_delivered_customer_date": [""] * 4, "order_estimated_delivery_date": [""] * 4,
    })
    good, bad, reasons = lake.validate(lake.cast(raw, c), c)
    assert list(good.order_id) == ["o1"]
    assert len(bad) == 2
    assert reasons == {"null_order_id": 1, "not_allowed_order_status": 1, "duplicate_key_dropped": 1}


def test_land_is_idempotent(tmp_path):
    bronze = tmp_path / "bronze"
    first = lake.land(FIXTURES, bronze)
    second = lake.land(FIXTURES, bronze)
    assert set(first.values()) == {"new version"}
    assert set(second.values()) == {"unchanged"}


@pytest.mark.parametrize("table", list(CONTRACTS))
def test_raw_ddl_has_audit_columns(table):
    ddl = raw_ddl(table)
    assert "_batch_id text not null" in ddl and "_loaded_at timestamptz" in ddl

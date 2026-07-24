import pandas as pd

from src.pipeline import transform


def test_transform_quarantines_zero_quantity_and_keeps_cancellation() -> None:
    frame = pd.DataFrame(
        [
            {
                "InvoiceNo": "100",
                "StockCode": "ABC",
                "Description": "Product",
                "Quantity": 2,
                "InvoiceDate": "2026-01-01 10:00:00",
                "UnitPrice": 5,
                "CustomerID": "1",
                "Country": "Vietnam",
            },
            {
                "InvoiceNo": "101",
                "StockCode": "ABC",
                "Description": "Product",
                "Quantity": 0,
                "InvoiceDate": "2026-01-01 10:00:00",
                "UnitPrice": 5,
                "CustomerID": "1",
                "Country": "Vietnam",
            },
            {
                "InvoiceNo": "C102",
                "StockCode": "ABC",
                "Description": "Product",
                "Quantity": -1,
                "InvoiceDate": "2026-01-01 10:00:00",
                "UnitPrice": 5,
                "CustomerID": None,
                "Country": "Vietnam",
            },
        ]
    )
    clean, rejected, summary = transform(frame)
    assert len(clean) == 2
    assert len(rejected) == 1
    assert summary["cancellation_rows"] == 1
    assert clean.loc[clean["InvoiceNo"] == "C102", "CustomerID"].item() == "GUEST"


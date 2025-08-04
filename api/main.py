"""
main.py

FastAPI app exposing the /optimize endpoint for invoice scheduling.
"""

from fastapi import FastAPI, UploadFile, File, Form
import pandas as pd
from io import BytesIO

from scheduler import schedule_payments

app = FastAPI(title="Vendor Payment Timing Optimizer API")

@app.post("/optimize", summary="Optimize a batch of vendor invoices")
async def optimize(
    cash: float = Form(..., description="Current cash balance"),
    runway: int = Form(..., description="Desired runway in days"),
    frequency: str = Form(..., description="Payrun frequency: Weekly, Bi-Weekly, Monthly"),
    max_spend: float = Form(None, description="Optional maximum AP spend override"),
    file: UploadFile = File(..., description="CSV or Excel file of vendor bills")
):
    """
    Accepts:
    - cash: float
    - runway: int
    - frequency: str
    - max_spend: float or None
    - file: uploaded .csv, .xls or .xlsx

    Returns:
    - JSON list of scheduled payment records.
    """
    # Read file bytes
    content = await file.read()
    if file.filename.lower().endswith((".xls", ".xlsx")):
        df = pd.read_excel(BytesIO(content))
    else:
        df = pd.read_csv(BytesIO(content))

    # Call scheduler
    schedule_df = schedule_payments(
        df,
        current_cash=cash,
        runway_days=runway,
        frequency=frequency,
        max_spend=max_spend
    )
    # Return JSON
    return schedule_df.to_dict(orient="records")

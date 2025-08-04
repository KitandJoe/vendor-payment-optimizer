"""
scheduler.py

Deterministic invoice‐scheduling logic for Vendor Payment Optimizer.
"""

import datetime as dt
import pandas as pd
from typing import Optional, Tuple

def parse_discount_terms(term: str) -> Tuple[float, Optional[int], Optional[int]]:
    """
    Parse a discount string like "2/10 Net 30":
    - returns (discount_rate, days_until_discount_expires, net_due_days)
    """
    if not isinstance(term, str) or "Net" not in term:
        return 0.0, None, None

    # e.g. "2/10 Net 30"
    try:
        pct_part, net_part = term.split(" Net ")
        discount_pct, days_str = pct_part.split("/")
        rate = float(discount_pct) / 100.0
        discount_days = int(days_str)
        net_days = int(net_part)
        return rate, discount_days, net_days
    except Exception:
        return 0.0, None, None

def schedule_payments(
    df: pd.DataFrame,
    current_cash: float,
    runway_days: int,
    frequency: str,
    max_spend: Optional[float] = None
) -> pd.DataFrame:
    """
    - df must contain columns: Invoice#, VendorName, Amount, DueDate (datetime/date),
      DiscountTerms (string), Priority (int)
    - Returns a DataFrame with:
      VendorName, Invoice#, OrigDueDate, RecPayDate, Amount, Discount$, Priority
    """

    # 1. Parse DueDate -> datetime.date
    df = df.copy()
    df['OrigDueDate'] = pd.to_datetime(df['DueDate']).dt.date

    # 2. Compute next payrun date based on frequency
    today = dt.date.today()
    if frequency.lower().startswith("weekly"):
        next_run = today + dt.timedelta(weeks=1)
    elif frequency.lower().startswith("bi"):
        next_run = today + dt.timedelta(weeks=2)
    else:  # monthly
        next_run = today + dt.timedelta(days=30)

    # 3. Filter + sort
    pay_list = []

    # A) Priority == 1 due before next run
    mask1 = (df['Priority'] == 1) & (df['OrigDueDate'] <= next_run)
    pay_list.append(df[mask1])

    # B) Due on or before next run (any priority)
    mask2 = (df['OrigDueDate'] <= next_run) & (~mask1)
    pay_list.append(df[mask2])

    # C) Discounts: compute savings per dollar / days until forfeiture
    remaining = df[~(mask1 | mask2)].copy()
    remaining[['rate', 'disc_days', 'net_days']] = remaining['DiscountTerms'].apply(
        lambda t: pd.Series(parse_discount_terms(t))
    )
    remaining['days_until_discount'] = remaining['disc_days'].fillna(0)
    # savings-per-dollar metric
    remaining['savings_rate'] = remaining['rate'] / remaining['days_until_discount'].replace(0, pd.NA)
    pay_list.append(remaining.sort_values('savings_rate', ascending=False))

    # D) Concatenate candidates
    candidate_df = pd.concat(pay_list)

    # E) Enforce cash & max_spend
    spend_cap = min(current_cash, max_spend) if max_spend else current_cash
    scheduled = []
    total_spent = 0.0
    for _, row in candidate_df.iterrows():
        amt = float(row['Amount'])
        if total_spent + amt > spend_cap:
            continue
        # compute discount$
        rate = row.get('rate', 0.0) or 0.0
        disc_amount = amt * rate
        scheduled.append({
            'VendorName': row['VendorName'],
            'Invoice#': row['Invoice#'],
            'OrigDueDate': row['OrigDueDate'],
            'RecPayDate': next_run,
            'Amount': amt,
            'Discount$': round(disc_amount, 2),
            'Priority': int(row['Priority'])
        })
        total_spent += amt

    result_df = pd.DataFrame(scheduled)
    return result_df

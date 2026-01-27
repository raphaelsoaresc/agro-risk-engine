import datetime
import pytz
import yfinance as yf
import pandas as pd

def fetch_market_data(
    tickers: list[str],
    rename_map: dict,
    lookback_days: int = 365,
    timezone: str = "UTC"
) -> pd.DataFrame:

    tz = pytz.timezone(timezone)
    end_date = datetime.datetime.now(tz)
    start_date = end_date - datetime.timedelta(days=lookback_days)

    data = yf.download(
        tickers,
        start=start_date,
        end=end_date,
        progress=False
    )["Close"]

    if rename_map:
        data = data.rename(columns=rename_map)

    data = data.ffill()

    if data.empty:
        raise RuntimeError("Nenhum dado retornado do Yahoo Finance.")

    return data

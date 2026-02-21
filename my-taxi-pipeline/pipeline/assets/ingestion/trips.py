"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"

@bruin"""

# Add imports needed for ingestion
import os
import io
import requests
import pandas as pd
from datetime import datetime
from typing import List, Optional
from dateutil.relativedelta import relativedelta

# NYC taxi TLC data endpoint
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"


def _iter_months(start: datetime, end: datetime) -> List[str]:
  """Return list of YYYY-MM strings inclusive between start and end (by month)."""
  months = []
  cur = datetime(start.year, start.month, 1)
  last = datetime(end.year, end.month, 1)
  while cur <= last:
    months.append(cur.strftime("%Y-%m"))
    cur = cur + relativedelta(months=1)
  return months


def _download_parquet(url: str) -> Optional[pd.DataFrame]:
  """Download a parquet file at `url` into a DataFrame. Returns None on 404 or other failures."""
  try:
    resp = requests.get(url, timeout=60)
    if resp.status_code == 200:
      return pd.read_parquet(io.BytesIO(resp.content))
    return None
  except Exception:
    return None


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
  """Normalize column names for pickup/dropoff datetimes and return a cleaned DataFrame.

  - Renames the pickup/dropoff datetime columns to `pickup_datetime`/`dropoff_datetime`.
  - Leaves other columns as-is.
  """
  # identify pickup/dropoff datetime columns (common patterns)
  pickup_col = next((c for c in df.columns if "pickup_datetime" in c.lower()), None)
  dropoff_col = next((c for c in df.columns if "dropoff_datetime" in c.lower()), None)

  if pickup_col:
    df = df.rename(columns={pickup_col: "pickup_datetime"})
  if dropoff_col:
    df = df.rename(columns={dropoff_col: "dropoff_datetime"})

  # ensure datetimes are parsed
  if "pickup_datetime" in df.columns:
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
  if "dropoff_datetime" in df.columns:
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

  return df


def materialize(start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
  """Bruin materialization entrypoint.

  Parameters may be passed as ISO datetimes (e.g. `2026-02-01T00:00:00Z`) or the function will fall
  back to environment variables `BRUIN_START_DATE` / `BRUIN_END_DATE`.

  The function downloads available monthly parquet files for both `yellow` and `green` datasets
  between the provided months, normalizes datetime columns, concatenates them, and returns a
  single DataFrame which Bruin will persist according to the materialization settings.
  """
  # allow explicit params, then env vars
  start = start_date or os.environ.get("BRUIN_START_DATE") or os.environ.get("START_DATE")
  end = end_date or os.environ.get("BRUIN_END_DATE") or os.environ.get("END_DATE")

  if not start or not end:
    raise ValueError("start_date and end_date must be provided either as args or environment variables")

  # parse to datetimes
  start_dt = datetime.fromisoformat(start.replace("Z", ""))
  end_dt = datetime.fromisoformat(end.replace("Z", ""))

  months = _iter_months(start_dt, end_dt)

  dfs: List[pd.DataFrame] = []
  for m in months:
    for color in ("yellow", "green"):
      filename = f"{color}_tripdata_{m}.parquet"
      url = BASE_URL + filename
      df = _download_parquet(url)
      if df is None:
        # skip missing months/colors silently
        continue
      df = _normalize_df(df)
      # record source info
      df["_source_file"] = filename
      df["_source_url"] = url
      dfs.append(df)

  if not dfs:
    raise RuntimeError("No trip data files found for the requested date range")

  out = pd.concat(dfs, ignore_index=True, sort=False)

  # keep at least pickup/dropoff if present
  cols = []
  if "pickup_datetime" in out.columns:
    cols.append("pickup_datetime")
  if "dropoff_datetime" in out.columns:
    cols.append("dropoff_datetime")
  # add rest of columns after these two
  other_cols = [c for c in out.columns if c not in cols]
  ordered = cols + other_cols
  out = out[ordered]

  return out




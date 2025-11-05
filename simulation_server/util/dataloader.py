from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import sqlalchemy as sqla
from .druid import to_timestamp, get_table
from ..simulation import SimException


def query_time_range(
    tbl_name: str,
    start: datetime, end: datetime,
    start_col: str, end_col: str, *,
    druid_engine, parse_dates: list[str] = [],
) -> pd.DataFrame:
    """ Queries a time range in druid. Returns a dataframe, throws if empty. """
    tbl = get_table(tbl_name, druid_engine)
    query = (
        sqla.select(sqla.text("*"))
            .where(
                # __time is submission time
                (tbl.c['__time'] <= to_timestamp(end)) &
                (tbl.c['__time'] >= to_timestamp(start - timedelta(days=7))) &
                (tbl.c[start_col] <= to_timestamp(end)) &
                (tbl.c[end_col] >= to_timestamp(start))
            )
    )
    df = pd.read_sql(query, druid_engine, parse_dates=parse_dates)
    if len(df) == 0:
        raise SimException(f"No data found for {start.isoformat()} -> {end.isoformat()}")
    return df


def split_list(x):
    x = x.split(",") if x else []
    return np.array([int(x) for x in x])

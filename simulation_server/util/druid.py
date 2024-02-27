from typing import Union
from datetime import datetime, date, timedelta
import functools, os

import sqlalchemy as sqla
from sqlalchemy.sql import ColumnElement
from .misc import to_iso_duration
# from ..config import get_app_settings


def get_druid_engine(**kwargs):
    """ Get an SQLAlchemy Engine object towards production Druid """
    # Acquire the credentials
    DRUID_URL = os.environ['DRUID_SERVICE_URL']
    DRUID_SCHEME = DRUID_URL.split(':')[0]
    DRUID_HOST = DRUID_URL.split(':')[1][2:]
    DRUID_USERNAME = os.environ['DRUID_SERVICE_USERNAME']
    DRUID_PASSWORD = os.environ['DRUID_SERVICE_PASSWORD']
    DRUID_SQLA_URL = f'druid+https://{DRUID_USERNAME}:{DRUID_PASSWORD}@{DRUID_HOST}:443/druid/v2/sql/?header=true'

    # For some reason sqla/pydruid renders `cast(col, sqla.TIMESTAMP)` to `CAST(col AS LONG)`. LONG
    # isn't even a valid druid type. Several other cast types seem to be broken as well. This is a
    # manual override to make sqla render them properly.
    from sqlalchemy.ext.compiler import compiles
    cast_fixes = {
        sqla.types.TIMESTAMP: "TIMESTAMP",
        sqla.types.INT: "INT",
    }

    # TODO: Maybe should do this somewhere else to make sure it doesn't get called twice
    for (sqla_type, override) in cast_fixes.items():
        compiles(sqla_type, "druid")(lambda type_, compiler, override=override, **kw: override)

    kwargs = {
        "context": {
            # By default, druid does an approximate count for COUNT(DISTINCT), this turns that off.
            # You can still use APPROX_COUNT_DISTINCT to do approx count explicitly
            'useApproximateCountDistinct': False,
            # Return SQL Arrays as arrays instead of strings
            "sqlStringifyArrays": False,
            **kwargs.get("context", {}),
        },
        **kwargs,
    }

    return sqla.create_engine(DRUID_SQLA_URL, **kwargs)


@functools.cache
def get_table(name: str, engine: sqla.engine.Engine):
    """
    Retrieve a SQLAlchemy Table object by name from the Druid database. Handles switching between
    tables by env.
    This is a little awkward to do because we don't have an ORM.
    """
    # env = get_app_settings().env
    # dev_env = table_dev_env_overrides.get(name, 'prod')
    # env = env if (env == "prod" or dev_env == "current") else dev_env
    # env_prefix = "" if env == "prod" else f"{env}-"
    # name = f"{env_prefix}{name}"

    return sqla.Table(name, sqla.MetaData(), autoload_with=engine)


def to_timestamp(col) -> ColumnElement:
    """ Cast col to sqla TIMESTAMP if needed. """
    if isinstance(col, (datetime, date)):
        return sqla.cast(col.isoformat(), sqla.TIMESTAMP)
    else:
        col_type = getattr(col, 'type', None)
        if isinstance(col_type, sqla.TIMESTAMP) or col_type == sqla.TIMESTAMP:
            return col
        else:
            # TODO: There's some issue in Druid causing "ghost segments" or something. Restarted
            # ingest tasks cause several issues in the table, here it sometimes thinks that strings
            # are multivalue columns, and so the TIMESTAMP cast fails. Druid just kind of infers on
            # the fly whether a string column is multivalue so mv_to_string will work on plain
            # strings as well, and this is a workaround for part of the issue. Need to figure out
            # where things are getting corrupted.
            return sqla.cast(sqla.func.mv_to_string(col, ''), sqla.TIMESTAMP)


def time_floor(timestamp_col, granularity: Union[timedelta, str], origin=None) -> ColumnElement:
    """
    Wrapper around Druid time_floor.
    You'll typically want to use your query range start as origin.
    """
    # In our typical timeseries queries where we are grouping by the floored timestamp, if you leave
    # origin as the epoch, the first timestamp may be floored down less than your query range start/end
    # if start isn't aligned to granularity. This also leaves you with a "partial" point at the beginning
    # that doesn't include the data for its full interval. E.g. if you have 5s granularity, and start
    # is ...T00:00:3, then the first point would be ...T00:00:00 but only contain data for seconds 3 to 5.
    # If you set "origin" to the start of the query range the first timestamp returned by the query will
    # always be exactly the same as start. You can still have a partial data point at the end though.
    timestamp_col = to_timestamp(timestamp_col)
    granularity = granularity if isinstance(granularity, str) else to_iso_duration(granularity)

    if origin:
        return sqla.func.time_floor(timestamp_col, granularity, to_timestamp(origin))
    else:
        return sqla.func.time_floor(timestamp_col, granularity)


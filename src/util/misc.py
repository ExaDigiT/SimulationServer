from typing import TypeVar, Optional
from collections.abc import Collection
from datetime import datetime, timedelta
import itertools

from pydantic import TypeAdapter
import pandas as pd


def parse_iso_duration(iso_str: str):
    return pd.Timedelta(iso_str).to_pytimedelta()


def to_iso_duration(delta: timedelta):
    return pd.Timedelta(delta).isoformat()


DURATIONS = [parse_iso_duration(d) for d in [
    'PT1S', 'PT2S', 'PT5S', 'PT10S', 'PT15S', 'PT20S', 'PT30S',
    'PT1M', 'PT2M', 'PT5M', 'PT10M', 'PT15M', 'PT20M', 'PT30M',
    'PT1H', 'PT2H', 'PT6H', 'PT12H',
    'P1D', 'P2D', 'P7D', 'P14D',
    'P30D', 'P60D', 'P90D', 'P180D', # 'P1M', 'P2M', 'P3M', 'P6M' pd.Timedelta can't parse Y and M
    'P365D', 'P730D', 'P1825D', 'P3650D', #  P1Y, P2Y, P5Y, P10Y
]]
""" List of reasonable duration "breakpoints" """


def snap_duration(delta: timedelta, round = "down"):
    """
    Snaps delta to a duration in DURATION. By default uses the first duration smaller than delta.

    mode: "up" or "down", default down. Pass "up" to use the first duration larger than delta
    """
    if round == "up":
        return next((d for d in DURATIONS if d >= delta), DURATIONS[-1])
    else:
        return next((d for d in reversed(DURATIONS) if d <= delta), DURATIONS[0])


DatetimeValidator = TypeAdapter(datetime)
""" Just a TypeAdapter for parsing dates more flexibly than datetime.isoformat """


S = TypeVar("S", bound=str)
def expand_field_selectors(
    fields: Optional[Collection[S]],
    shorthands: dict[S, list[S]],
) -> list[S]:
    """
    Takes a list of strings and expands "shorthand" names. Used to select a set of field options,
    and have shorthands for common sets of fields like "all", or "power". Also removes duplicates.
    If shorthands contains "default", that will be used if fields is empty or None.
    Maintains order of the passed in fields.
    """
    fields_list: list[S] = list(fields) if fields else []
    if len(fields_list) == 0 and 'default' in shorthands:
        fields_list = ['default']

    fields_out: list[S] = []
    for field in fields_list:
        if field in shorthands: # Recursively expand shorthands
            fields_out.extend(expand_field_selectors(shorthands[field], shorthands))
        else:
            fields_out.append(field)

    fields_out = [*dict.fromkeys(fields_out)] # Dedup, preserver order
    return fields_out


def fields_from_selectors(shorthands: dict[str, list[str]]) -> tuple[str, ...]:
    fields = [
        *shorthands.keys(),
        *itertools.chain(*shorthands.values()),
    ]
    fields = [*dict.fromkeys(fields)] # Dedup, preserver order
    return tuple(fields)

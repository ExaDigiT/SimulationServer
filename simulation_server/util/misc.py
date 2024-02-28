from typing import TypeVar
from collections.abc import Iterable
from datetime import timedelta
import json

from pydantic import TypeAdapter


_TimedeltaAdapter = TypeAdapter(timedelta)

def parse_iso_duration(iso_str: str):
    return _TimedeltaAdapter.validate_strings(iso_str)


def to_iso_duration(delta: timedelta):
    return json.loads(_TimedeltaAdapter.dump_json(delta))


T = TypeVar("T"); U = TypeVar("U")
def pick(d: dict[T, U], keys: Iterable[T]) -> dict[T, U]:
    """ Returns a subset of a dictionary. `keys` that aren't in `d` will be ignored """
    key_set = set(keys)
    return {k: v for k, v in d.items() if k in key_set}


def omit(d: dict[T, U], keys: Iterable[T]) -> dict[T, U]:
    key_set = set(keys)
    return {k: v for k, v in d.items() if k not in key_set}

from typing import TypeVar, Any
from collections.abc import Iterable
from datetime import timedelta
import json, re

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


def parse_path(path: str):
    """ Parse simple json paths like "a.b[1]['d']" """
    part_pat = r"""^(\w+)|\.(\w+)|\['([^']+)'\]|\["([^"]+)"\]|\[(\d+)\]"""

    if not re.fullmatch(f"({part_pat})+", path):
        raise ValueError(f"Invalid path {repr(path)}")
    
    def parse_match(m: re.Match):
        # get index first (and only) group with data
        i = next(i + 1 for i, g in enumerate(m.groups()) if g)
        # Parse int only for `[0]` syntax
        return int(m[i]) if i == 5 else m[i]

    return [parse_match(m) for m in re.finditer(part_pat, path)]


def nest_dict(data: dict[str, Any]):
    """
    Takes a flat dictionary and "nests" it based on key names.

    keys containing dots or `[]` index syntax will be put under subdicts of the same path. E.g.
    `{"a": 1, "b.c": 2, "b.d": 3, "b['e'].f": 4}` wil return
    `{"a": 1, "b": {"c": 2, "d": 3, "e": {"f": 4}}}`
    """
    out = {}
    
    for k, v in data.items():
        path = parse_path(k)
        sub = out
        for part in path[:-1]:
            if part not in sub:
                sub[part] = {}
            sub = sub[part]
            if not isinstance(sub, dict):
                raise ValueError(f"{repr(k)} conflicts with another key")
        if path[-1] in sub:
            raise ValueError(f"{repr(k)} conflicts with another key")
        sub[path[-1]] = v

    return out


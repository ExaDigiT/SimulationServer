from datetime import timedelta
import json

from pydantic import TypeAdapter


_TimedeltaAdapter = TypeAdapter(timedelta)

def parse_iso_duration(iso_str: str):
    return _TimedeltaAdapter.validate_strings(iso_str)


def to_iso_duration(delta: timedelta):
    return json.loads(_TimedeltaAdapter.dump_json(delta))


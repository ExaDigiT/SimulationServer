""" Some general purpose models """
from typing import Generic, TypeVar, Any, Annotated as A, Optional
from datetime import timedelta
from pydantic import (
    BaseModel as PydanticBaseModel, ConfigDict, PlainSerializer, BeforeValidator, WithJsonSchema,
    AwareDatetime,
)

# TODO: A lot of this is copied from LVA, should consider if we could do better code reuse.

class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(
        use_attribute_docstrings = True,
    )


def _before_validator_num_timedelta(v):
    if isinstance(v, str):
        try:
            return float(v)
        except ValueError:
            return v
    return v


NumTimedelta = A[
    timedelta,
    BeforeValidator(_before_validator_num_timedelta),
    PlainSerializer(lambda d: d.total_seconds(), return_type=float),
    # Validator is not necessary, default validator will take float
    WithJsonSchema({'type': 'number', 'format': "time-delta"}),
]
"""
An annotated version of timedelta that serializes to float instead of ISO8601 duration strings.
It allows parsing as either number or ISO8601 strings or a float of seconds, and has a json schema
of `{"type": "number", "format": "time-delta"}`.
"""


def _validate_comma_separated_list(data):
    def smart_split(s):
        return [item.strip() for item in s.split(',') if item.strip()]

    if isinstance(data, list):
        items = []
        for item in data:
            if isinstance(item, str):
                items.extend(smart_split(item))
            else:
                items.append(item)
        return items
    elif isinstance(data, str):
        return smart_split(data)
    else:
        return data


T = TypeVar("T")
CommaSeparatedList = A[
    Optional[list[T]],
    BeforeValidator(_validate_comma_separated_list),
]
"""
Comma separated list.
Can pass multiple args, e.g. `arg=a&arg=b` or a single comma separated list, e.g. `arg=a,b`.
Trims strings and ignores trailing commas.
"""


class Page(BaseModel, Generic[T]):
    results: list[T]
    offset: int
    limit: int
    total_results: int

    @classmethod
    def model_parametrized_name(cls, params: tuple[type[Any], ...]) -> str:
        """ Set the name of instances of the generic type (which will be used in openapi.json) """
        return f'{params[0].__name__}Page'


class ObjectTimeseries(BaseModel, Generic[T]):
    start: AwareDatetime
    """ Start time of the timeseries """
    end: AwareDatetime
    """ End time of the timeseries """
    granularity: NumTimedelta
    """ Time between each timestamp """
    data: list[T]

    @classmethod
    def model_parametrized_name(cls, params: tuple[type[Any], ...]) -> str:
        """ Set the name of instances of the generic type (which will be used in openapi.json) """
        # TODO: Currently a bug in Pydantic means this doesn't actually work
        return f'{params[0].__name__}Timeseries'


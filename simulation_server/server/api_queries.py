from typing import Callable, Optional, Generic, TypeVar, NamedTuple, Union, Any, Literal, Annotated as A, cast
from collections.abc import Collection
from datetime import datetime, timedelta
import functools, re, inspect, math, dataclasses, itertools

from pydantic import BaseModel, model_validator, TypeAdapter, Field, AwareDatetime
from annotated_types import Ge, Predicate
import sqlalchemy as sqla
from sqlalchemy.sql import FromClause, ColumnElement
from fastapi import Query, HTTPException, Depends

from ..models.base import BaseModel, NumTimedelta
from ..util.druid import to_timestamp, time_floor

T = TypeVar("T")


### Helpers ###

def _steal_description(model: type[BaseModel], field: str):
    return Query(description=model.model_fields[field].description)


NumValidator = TypeAdapter(Union[int, float])
TimedeltaValidator = TypeAdapter(cast(timedelta, NumTimedelta))
DatetimeValidator = TypeAdapter(datetime)



DURATIONS = [TimedeltaValidator.validate_strings(d) for d in [
    'PT1S', 'PT2S', 'PT3S', 'PT4S', 'PT5S', 'PT10S', 'PT15S', 'PT20S', 'PT30S',
    'PT1M', 'PT2M', 'PT3M', 'PT4M', 'PT5M', 'PT10M', 'PT15M', 'PT20M', 'PT30M',
    'PT1H', 'PT2H', 'PT3H', 'PT4H', 'PT6H', 'PT8H', 'PT12H',
    'P1D', 'P2D', 'P3D', 'P4D', 'P5D', 'P7D', 'P14D',
    'P1M', 'P2M', 'P3M', 'P4M', 'P6M', # Assumes 30 days/month
    'P1Y', 'P2Y', 'P3Y', 'P4Y', 'P5Y', 'P10Y', # Assumes 365 days/year
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



### Basic Params ###

class Granularity(BaseModel):
    granularity: A[Optional[NumTimedelta],
        Predicate(lambda v: not v or v >= timedelta(seconds = 1)), # Ge doesn't work on timedelta
    ] = None
    """ The time interval between data points in the returned timeseries. """

    resolution: A[Optional[int], Ge(1)] = None
    """
    The (approximate) number of points that should be in the returned timeseries.
    You can only specify one of granularity or resolution.
    """

    @model_validator(mode='after')
    def check_granularity_xor_resolution(self):
        if self.granularity and self.resolution:
            raise ValueError('You can only specify one of default granularity or resolution')
        elif not self.granularity and not self.resolution:
            raise ValueError('You must specify one of granularity or resolution')
        else:
            return self


    def get(self, start: datetime, end: datetime) -> NumTimedelta:
        """
        If the user specified exact granularity return it, if they specified a resolution, calculate
        the granularly from the start and end times.
        """
        if not self.granularity and self.resolution:
            # Don't return 0 timedelta if start >= end
            range_td = max(end - start, timedelta(seconds = 1))
            if self.resolution == 1:
                # Special case for a resolution of 1, make sure its returns exactly one point
                # Round up to nearest second
                return timedelta(seconds=math.ceil(range_td.total_seconds()))
            else:
                return snap_duration(range_td / self.resolution, round = 'down')
        elif self.granularity and not self.resolution:
            return self.granularity
        else: # Shouldn't happen because of model_validator
            raise ValueError("You can only specify one of granularity or resolution")


def granularity_params(
    default_granularity: Optional[NumTimedelta] = None, default_resolution: Optional[int] = None,
):
    """ Dependency for common granularity/resolution prams for druid queries. """
    def dependency(
        granularity: A[Optional[NumTimedelta], _steal_description(Granularity, 'granularity')] = None,
        resolution: A[Optional[int], _steal_description(Granularity, 'resolution')] = None,
    ):
        # Granularity.check_granularity_xor_resolution will validate further
        if not granularity and not resolution:
            return Granularity(granularity=default_granularity, resolution=default_resolution)
        else:
            return Granularity(granularity=granularity, resolution=resolution)
    return dependency



class QuerySpan(BaseModel):
    start: AwareDatetime
    end: AwareDatetime
    granularity: NumTimedelta


    @model_validator(mode='after')
    def check_range(self):
        if self.start > self.end:
            raise ValueError('end must be >= start')
        return self


    def floor(self, timestamp_col):
        """ Returns the SQLA column floored (using start as origin so data points align to range)"""
        return time_floor(timestamp_col, self.granularity, self.start)


    def filter(self, timestamp_col):
        """ Returns the SQLA filters needed for this span """
        timestamp_col = to_timestamp(timestamp_col)
        return [to_timestamp(self.start) <= timestamp_col, timestamp_col < to_timestamp(self.end)]


def query_span_params(
    default_granularity: Optional[NumTimedelta] = None, default_resolution: Optional[int] = None,
):
    """ Dependency for common start/end/granularity/resolution prams for druid queries. """
    def dependency(
        start: AwareDatetime, end: AwareDatetime,
        granularity: A[Granularity, Depends(granularity_params(default_granularity, default_resolution))],
    ):
        return QuerySpan(start = start, end = end, granularity = granularity.get(start, end))
    return dependency



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


def get_selectors(shorthands: dict[str, list[str]]) -> tuple[str, ...]:
    """ For making a Literal like Literal[get_selectors(shorthands)] """
    assert all(isinstance(x, list) for x in shorthands.values())
    fields = [*shorthands.keys(), *itertools.chain(*shorthands.values())]
    return tuple(dict.fromkeys(fields)) # Dedup, preserve order



### Filters and Sort ###

@dataclasses.dataclass
class FilterOp(Generic[T]):
    name: str
    """ Name of the op """

    parse: Callable[[str], T]
    """ Function to parse the filter value into the correct data type """

    sql_query: Callable[[ColumnElement, T], ColumnElement]
    """Takes an sqlalchemy column and the value, and returns an sqlalchemy filter expression """

    py_query: Callable[[Any, T], bool]
    """Takes a python value and the filter value, and returns true or false """


class ApiFieldType:
    name: str

    filter_ops: dict[str, FilterOp]
    """
    dict of op name to a function that takes a sqlalchemy column and does the desired operation
    Function should raise a ValueError if there is a parsing issue.
    """

    sql_sort_conv: Callable[[ColumnElement], ColumnElement]
    """ Conversion function to run on the column before sorting. Only applicable to queries """

    def __init__(self,
        name: str,
        filter_ops: list[FilterOp],
        sql_sort_conv: Optional[Callable[[ColumnElement], ColumnElement]] = None,
    ):
        self.name = name
        self.filter_ops = {op.name: op for op in filter_ops}
        self.sql_sort_conv = sql_sort_conv if sql_sort_conv else (lambda x: x)



def _create_default_field_types():
    def parse_str(val: str): return val
    def parse_num(val: str): return NumValidator.validate_strings(val)
    def parse_date(val: str): return DatetimeValidator.validate_strings(val)
    def parse_td(val: str): return TimedeltaValidator.validate_strings(val).total_seconds()
    def parse_list(func: Callable):
        """ Return a function to parse a list with datatype """
        return lambda val: [func(d) for d in re.split(",", val) if d]
    def contains_one_of(col: ColumnElement, vals: list[Any]):
        # Could do this with array_overlap if I figure out how to make sqla output an array literal
        if len(vals) == 0:
            return sqla.or_(False)
        else:
            return sqla.or_(*[sqla.func.array_contains(col, v) for v in vals])

    simple_types = [
        ApiFieldType('string',
            filter_ops = [
                FilterOp("contains",
                    parse = parse_str,
                    sql_query = lambda col, val: col.contains(val),
                    py_query = lambda col, val: val in col,
                ),
                FilterOp("not_contains",
                    parse = parse_str,
                    sql_query = lambda col, val: sqla.not_(col.contains(val)),
                    py_query = lambda col, val: val not in col,
                ),
                FilterOp("eq",
                    parse = parse_str,
                    sql_query = lambda col, val: col == val,
                    py_query = lambda col, val: col == val,
                ),
                FilterOp("neq",
                    parse = parse_str,
                    sql_query = lambda col, val: col != val,
                    py_query = lambda col, val: col != val,
                ),
                FilterOp("starts_with",
                    parse = parse_str,
                    sql_query = lambda col, val: col.startswith(val),
                    py_query = lambda col, val: col.startswith(val),
                ),
                FilterOp("ends_with",
                    parse = parse_str,
                    sql_query = lambda col, val: col.endswith(val),
                    py_query = lambda col, val: col.endswith(val),
                ),
                FilterOp("one_of",
                    parse = parse_list(parse_str),
                    sql_query = lambda col, val: col.in_(val),
                    py_query = lambda col, val: col in val,
                ),
                FilterOp("not_one_of",
                    parse = parse_list(parse_str),
                    sql_query = lambda col, val: sqla.not_(col.in_(val)),
                    py_query = lambda col, val: col not in val,
                ),
                FilterOp("min_len",
                    parse = parse_num,
                    sql_query = lambda col, val: sqla.func.length(col) >= val,
                    py_query = lambda col, val: len(col) >= val,
                ),
                FilterOp("max_len",
                    parse = parse_num,
                    sql_query = lambda col, val: sqla.func.length(col) <= val,
                    py_query = lambda col, val: len(col) <= val,
                ),
            ],
        ),
        ApiFieldType('number',
            filter_ops = [
                FilterOp("gt",
                    parse = parse_num,
                    sql_query = lambda col, val: col > val,
                    py_query = lambda col, val: col > val,
                ),
                FilterOp("gte",
                    parse = parse_num,
                    sql_query = lambda col, val: col >= val,
                    py_query = lambda col, val: col >= val,
                ),
                FilterOp("lt",
                    parse = parse_num,
                    sql_query = lambda col, val: col < val,
                    py_query = lambda col, val: col < val,
                ),
                FilterOp("lte",
                    parse = parse_num,
                    sql_query = lambda col, val: col <= val,
                    py_query = lambda col, val: col <= val,
                ),
                FilterOp("eq",
                    parse = parse_num,
                    sql_query = lambda col, val: col == val,
                    py_query = lambda col, val: col == val,
                ),
                FilterOp("neq",
                    parse = parse_num,
                    sql_query = lambda col, val: col != val,
                    py_query = lambda col, val: col != val,
                ),
                FilterOp("one_of",
                    parse = parse_list(parse_num),
                    sql_query = lambda col, val: col.in_(val),
                    py_query = lambda col, val: col in val,
                ),
                FilterOp("not_one_of",
                    parse = parse_list(parse_num),
                    sql_query = lambda col, val: sqla.not_(col.in_(val)),
                    py_query = lambda col, val: col not in val,
                ),
            ],
        ),
        ApiFieldType('date',
            filter_ops = [
                FilterOp("gt",
                    parse = parse_date,
                    sql_query = lambda col, val: to_timestamp(col) > to_timestamp(val),
                    py_query = lambda col, val: col > val,
                ),
                FilterOp("gte",
                    parse = parse_date,
                    sql_query = lambda col, val: to_timestamp(col) >= to_timestamp(val),
                    py_query = lambda col, val: col >= val,
                ),
                FilterOp("lt",
                    parse = parse_date,
                    sql_query = lambda col, val: to_timestamp(col) < to_timestamp(val),
                    py_query = lambda col, val: col < val,
                ),
                FilterOp("lte",
                    parse = parse_date,
                    sql_query = lambda col, val: to_timestamp(col) <= to_timestamp(val),
                    py_query = lambda col, val: col <= val,
                ),
                FilterOp("eq",
                    parse = parse_date,
                    sql_query = lambda col, val: to_timestamp(col) == to_timestamp(val),
                    py_query = lambda col, val: col == val,
                ),
                FilterOp("neq",
                    parse = parse_date,
                    sql_query = lambda col, val: to_timestamp(col) != to_timestamp(val),
                    py_query = lambda col, val: col != val,
                ),
                FilterOp("one_of",
                    parse = parse_list(parse_date),
                    sql_query = lambda col, val: to_timestamp(col).in_([
                        to_timestamp(d) for d in val
                    ]),
                    py_query = lambda col, val: col in val,
                ),
                FilterOp("not_one_of",
                    parse = parse_list(parse_date),
                    sql_query = lambda col, val: sqla.not_(to_timestamp(col).in_([
                        to_timestamp(d) for d in val
                    ])),
                    py_query = lambda col, val: col not in val,
                ),
            ],
            sql_sort_conv = to_timestamp,
        ),
        ApiFieldType('timedelta',
            # Assumes timedelta is stored in the DB as a float of seconds
            filter_ops = [
                FilterOp("gt",
                    parse = parse_td,
                    sql_query = lambda col, val: col > val,
                    py_query = lambda col, val: col > val,
                ),
                FilterOp("gte",
                    parse = parse_td,
                    sql_query = lambda col, val: col >= val,
                    py_query = lambda col, val: col >= val,
                ),
                FilterOp("lt",
                    parse = parse_td,
                    sql_query = lambda col, val: col < val,
                    py_query = lambda col, val: col < val,
                ),
                FilterOp("lte",
                    parse = parse_td,
                    sql_query = lambda col, val: col <= val,
                    py_query = lambda col, val: col <= val,
                ),
                FilterOp("eq",
                    parse = parse_td,
                    sql_query = lambda col, val: col == val,
                    py_query = lambda col, val: col == val,
                ),
                FilterOp("neq",
                    parse = parse_td,
                    sql_query = lambda col, val: col != val,
                    py_query = lambda col, val: col != val,
                ),
                FilterOp("one_of",
                    parse = parse_list(parse_td),
                    sql_query = lambda col, val: col.in_(val),
                    py_query = lambda col, val: col in val,
                ),
                FilterOp("not_one_of",
                    parse = parse_list(parse_td),
                    sql_query = lambda col, val: sqla.not_(col.in_(val)),
                    py_query = lambda col, val: col not in val,
                ),
            ],
        ),
    ]

    parsers = {
        'string': parse_str,
        'number': parse_num,
        'date': parse_date,
        'timedelta': parse_td,
    }

    array_types = []
    for field_type in simple_types:
        parser = parsers[field_type.name]
        array_types.append(ApiFieldType(f'array[{field_type.name}]',
            filter_ops = [
                FilterOp("contains",
                    parse = parser,
                    sql_query = lambda col, val: sqla.func.array_contains(col, val),
                    py_query = lambda col, val: val in col,
                ),
                FilterOp("not_contains",
                    parse = parser,
                    sql_query = lambda col, val: sqla.not_(sqla.func.array_contains(col, val)),
                    py_query = lambda col, val: val not in col,
                ),
                # TODO: Current version of druid doesn't support array equality. We can add these when we upgrade
                # FilterOp("eq",
                #     parse = parser,
                #     sql_query = lambda col, val: array_equals(col, val),
                #     py_query = lambda col, val: col == val,
                # ),
                # FilterOp("neq",
                #     parse = parser,
                #     sql_query = lambda col, val: col != val,
                #     py_query = lambda col, val: col != val,
                # ),
                FilterOp("min_len",
                    parse = parse_num,
                    sql_query = lambda col, val: sqla.func.array_length(col) >= val,
                    py_query = lambda col, val: len(col) >= val,
                ),
                FilterOp("max_len",
                    parse = parse_num,
                    sql_query = lambda col, val: sqla.func.array_length(col) <= val,
                    py_query = lambda col, val: len(col) <= val,
                ),
                FilterOp("contains_one_of",
                    parse = parse_list(parser),
                    sql_query = lambda col, val: contains_one_of(col, val),
                    py_query = lambda col, val: not set(col).isdisjoint(val),

                ),
                FilterOp("not_contains_one_of",
                    parse = parse_list(parser),
                    sql_query = lambda col, val: sqla.not_(contains_one_of(col, val)),
                    py_query = lambda col, val: set(col).isdisjoint(val),
                ),
            ],
            sql_sort_conv = lambda col: sqla.func.array_to_string(col, ","),
        ))

    return [*simple_types, *array_types]


DEFAULT_FIELD_TYPES = _create_default_field_types()


class Filters:
    """
    Used to create filters for use in Druid

    Use filter_params as a FastAPI dep to create an instance of this class
    """

    class Filter(NamedTuple):
        field: str
        op: FilterOp
        value: Any


    filters: list[Filter]
    """ (field_name, FilterOp, value) tuples """


    def __init__(self, filters: Optional[list[Filter]] = None):
        """ You'll usually want to use filter_params or parse_filters instead. """
        self.filters = filters or []


    @classmethod
    def parse_filters(cls,
        fields: dict[str, str],
        field_types: list[ApiFieldType],
        filters: dict[str, Optional[list[str]]],
    ):
        field_types_dict = {ft.name: ft for ft in field_types}

        parsed: list[Filters.Filter] = []
        for field_name, field_filters in filters.items():
            # FastAPI has already checked that field_name is a valid field
            field_type = field_types_dict[fields[field_name]]
            field_filters = field_filters if field_filters else []

            for field_filter in field_filters:
                if ':' not in field_filter:
                    raise ValueError(f'Invalid filter "{field_filter}", expected format "op:value", e.g. "eq:42"')
                op_name, value_str = field_filter.split(":", maxsplit = 1)

                if op_name not in field_type.filter_ops:
                    raise ValueError(f'Unknown operator "{op_name}" for filter {field_name}')
                op = field_type.filter_ops[op_name]

                try:
                    value = op.parse(value_str)
                except ValueError as e:
                    raise ValueError(f'Filter {field_name}="{op_name}:{value_str}" invalid: {e}') from e

                parsed.append(Filters.Filter(field_name, op, value))

        return Filters(parsed)


    def get(self, field: str) -> list[Filter]:
        """ Return filters for a specific field """
        return [f for f in self.filters if f.field == field]


    def filter_sql(self, cols: Union[dict[str, ColumnElement], FromClause]) -> list[ColumnElement]:
        """
        Returns a list of sqlalchemy column operators suitable for use in a where clause.

        Pass a mapping of field_name to SQLAlchemy column objects. Any fields omitted from the cols
        dict will be skipped. If all fields map directly to table column names you can pass the
        table instead.
        """
        if isinstance(cols, FromClause):
            cols = {field: cols.c[field] for field, _, _ in self.filters}
        return [
            f.op.sql_query(cols[f.field], f.value)
            for f in self.filters
            if f.field in cols
        ]


    FilterColsType = Union[None, dict[str, Callable[[T], Any]], Callable[[T, str], Any]]
    def filter_list(self, data: list[T], cols: FilterColsType = None) -> list[T]:
        """
        Filters a python list by filters.
        
        If cols is passed a dict, it should map api field names to accessor functions. Any fields
        omitted from the cols dict will be skipped. If cols dict is left None, it will default to
        just using the index operator with the field name. You can also pass cols as a single
        function that takes both the item and the field name.
        """
        selected = {field for field, _, _ in self.filters}
        if cols is None:
            col_func = lambda d, field_name: d[field_name]
        elif isinstance(cols, dict):
            col_func = lambda d, field_name: cols[field_name](d)
            selected = set(cols.keys())
        else:
            col_func = cols

        def filter_func(elem):
            for f in self.filters:
                if f.field in selected and not f.op.py_query(col_func(elem, f.field), f.value):
                    return False
            return True

        return [e for e in data if filter_func(e)]


def filter_params( # maybe rename to filters
    fields: dict[str, str], extra_field_types: Optional[list[ApiFieldType]] = None
) -> Callable[..., Filters]:
    """
    Dependency for generic filtering logic for sqlalchemy queries.

    Pass a dict mapping api fields to a "field_type". It will make a get parameter for each field.
    Each field is formated as `op:value`. E.g. you can pass `allocation_id=eg:1` or
    `name=contains:bob`.

    Each api field has a "type", which identifies a `ApiFieldType` object in either
    DEFAULT_FIELD_TYPES or any `extra_field_types` you specify. The "op" must match one of the
    operators in the corresponding `ApiFieldType` for the field.
    """
    extra_field_types = extra_field_types or []
    field_types = [*DEFAULT_FIELD_TYPES, *extra_field_types]
    field_types_dict = {ft.name: ft for ft in field_types}

    def dep(**kwargs):
        try:
            return Filters.parse_filters(fields, field_types, kwargs)
        except ValueError as e:
            raise HTTPException(422, str(e)) from e

    # Create a function with the right annotations/signature for FastAPI to parse. FastAPI needs to
    # see a Query() annotation or it will default the lists to body arguments.
    # Before Pydantic2 we used a Pydantic model's __init__ function to make the right annotations.
    # However, Pydantic2 also uses Annotated syntax for its Fields, and Query is a subclass of
    # Pydantic Field, so Pydantic2 eats the FasAPI Query annotation. So instead we make our own
    # function signature here ourselves by just setting the magic __signature__ attribute (this is
    # how Pydantic actually does it internally for __init__ as well).
    params = []
    for field, field_type in fields.items():
        filter_ops = list(field_types_dict[field_type].filter_ops.keys())
        description = (
            f'Filters on {field}. Can be specified multiple times. Format as `op:filter_value`. ' +
            f'Available ops: {", ".join(filter_ops)}'
        )
        param = inspect.Parameter(
            name = field,
            kind = inspect.Parameter.POSITIONAL_OR_KEYWORD,
            # Use Query() to prevent lists from getting turned to body params
            annotation = A[Optional[list[str]], Query(description = description)],
            default = None,
        )
        params.append(param)

    dep.__signature__ = inspect.Signature(params, return_annotation = Filters)
    return dep


SortDirection = Literal['asc', 'desc']

class Sort:
    """
    Used to create sort options for use in Druid.

    Use sort_params to create instances of this class
    """

    class FieldSort(NamedTuple):
        field: str
        field_type: ApiFieldType
        dir: SortDirection


    sort: list[FieldSort]

    def __init__(self, sort: Optional[list[FieldSort]] = None):
        """ You'll usually want to use sort_params or parse_sort instead. """
        self.sort = sort or []


    @staticmethod
    def split_sort(sort_field: str) -> tuple[SortDirection, str]:
        if ':' not in sort_field:
            raise ValueError(
                f'Invalid sort "{sort_field}", expected format "dir:field", e.g. "asc:name"'
            )
        dir, field_name = sort_field.split(':', 1)
        if dir not in ['asc', 'desc']:
            raise ValueError(f'Invalid sort direction {dir}, expected "asc" or "desc"')
        return dir, field_name # type: ignore


    @classmethod
    def parse_sort(cls,
        fields: dict[str, str],
        field_types: list[ApiFieldType],
        default_sort: list[str],
        sort: list[str],
    ):
        field_types_dict = {ft.name: ft for ft in field_types}
        default_sort_parsed = [Sort.split_sort(s) for s in default_sort]
        sort_parsed = [Sort.split_sort(s) for s in sort]

        # Add default sorts to end unless they are specified explicitly
        sorted_fields: set[str] = {field for dir, field in sort_parsed}
        sort_parsed.extend(s for s in default_sort_parsed if s[1] not in sorted_fields)

        result: list[Sort.FieldSort] = []
        sorted_fields = set()
        for dir, field_name in sort_parsed:
            if field_name not in fields:
                raise ValueError(f"Unknown sort field {field_name}")
            if field_name in sorted_fields:
                raise HTTPException(422, f"Sorted field {field_name} multiple times")
            field_type = field_types_dict[fields[field_name]]
            result.append(Sort.FieldSort(field_name, field_type, dir))
            sorted_fields.add(field_name)

        return Sort(result)


    @property
    def sort_order(self) -> list[tuple[str, SortDirection]]:
        """ Returns the sort list passed as (field_name, asc/desc) tuples """
        return [(s.field, s.dir) for s in self.sort]


    def sort_sql(self, cols: Union[dict[str, ColumnElement], FromClause]) -> list[ColumnElement]:
        """
        Returns a list of sqlalchemy column operators suitable for use in a order_by clause.
        Pass a mapping of `ApiField.name` to SQLAlchemy column objects. Any fields omitted from
        the cols dict will be skipped. If all fields map directly to table names, you can pass the
        table instead.
        """
        if isinstance(cols, FromClause):
            cols = {field: cols.c[field] for field, _, _ in self.sort}

        result: list[ColumnElement] = []
        for s in self.sort:
            if s.field in cols:
                col = s.field_type.sql_sort_conv(cols[s.field])
                result.append(col.desc() if s.dir == 'desc' else col.asc())

        return result


    SortColsType = Union[None, dict[str, Callable[[T], Any]], Callable[[T, str], Any]]
    def sort_list(self, data: list[T], cols: SortColsType = None) -> list[T]:
        """
        Sorts a python list of data.
        
        If cols is passed a dict, it should map api field names to accessor functions. Any fields
        omitted from the cols dict will be skipped. If cols dict is left None, it will default to
        just using the index operator with the field name. You can also pass cols as a single
        function that takes both the item and the field name.
        """
        selected = {field for field, _, _, in self.sort}
        if cols is None:
            col_func = lambda d, field_name: d[field_name]
        elif isinstance(cols, dict):
            col_func = lambda d, field_name: cols[field_name](d)
            selected = set(cols.keys())
        else:
            col_func = cols

        # Use compare func instead of key so we can change order on each field
        def cmp(a, b):
            for s in self.sort:
                if s.field in selected:
                    a_val, b_val = col_func(a, s.field), col_func(b, s.field)
                    direction = 1 if s.dir == "asc" else -1

                    if a_val < b_val:
                        return -direction
                    elif a_val > b_val:
                        return direction
            return 0 # Equal

        return sorted(data, key = functools.cmp_to_key(cmp))


def sort_params(
    fields: dict[str, str], default_sort: Optional[list[str]] = None,
    extra_field_types: Optional[list[ApiFieldType]] = None,
) -> Callable[..., Sort]:
    """
    Dependency for generic sorting logic for sqlalchemy queries.
    It will add a "sort" get parameter that takes a list of field names in the format
    `asc:my_field` or `desc:my_field`.
    """
    extra_field_types = extra_field_types or []
    default_sort = default_sort or []
    field_types = [*DEFAULT_FIELD_TYPES, *extra_field_types]
    description = (
        "Sort order of the return results. Can be specified multiple times to sort by multiple " +
        "values. Format as either `asc:field` or `desc:field`. Available fields: " +
        ', '.join(fields.keys())
    )

    def dependency(sort: A[Optional[list[str]], Query(description=description)] = None):
        sort = sort or []
        try:
            return Sort.parse_sort(
                fields = fields,
                field_types = field_types,
                default_sort = default_sort,
                sort = sort,
            )
        except ValueError as e:
            raise HTTPException(422, str(e)) from e

    return dependency


from typing import Callable, Optional, Generic, TypeVar, NamedTuple, Union, Any, Literal, Annotated as A
from datetime import datetime, timedelta
import functools, re, inspect, math, dataclasses

from pydantic import model_validator, TypeAdapter, Field, AwareDatetime
from annotated_types import Ge, Predicate
import sqlalchemy as sqla
from sqlalchemy.sql import FromClause, ColumnElement
from sqlalchemy.sql.operators import ColumnOperators
from fastapi import Query, HTTPException, Depends

from ..models.base import BaseModel, NumTimedelta
from .misc import DatetimeValidator, snap_duration
from .druid import to_timestamp, time_floor

# TODO: Most of these utils are copied from LVA. Need to consider how to do better coder resuse.


def steal_description(model: type[BaseModel], field: str):
    return Query(description=model.model_fields[field].description)


class Granularity(BaseModel):
    granularity: A[Optional[NumTimedelta],
        Predicate(lambda v: not v or v > timedelta(seconds = 0)), # Ge doesn't work on timedelta
        Field(description = "The time interval between data points in the returned timeseries."),
    ] = None
    resolution: A[Optional[int],
        Ge(1),
        Field(description = """
            The (approximate) number of points that should be in the returned timeseries.
            You can only specify one of granularity or resolution.
        """),
    ] = None


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
    assert not default_granularity or not default_resolution, \
        "You can only specify one of default granularity or resolution"

    def dependency(
        granularity: A[Optional[NumTimedelta], steal_description(Granularity, 'granularity')] = None,
        resolution: A[Optional[int], steal_description(Granularity, 'resolution')] = None,
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
        """ Returns SQLA column to floor (using start as origin so data points align to range)"""
        return time_floor(timestamp_col, self.granularity, self.start)

    def filter(self, timestamp_col):
        """ Returns the SQLA filters needed for this span """
        timestamp_col = to_timestamp(timestamp_col)
        return [
            timestamp_col >= to_timestamp(self.start),
            timestamp_col < to_timestamp(self.end),
        ]


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



class ApiField(NamedTuple):
    """ For use in filter_params. Represents one field that can be filtered """

    name: str
    """ Name of the field in the API params """

    type: type
    """ Type annotation of the field """

    filter_ops: str
    """" Name of the ApiFilterOps that should be used for this field. """


FilterOpT = TypeVar("FilterOpT")
@dataclasses.dataclass
class FilterOp(Generic[FilterOpT]): # Generic NamedTuples are python3.11 feature
    name: str
    """ Name of the op """

    parse: Callable[[str], FilterOpT]
    """ Function to parse the string into the correct data type """

    query: Callable[[ColumnElement, FilterOpT], ColumnOperators]
    """Takes an sqlalchemy column and the value, and returns an sqlalchemy filter expression """

    literal: Callable[[Any, FilterOpT], bool]
    """Takes a literal value and the filter value, and returns true or false """


class ApiFilterOps:
    name: str

    ops: dict[str, FilterOp]
    """
    dict of op name to a function that takes a sqlalchemy column and does the desired operation
    Function should raise a ValueError if there is a parsing issue.
    """

    sort_conv: Optional[Callable[[ColumnElement], ColumnOperators]]
    """ Conversion function to run on the column before sorting. Only applicable to queries """

    def __init__(self, name: str, ops: list[FilterOp], sort_conv = None):
        self.name = name
        self.ops = {op.name: op for op in ops}
        self.sort_conv = sort_conv


def parse_str(val: str): return val
def parse_str_list(val: str) -> list[str]: return [d for d in re.split(",", val) if d]
NumValidator = TypeAdapter(Union[int, float])
def parse_num(val: str): return NumValidator.validate_strings(val)
def parse_num_list(val: str): return [parse_num(d) for d in parse_str_list(val)]
def parse_date(val: str): return DatetimeValidator.validate_strings(val)
def parse_date_list(val: str): return [parse_date(d) for d in parse_str_list(val)]
# TODO because of the hacky string replace logic we are doing in the jobs endpoint binding a list
# doesn't work. When we fix that, we can change this to a simple `col.in_`
def or_based_in(col: ColumnElement, vals: list[Any]):
    return sqla.or_(False) if len(vals) == 0 else sqla.or_(*[col == v for v in vals])


DEFAULT_FIELD_TYPES: list[ApiFilterOps] = [
    ApiFilterOps('string',
        [
            FilterOp("contains",
                parse = parse_str,
                query = lambda col, val: col.contains(val),
                literal = lambda col, val: val in col,
            ),
            FilterOp("not_contains",
                parse = parse_str,
                query = lambda col, val: sqla.not_(col.contains(val)),
                literal = lambda col, val: val not in col,
            ),
            FilterOp("eq",
                parse = parse_str,
                query = lambda col, val: col == val,
                literal = lambda col, val: col == val,
            ),
            FilterOp("neq",
                parse = parse_str,
                query = lambda col, val: col != val,
                literal = lambda col, val: col != val,
            ),
            FilterOp("starts_with",
                parse = parse_str,
                query = lambda col, val: col.startswith(val),
                literal = lambda col, val: col.startswith(val),
            ),
            FilterOp("ends_with",
                parse = parse_str,
                query = lambda col, val: col.endswith(val),
                literal = lambda col, val: col.endswith(val),
            ),
            FilterOp("one_of",
                parse = parse_str_list,
                query = lambda col, val: or_based_in(col, val),
                literal = lambda col, val: col in val,
            ),
            FilterOp("not_one_of",
                parse = parse_str_list,
                query = lambda col, val: sqla.not_(or_based_in(col, val)),
                literal = lambda col, val: col not in val,
            ),
            FilterOp("min_len",
                parse = parse_num,
                query = lambda col, val: sqla.func.length(col) >= val,
                literal = lambda col, val: len(col) >= val,
            ),
            FilterOp("max_len",
                parse = parse_num,
                query = lambda col, val: sqla.func.length(col) <= val,
                literal = lambda col, val: len(col) <= val,
            ),
        ]
    ),
    ApiFilterOps('number',
        [
            FilterOp("gt",
                parse = parse_num,
                query = lambda col, val: col > val,
                literal = lambda col, val: col > val,
            ),
            FilterOp("gte",
                parse = parse_num,
                query = lambda col, val: col >= val,
                literal = lambda col, val: col >= val,
            ),
            FilterOp("lt",
                parse = parse_num,
                query = lambda col, val: col < val,
                literal = lambda col, val: col < val,
            ),
            FilterOp("lte",
                parse = parse_num,
                query = lambda col, val: col <= val,
                literal = lambda col, val: col <= val,
            ),
            FilterOp("eq",
                parse = parse_num,
                query = lambda col, val: col == val,
                literal = lambda col, val: col == val,
            ),
            FilterOp("neq",
                parse = parse_num,
                query = lambda col, val: col != val,
                literal = lambda col, val: col != val,
            ),
            FilterOp("one_of",
                parse = parse_num_list,
                query = lambda col, val: or_based_in(col, val),
                literal = lambda col, val: col in val,
            ),
            FilterOp("not_one_of",
                parse = parse_num_list,
                query = lambda col, val: sqla.not_(or_based_in(col, val)),
                literal = lambda col, val: col not in val,
            ),
        ]
    ),
    ApiFilterOps('date',
        [
            FilterOp("gt",
                parse = parse_date,
                query = lambda col, val: to_timestamp(col) > to_timestamp(val),
                literal = lambda col, val: col > val,
            ),
            FilterOp("gte",
                parse = parse_date,
                query = lambda col, val: to_timestamp(col) >= to_timestamp(val),
                literal = lambda col, val: col >= val,
            ),
            FilterOp("lt",
                parse = parse_date,
                query = lambda col, val: to_timestamp(col) < to_timestamp(val),
                literal = lambda col, val: col < val,
            ),
            FilterOp("lte",
                parse = parse_date,
                query = lambda col, val: to_timestamp(col) <= to_timestamp(val),
                literal = lambda col, val: col <= val,
            ),
            FilterOp("eq",
                parse = parse_date,
                query = lambda col, val: to_timestamp(col) == to_timestamp(val),
                literal = lambda col, val: col == val,
            ),
            FilterOp("neq",
                parse = parse_date,
                query = lambda col, val: to_timestamp(col) != to_timestamp(val),
                literal = lambda col, val: col != val,
            ),
            FilterOp("one_of",
                parse = parse_date_list,
                query = lambda col, val: or_based_in(to_timestamp(col),
                    [to_timestamp(d) for d in val]
                ),
                literal = lambda col, val: col in val,
            ),
            FilterOp("not_one_of",
                parse = parse_date_list,
                query = lambda col, val: sqla.not_(or_based_in(to_timestamp(col),
                    [to_timestamp(d) for d in val]
                )),
                literal = lambda col, val: col not in val,
            ),
        ],
        sort_conv = to_timestamp,
    )
]


class Filters:
    """
    Used to create filters for use in Druid

    Use filter_params as a FastAPI dep to create an instance of this class
    """

    filters: list[tuple[ApiField, FilterOp, Any]]
    """ (ApiField, FilterOp, value) tuples """

    def __init__(self,
        fields: Optional[list[ApiField]] = None,
        field_types: Optional[list[ApiFilterOps]] = None,
        filters: Optional[list[tuple[str, str, str]]] = None
    ):
        """
        Creates a Filters object.
        
        fields: list of recognized ApiField
        field_types: list of recognized field_types
        filters: list of the actual filters, as (field_name, op, value) tuples.
        """
        self.filters = Filters._parse_filters(
            fields or [],
            field_types or DEFAULT_FIELD_TYPES,
            filters or [],
        )

    @classmethod
    def _parse_filters(cls,
        fields: list[ApiField], field_types: list[ApiFilterOps],
        filters: list[tuple[str, str, str]],
    ) -> list[tuple[ApiField, FilterOp, str]]:
        fields_dict = {f.name: f for f in fields}
        field_types_dict = {ft.name: ft for ft in field_types}

        parsed: list[tuple[ApiField, FilterOp, Any]] = []
        for field_name, op_name, value_str in filters:
            field = fields_dict[field_name]
            field_type = field_types_dict[field.filter_ops]
         
            if op_name not in field_type.ops:
                raise ValueError(f'Unknown operator "{op_name}" for filter {field.name}')
    
            op = field_type.ops[op_name]
            try:
                value = op.parse(value_str)
            except ValueError as e:
                raise ValueError(f'Filter {field.name}="{op_name}:{value_str}" invalid: {e}') from e

            parsed.append((field, op, value))

        return parsed


    def get_field_filters(self, name: str):
        """ Return filters for a specific field """
        return [f for f in self.filters if f[0].name == name]


    def apply(self, cols: Union[dict[str, ColumnElement], FromClause]) -> list[ColumnOperators]:
        """
        Returns a list of sqlalchemy column operators suitable for use in a where clause.
        Pass a mapping of `ApiField.name` to SQLAlchemy column objects. Any fields omitted from
        the cols dict will be skipped. If all fields map directly to table names, you can pass the
        table instead.
        """
        if isinstance(cols, FromClause):
            cols = {field.name: cols.c[field.name] for field, _, _ in self.filters}
        results: list[ColumnOperators] = []
        for field, op, value in self.filters:
            if field.name in cols:
                results.append(op.query(cols[field.name], value))
        return results


    Datum = TypeVar("Datum")
    FilterColsType = Union[None, dict[str, Callable[[Datum], Any]], Callable[[Datum, str], Any]]
    def filter_list(self, data: list[Datum], cols: FilterColsType = None) -> list[Datum]:
        """
        Filters a python list of data by filters. Pass a dict mapping api fields to accessors.
        Any fields omitted from the cols dict will be skipped. If cols dict is left None, it will
        default to just using the index operator with the field name. You can also pass cols as a
        single function that takes both the Datum and the field name.
        """
        selected = {field.name for field, _, _ in self.filters}
        if cols is None:
            col_func = lambda d, field_name: d[field_name]
        elif isinstance(cols, dict):
            col_func = lambda d, field_name: cols[field_name](d)
            selected = set(cols.keys())
        else:
            col_func = cols


        def filter_func(elem):
            for field, op, value in self.filters:
                if field.name in selected and not op.literal(col_func(elem, field.name), value):
                    return False
            return True

        results: list = []
        for elem in data:
            if filter_func(elem):
                results.append(elem)

        return results


def filter_params(
    fields: list[ApiField], extra_types: Optional[list[ApiFilterOps]] = None
) -> Callable[..., Filters]:
    """
    Dependency for generic filtering logic for sqlalchemy queries.
    Pass a list of `ApiField`, and it will make a get parameter for each field. Each field is
    formated as `op:value`. E.g. you can pass `allocation_id=eg:1` or `name=contains:bob`.

    Each ApiField has a "type", which identifies a `ApiFilterOps` object in either the default
    FieldTypes or any `extra_types` you specify. The "op" must match one of the operators in the
    corresponding `ApiFilterOps` for the field.
    """
    extra_types = extra_types or []
    field_types = [*DEFAULT_FIELD_TYPES, *extra_types]
    field_types_ops = {ft.name: ft for ft in field_types}

    # Create a function with the right annotations/signature for FastAPI to parse. FastAPI needs to
    # see a Query() annotation or it will default the lists to body arguments.
    # Before Pydantic2 we used a Pydantic model's __init__ function to make the right annotations.
    # However, Pydantic2 also uses Annotated syntax for its Fields, and Query is a subclass of
    # Pydantic Field, so Pydantic2 eats the FasAPI Query annotation. So instead we make our own
    # function signature here ourselves by just setting the magic __signature__ attribute (this is
    # how Pydantic actually does it internally for __init__ as well).
    params = [
        # Use Query() to prevent lists from getting turned to body params
        inspect.Parameter(
            name = field.name,
            kind = inspect.Parameter.POSITIONAL_OR_KEYWORD,
            annotation = A[Optional[list[str]], Query(description = 
                f'Filters on {field.name}. Can be specified multiple times. Format as ' +
                '`op:filter_value`. Available ops: ' +
                ", ".join(field_types_ops[field.filter_ops].ops.keys())
            )],
            default = None,
        )
        for field in fields
    ]
    sig = inspect.Signature(params, return_annotation=Filters)

    def dependency(**kwargs):
        # Flatten the filters
        flat_filters: list[tuple[str, str, str]] = []
        for field_name, field_filters in kwargs.items():
            if field_filters is not None:
                for field_filter in field_filters:
                    if ':' not in field_filter:
                        raise HTTPException(422,
                            f'Invalid filter "{field_filter}", expected format "op:value", e.g. "eq:42"'
                        )
                    op_name, value = field_filter.split(":", maxsplit=1)
                    flat_filters.append((field_name, op_name, value))

        try:
            filters = Filters(
                fields = fields,
                field_types = field_types,
                filters = flat_filters,
            )
        except ValueError as e:
            raise HTTPException(422, str(e)) from e

        return filters
    dependency.__signature__ = sig

    return dependency


SortDirection = Literal['asc', 'desc']

class Sort:
    """ Use sort_params to create instances of this class """
    sorts: list[tuple[ApiField, SortDirection, ApiFilterOps]]

    def __init__(self,
        fields: list[ApiField],
        field_types: list[ApiFilterOps],
        default_sort: list[tuple[str, SortDirection]],
        sort: list[tuple[str, SortDirection]],
    ):
        self.sorts = Sort._parse_sorts(fields, field_types, default_sort, sort)


    @classmethod
    def _parse_sorts(cls,
        fields: list[ApiField],
        field_types: list[ApiFilterOps],
        default_sort: list[tuple[str, SortDirection]],
        sort: list[tuple[str, SortDirection]],
    ) -> list[tuple[ApiField, SortDirection, ApiFilterOps]]:
        fields_dict = {f.name: f for f in fields}
        field_types_dict = {ft.name: ft for ft in field_types}

        sort = [*sort] # copy
        for field_name, op in default_sort:
            # Add default sorts to end unless they are specified explicitly
            if not any(f == field_name for f, _ in sort):
                sort.append((field_name, op))

        result: list = []
        sorted_fields: set[str] = set()
        for field_name, op in sort:
            if field_name not in fields_dict:
                raise ValueError(f"Unknown sort field {field_name}")
            if field_name in sorted_fields:
                raise HTTPException(422, f"Sorted field {field_name} multiple times")

            field = fields_dict[field_name]
            result.append((field, op, field_types_dict[field.filter_ops]))
            sorted_fields.add(field_name)

        return result


    @property
    def sort_order(self) -> list[tuple[str, str]]:
        """ Returns the sort list passed as (field_name, asc/desc) tuples """
        return [(field.name, op) for field, op, _ in self.sorts]


    def apply(self, cols: Union[dict[str, ColumnElement], FromClause]) -> list[ColumnOperators]:
        """
        Returns a list of sqlalchemy column operators suitable for use in a order_by clause.
        Pass a mapping of `ApiField.name` to SQLAlchemy column objects. Any fields omitted from
        the cols dict will be skipped. If all fields map directly to table names, you can pass the
        table instead.
        """
        if isinstance(cols, FromClause):
            cols = {field.name: cols.c[field.name] for field, _, _ in self.sorts}

        result: list[ColumnOperators] = []
        for field, op, filter_ops in self.sorts:
            if field.name in cols:
                col = cols[field.name]
                if filter_ops.sort_conv:
                    col = filter_ops.sort_conv(col)
                result.append(col.desc() if op == 'desc' else col.asc())
        return result


    Datum = TypeVar("Datum")
    SortColsType = Union[None, dict[str, Callable[[Datum], Any]], Callable[[Datum, str], Any]]
    def sort_list(self, data: list[Datum], cols: SortColsType = None) -> list[Datum]:
        """
        Sorts a python list of data. Pass a dict mapping api fields to accessors. Any fields omitted
        from the cols dict will be skipped. If cols dict is left None, it will default to just using
        the index operator with the field name. You can also pass cols as a single function that
        takes both the Datum and the field name.
        """
        selected = {field.name for field, _, _, in self.sorts}
        if cols is None:
            col_func = lambda d, field_name: d[field_name]
        elif isinstance(cols, dict):
            col_func = lambda d, field_name: cols[field_name](d)
            selected = set(cols.keys())
        else:
            col_func = cols

        # Can't use a key func and do reverse
        def cmp(a, b):
            for field, op, filter_ops in self.sorts:
                if field.name in selected:
                    a_val, b_val = col_func(a, field.name), col_func(b, field.name)
                    direction = 1 if op == "asc" else -1

                    if a_val < b_val:
                        return -direction
                    elif a_val > b_val:
                        return direction
            return 0 # Equal

        return sorted(data, key = functools.cmp_to_key(cmp))


def sort_params(
    fields: list[ApiField], default_sort: Optional[list[str]] = None,
    extra_types: Optional[list[ApiFilterOps]] = None,
) -> Callable[..., Sort]:
    """
    Dependency for generic sorting logic for sqlalchemy queries.
    It will add a "sort" get parameter that takes a list of field names in the format
    `asc:my_field` or `desc:my_field`.
    """
    extra_types = extra_types or []
    default_sort = default_sort or []
    field_types = [*DEFAULT_FIELD_TYPES, *extra_types]
    description = (
        "Sort order of the return results. Can be specified multiple times to sort by multiple " +
        " values. Format as either `asc:field` or `desc:field`. Available fields: " +
        ', '.join(f.name for f in fields)
    )

    def split_sort(sort_field: str) -> tuple[str, SortDirection]:
        if ':' not in sort_field:
            raise HTTPException(422,
                f'Invalid sort "{sort_field}", expected format "dir:field", e.g. "asc:name"'
            )
        op, field_name = sort_field.split(':', 1)
        if op not in ['asc', 'desc']:
            raise HTTPException(422, f"Invalid sort direction {op}, should be asc or desc")
        return field_name, op


    def dependency(sort: A[Optional[list[str]], Query(description=description)] = None):
        sort = sort or []
        try:
            sort_obj = Sort(
                fields = fields,
                default_sort = [split_sort(s) for s in default_sort],
                sort = [split_sort(s) for s in sort],
                field_types = field_types,
            )
        except ValueError as e:
            raise HTTPException(422, str(e)) from e
        return sort_obj

    return dependency


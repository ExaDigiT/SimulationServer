import pytest
from datetime import timedelta as td, datetime, timezone as tz
import re

from sqlalchemy import select, literal, cast, TIMESTAMP
from fastapi import HTTPException

from src.util.api_queries import (
    QuerySpan, query_span_params, Granularity, granularity_params,
    Filters, filter_params, Sort, sort_params, expand_field_selectors,
)
from .helpers import normalize_sql


def test_granularity():
    gran_dep = granularity_params(default_granularity=td(seconds=30))
    gran = gran_dep()
    assert gran.get(datetime(2021, 1, 1), datetime(2021, 1, 2)) == td(seconds=30)

    gran_dep = granularity_params(default_resolution=10)
    gran = gran_dep()
    assert gran.get(datetime(2021, 1, 1), datetime(2021, 1, 2)) == td(hours=2)

    gran_dep = granularity_params()
    gran = gran_dep(granularity=td(minutes=3))
    assert gran.get(datetime(2021, 1, 1), datetime(2021, 1, 2)) == td(minutes=3)

    gran = gran_dep(resolution=100)
    assert gran.get(datetime(2021, 1, 1), datetime(2021, 5, 2)) == td(days=1)
    assert gran.get(datetime(2021, 1, 1), datetime(2021, 1, 2)) == td(minutes=10)

    # Should have min of 1 second
    assert gran.get(datetime(2021, 1, 1), datetime(2021, 1, 1)) == td(seconds=1)


    # resolution of 1 should return exactly the same as the start/end period without snapping
    gran = gran_dep(resolution=1)
    assert gran.get(datetime(2021, 1, 1), datetime(2021, 1, 2)) == td(days=1)
    assert gran.get(datetime(2021, 1, 1), datetime(2021, 1, 1, 1, 20)) == td(hours=1, minutes=20)


    with pytest.raises(ValueError):
        gran_dep(granularity=td(seconds=1), resolution=100)

    gran_dep = granularity_params()
    gran = gran_dep(granularity=td(minutes=3))
    assert gran.get(datetime(2021, 1, 1), datetime(2021, 1, 2)) == td(minutes=3)
    with pytest.raises(ValueError):
        gran_dep() # No defaults so its required


def test_granularity_validation():
    with pytest.raises(ValueError):
        Granularity.model_validate(dict(granularity = None, resolution = None))
    with pytest.raises(ValueError):
        Granularity.model_validate(dict(granularity = "PT1S", resolution = 1))
    with pytest.raises(ValueError):
        Granularity.model_validate(dict(resolution = 0))

    Granularity.model_validate(dict(granularity = 5))
    
    with pytest.raises(ValueError):
        Granularity.model_validate(dict(granularity = -5))


def test_query_span():
    qsp_dep = query_span_params(default_granularity=td(seconds=30))
    qsp = qsp_dep(datetime(2021, 1, 1, tzinfo=tz.utc), datetime(2021, 1, 2, tzinfo=tz.utc),
                    granularity=Granularity(resolution=10))
    assert qsp == QuerySpan(
        start = datetime(2021, 1, 1, tzinfo=tz.utc), end = datetime(2021, 1, 2, tzinfo=tz.utc),
        granularity = td(hours=2)
    )


def test_filter_params_parse():
    filters_dep = filter_params({
        'a': 'string',
        'b': 'number',
        'c': 'date',
    })

    filters = filters_dep()
    assert filters.filters == []

    filters = filters_dep(a = ['eq:apple'], b = ['lt:2'], c = ['gt:2022-03-05T06:07'])
    assert [p.op.name for p in filters.filters] == ['eq', 'lt', 'gt']
    assert [p.value for p in filters.filters] == ['apple', 2, datetime.fromisoformat("2022-03-05T06:07")]

    filters = filters_dep(a = ['starts_with:go'], b = ['gt:3.5'])
    assert [p.value for p in filters.filters] == ['go', 3.5]
    assert [p.value for p in filters.get('a')] == ['go']
    assert [p.value for p in filters.get('b')] == [3.5]


    filters = filters_dep(a = ['starts_with:go'], b = None)
    assert [p.value for p in filters.filters] == ['go']

    filters = filters_dep(b = ['one_of:2,3'])
    assert [p.value for p in filters.filters] == [[2, 3]]

    filters = filters_dep(c = ['one_of:2022-01-01T00:00,2022-01-02 01:00'])
    assert [p.value for p in filters.filters] == [[
        datetime.fromisoformat("2022-01-01 00:00"),
        datetime.fromisoformat("2022-01-02 01:00"),
    ]]


def test_filter_params():
    filters_dep = filter_params({
            'a': 'string',
            'b': 'number',
            'c': 'date',
        },
    )

    tbl = select(
        literal('avocado').label('a'),
        literal(2).label('b'),
        literal('2022-03-05T00:00:00').label('c')
    ).subquery().alias('tbl')

    cols = {
        'a': tbl.c.a,
        'b': tbl.c.b + 1,
        'c': tbl.c.c,
    }

    empty = filters_dep()
    assert empty.get('a') == []
    assert empty.filter_sql(tbl) == []
    assert empty.filter_sql(cols) == []

    filters = filters_dep(a = ['eq:apple'], b = ['lt:2'], c = ['gt:2022-03-05 06:07'])
    filtered_tbl = select(tbl).where(*filters.filter_sql(tbl))
    expected = normalize_sql(r"""
        SELECT tbl.a, tbl.b, tbl.c
        FROM .* AS tbl
        WHERE tbl.a = :\w+ AND tbl.b < :\w+ AND CAST\(.*tbl.c.* AS TIMESTAMP\) > CAST\(.*:\w+.* AS TIMESTAMP\)
    """)
    assert re.fullmatch(expected, normalize_sql(filtered_tbl), re.I)

    params = filtered_tbl.compile().params
    assert set(params.values()).issuperset({'apple', 2, '2022-03-05T06:07:00'})

    filtered_tbl = select(tbl).where(*filters.filter_sql(cols))
    expected = normalize_sql(r"""
        SELECT tbl.a, tbl.b, tbl.c
        FROM .* AS tbl
        WHERE tbl.a = :\w+ AND tbl.b \+ :\w+ < :\w+ AND
            CAST\(.*tbl.c.* AS TIMESTAMP\) > CAST\(.*:\w+.* AS TIMESTAMP\)
    """)
    assert re.fullmatch(expected, normalize_sql(filtered_tbl), re.I)

    filters = filters_dep(a = ['starts_with:go', 'ends_with:ing'])
    filtered_tbl = select(tbl).where(*filters.filter_sql(tbl))
    expected = normalize_sql(r"""
        SELECT tbl.a, tbl.b, tbl.c 
        FROM .* AS tbl
        WHERE \(tbl.a LIKE :\w+ \|\| '%'\) AND \(tbl.a LIKE '%' \|\| :\w+\)
    """)
    assert re.fullmatch(expected, normalize_sql(filtered_tbl), re.I)
    params = filtered_tbl.compile().params
    assert set(params.values()).issuperset({'go', 'ing'})


def test_filter_params_partial():
    filters_dep = filter_params({
        'a': 'string',
        'b': 'number',
        'c': 'date',
    })

    tbl = select(
        literal('avocado').label('a'),
        literal(2).label('b'),
        literal('2022-03-05T00:00:00').label('c')
    ).subquery().alias('tbl')

    cols = {
        'a': tbl.c.a,
    }

    filters = filters_dep(a = ['eq:apple'], b = ['lt:2'])
    filtered_tbl = select(tbl).where(*filters.filter_sql(cols))
    expected = normalize_sql(r"""
        SELECT tbl.a, tbl.b, tbl.c
        FROM .* AS tbl
        WHERE tbl.a = :\w+
    """)
    assert re.fullmatch(expected, normalize_sql(filtered_tbl), re.I)


def test_filter_params_timestamp():
    filters_dep = filter_params({
        'c': 'date',
    })

    tbl = select(
        cast(literal('2022-03-05T00:00:00'), TIMESTAMP).label('c')
    ).subquery().alias('tbl')

    filters = filters_dep(c = ['eq:2022-03-05 06:07'])
    stmt = select(tbl).where(*filters.filter_sql(tbl))
    # Already TIMESTAMP, won't re-cast
    expected = normalize_sql(r"""
        SELECT tbl.c
        FROM .* AS tbl
        WHERE tbl.c = CAST\(.*:\w+.* AS TIMESTAMP\)
    """)
    assert re.fullmatch(expected, normalize_sql(stmt), re.I)

    tbl = select(
        literal('2022-03-05T00:00:00').label('c')
    ).subquery().alias('tbl')

    filters = filters_dep(c = ['eq:2022-03-05 06:07'])
    stmt = select(tbl).where(*filters.filter_sql(tbl))
    expected = normalize_sql(r"""
        SELECT tbl.c
        FROM .* AS tbl
        WHERE CAST\(.*tbl.c.* AS TIMESTAMP\) = CAST\(.*:\w+.* AS TIMESTAMP\)
    """)
    assert re.fullmatch(expected, normalize_sql(stmt), re.I)


def test_filter_params_errors():
    filters_dep = filter_params({
       'c': 'number',
    })

    with pytest.raises(HTTPException):
        filters = filters_dep(c = ['eq:not_a_number'])
    
    with pytest.raises(HTTPException):
        filters = filters_dep(c = ['one_of:not_a_number_list'])


def test_filters_empty():
    filters = Filters()
    assert filters.filter_list(['a'], {}) == ['a']
    assert filters.filter_sql({}) == []


@pytest.mark.unit
@pytest.mark.parametrize("params,data,cols,expected", [
    (
        {'a': ['eq:apple'], 'b': ['lt:2']},
        [{'a': 'apple', 'b': 1}, {'a': 'apples', 'b': 1}, {'a': 'apple', 'b': 4}],
        None,
        [{'a': 'apple', 'b': 1}],
    ),
    (
        {},
        [{'a': 'apple', 'b': 1}, {'a': 'apples', 'b': 1}, {'a': 'apple', 'b': 4}],
        None,
        [{'a': 'apple', 'b': 1}, {'a': 'apples', 'b': 1}, {'a': 'apple', 'b': 4}],
    ),
    ({'a': ['eq:apple']}, [], None, []),
    ({}, [], None, []),
    (
        {'b': ['one_of:1,2']},
        [{'b': 1}, {'b': 2}, {'b': 4}],
        None,
        [{'b': 1}, {'b': 2}],
    ),
    (
        {'b': ['not_one_of:1,2']},
        [{'b': 1}, {'b': 2}, {'b': 4}],
        None,
        [{'b': 4}],
    ),
    (
        {'a': ['min_len:3']},
        [{'a': 'a'}, {'a': 'aa'}, {'a': 'aaa'}, {'a': 'aaaa'}],
        None,
        [{'a': 'aaa'}, {'a': 'aaaa'}],
    ),
    (
        {'a': ['max_len:3']},
        [{'a': 'a'}, {'a': 'aa'}, {'a': 'aaa'}, {'a': 'aaaa'}],
        None,
        [{'a': 'a'}, {'a': 'aa'}, {'a': 'aaa'}],
    ),
    (
        {'d': ['lte:2022-01-02 00:00']},
        [
            {'d': datetime.fromisoformat("2022-01-01 00:00")},
            {'d': datetime.fromisoformat("2022-01-02 00:00")},
            {'d': datetime.fromisoformat("2022-01-04 00:00")},
        ],
        None,
        [
            {'d': datetime.fromisoformat("2022-01-01 00:00")},
            {'d': datetime.fromisoformat("2022-01-02 00:00")},
        ],
    ),
    (
        {'d': ['lte:2022-01-02 00:00'], "b": ["eq:0"]},
        [
            {'d': "2022-01-01 00:00", "b": 1},
            {'d': "2022-01-02 00:00", "b": 1},
            {'d': "2022-01-04 00:00", "b": 1},
        ],
        {"d": lambda d: datetime.fromisoformat(d['d'])}, # Skips omitted cols
        [
            {'d': "2022-01-01 00:00", "b": 1},
            {'d': "2022-01-02 00:00", "b": 1},
        ],
    ),
    (
        {'d': ['lte:2022-01-02 00:00']},
        [{'d': "2022-01-01 00:00"}, {'d': "2022-01-02 00:00"}, {'d': "2022-01-04 00:00"}],
        lambda d, field: datetime.fromisoformat(d[field]),
        [{'d': "2022-01-01 00:00"}, {'d': "2022-01-02 00:00"}],
    ),
    (
        {'e': ['eq:P1D']},
        [{'e': 86400}, {'e': 2}, {'e': 3}],
        None,
        [{'e': 86400}],
    ),
    (
        {'e': ['one_of:2,3']},
        [{'e': 86400}, {'e': 2}, {'e': 3}],
        None,
        [{'e': 2}, {'e': 3}],
    ),
    (
        {'ln': ['contains:2']},
        [{'ln': [1, 2, 3]}, {'ln': [4, 5, 6]}],
        None,
        [{'ln': [1, 2, 3]}],
    ),
    (
        {'ld': ['contains_one_of:2022-01-02 00:00,2022-01-03 00:00']},
        [{'ld': ["2022-01-01 00:00", "2022-01-02 00:00"]}, {'ld': ["2022-01-03 00:00", "2022-01-04 00:00"]}],
        lambda d, field: [datetime.fromisoformat(x) for x in d[field]],
        [{'ld': ["2022-01-01 00:00", "2022-01-02 00:00"]}, {'ld': ["2022-01-03 00:00", "2022-01-04 00:00"]}],
    ),
])
def test_filter_params_filter_list(params, data, cols, expected):
    filters_dep = filter_params({
        'a': 'string',
        'b': 'number',
        'c': 'number',
        'd': 'date',
        'e': 'timedelta',
        'ln': 'array[number]',
        'ld': 'array[date]',
    })
    filters = filters_dep(**params)
    assert filters.filter_list(data, cols) == expected


def test_sort_params():
    sort_dep = sort_params({
        'a': 'number',
        'b': 'string',
        'c': 'date',
    })

    tbl = select(
        literal('avocado').label('a'),
        literal(2).label('b'),
        literal('2022-03-05T00:00:00').label('c')
    ).subquery().alias('tbl')

    sort = sort_dep(['asc:a', "desc:b", "asc:c"])
    stmt = select(tbl).order_by(*sort.sort_sql(tbl))

    # Already TIMESTAMP, won't re-cast
    expected = normalize_sql(r"""
        SELECT tbl.a, tbl.b, tbl.c
        FROM .* AS tbl
        ORDER BY tbl.a ASC,  tbl.b DESC, CAST\(.*tbl.c.* AS TIMESTAMP\) ASC
    """)
    assert re.fullmatch(expected, normalize_sql(stmt), re.I)


def test_sort_params_errors():
    sort_dep = sort_params({
        'a': 'number',
        'b': 'string',
        'c': 'date',
    })

    with pytest.raises(HTTPException):
        sort = sort_dep(['asc:x'])

    with pytest.raises(HTTPException):
        sort = sort_dep(['x:a'])

    with pytest.raises(HTTPException):
        sort = sort_dep(['a'])

    with pytest.raises(HTTPException):
        sort = sort_dep(['asc:a', 'asc:a'])


def test_sort_default_sort():
    fields = {
        'a': 'number',
        'b': 'string',
        'c': 'date',
    }
    sort_dep = sort_params(fields, ["desc:a"])

    assert sort_dep([]).sort_order == [('a', 'desc')]
    assert sort_dep(['asc:b']).sort_order == [('b', 'asc'), ('a', 'desc')]
    assert sort_dep(['asc:a', 'asc:b']).sort_order == [('a', 'asc'), ('b', 'asc')]


@pytest.mark.unit
@pytest.mark.parametrize("params,data,cols,expected", [
    (
        ['asc:a'],
        [{'a': 'banana'}, {'a': 'apple'}],
        None,
        [{'a': 'apple'}, {'a': 'banana'}],
    ),
    (
        ['desc:a'],
        [{'a': 'apple'}, {'a': 'banana'}],
        None,
        [{'a': 'banana'}, {'a': 'apple'}],
    ),
    (
        [],
        [{'a': 'apple'}, {'a': 'banana'}],
        None,
        [{'a': 'apple'}, {'a': 'banana'}],
    ),
    ([], [], None, []),
    (['asc:a'], [], None, []),
    (
        ['asc:a', 'desc:b'],
        [
            {'a': 'banana', 'b': 1},
            {'a': 'banana', 'b': 2},
            {'a': 'banana', 'b': 3},
            {'a': 'apple', 'b': -1},
        ],
        None,
        [
            {'a': 'apple', 'b': -1},
            {'a': 'banana', 'b': 3},
            {'a': 'banana', 'b': 2},
            {'a': 'banana', 'b': 1},
        ],
    ),
    (
        ['asc:d'],
        [
            {'d': datetime.fromisoformat("2022-01-04 00:00")},
            {'d': datetime.fromisoformat("2022-01-01 00:00")},
            {'d': datetime.fromisoformat("2022-01-02 00:00")},
        ],
        None,
        [
            {'d': datetime.fromisoformat("2022-01-01 00:00")},
            {'d': datetime.fromisoformat("2022-01-02 00:00")},
            {'d': datetime.fromisoformat("2022-01-04 00:00")},
        ],
    ),
        (
        ['asc:d', 'asc:b'],
        [
            {'d':"2022-01-04 00:00", 'b': 1},
            {'d':"2022-01-01T00:00", 'b': 2},
            {'d':"2022-01-01 00:01", 'b': 3},
            {'d':"2022-01-02 00:00", 'b': 4},
        ],
        {"d": lambda d: datetime.fromisoformat(d['d'])},
        [
            {'d':"2022-01-01T00:00", 'b': 2},
            {'d':"2022-01-01 00:01", 'b': 3},
            {'d':"2022-01-02 00:00", 'b': 4},
            {'d':"2022-01-04 00:00", 'b': 1},
        ],
    ),
    (
        ['asc:d'],
        [
            {'d':"2022-01-04 00:00"}, {'d':"2022-01-01T00:00"},
            {'d':"2022-01-01 00:01"}, {'d':"2022-01-02 00:00"},
        ],
        lambda d, field: datetime.fromisoformat(d[field]),
        [
            {'d':"2022-01-01T00:00"}, {'d':"2022-01-01 00:01"},
            {'d':"2022-01-02 00:00"}, {'d':"2022-01-04 00:00"},
        ],
    ),
])
def test_sort_params_sort_list(params, data, cols, expected):
    sort_dep = sort_params({
        'a': 'string',
        'b': 'number',
        'c': 'number',
        'd': 'date',
    })

    sort = sort_dep(params)
    assert sort.sort_list(data, cols) == expected


def test_sort_empty():
    sort_dep = Sort()
    data = [3, 2, 1]
    assert sort_dep.sort_list(data) == data
    assert sort_dep.sort_sql({}) == []


def test_expand_field_selectors():
    assert expand_field_selectors(['a', 'b', 'c'], {}) == ['a', 'b', 'c']
    assert expand_field_selectors(['a', 'b', 'b'], {}) == ['a', 'b']

    shortcuts = {
        "all": ['a', 'b', 'c'],
    }

    assert expand_field_selectors(['a'], shortcuts) == ['a']
    assert expand_field_selectors(['a', 'all'], shortcuts) == ['a', 'b', 'c']
    assert expand_field_selectors(['b', 'all'], shortcuts) == ['b', 'a', 'c']

    shortcuts = {
        "food": ['spaghetti', 'fruit'],
        "fruit": ['apple', 'banana']
    }

    assert expand_field_selectors(['food', 'apple'], shortcuts) == ['spaghetti', 'apple', 'banana']

    assert expand_field_selectors([], shortcuts) == []

    shortcuts = {
        "default": ['a', 'b'],
        "all": ['a', 'b', 'c'],
    }
    assert expand_field_selectors([], shortcuts) == ['a', 'b']
    assert expand_field_selectors(None, shortcuts) == ['a', 'b']

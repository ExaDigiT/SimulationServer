import pytest
from datetime import timedelta as td

from pydantic import TypeAdapter, ValidationError

from simulation_server.models.base import CommaSeparatedList, NumTimedelta


def test_comma_separated_list():
    Adapter = TypeAdapter(CommaSeparatedList[str])

    assert Adapter.validate_python([]) == []
    assert Adapter.validate_python(["a"]) == ["a"]
    assert Adapter.validate_python(["a", "b"]) == ["a", "b"]
    assert Adapter.validate_python(["a,b", "c,d"]) == ["a", "b", "c", "d"]
    assert Adapter.validate_python([" a , , b "]) == ["a", "b"]
    assert Adapter.validate_python(" a , , b ") == ["a", "b"]
    assert Adapter.validate_python(None) == None


    Adapter = TypeAdapter(CommaSeparatedList[int])
    assert Adapter.validate_python(["1", 2, " 3, 4 "]) == [1, 2, 3, 4]

    with pytest.raises(ValidationError):
        Adapter.validate_python([" a "])


def test_num_timedelta():
    adapter = TypeAdapter(NumTimedelta)

    assert adapter.validate_python(10) == td(seconds=10)
    assert adapter.validate_strings("10") == td(seconds=10)
    assert adapter.validate_strings("P1D") == td(days=1)

    assert adapter.serializer.to_json(td(minutes=2)).decode() == "120.0"
    assert adapter.serializer.to_json(td(seconds=1.5)).decode() == "1.5"

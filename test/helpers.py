from typing import Callable, Any
import contextlib, re
import fastapi
import fastapi.params


@contextlib.contextmanager
def override_deps(api: fastapi.FastAPI, overrides: dict[Callable[..., Any], Callable[..., Any]]):
    """ Context manager to temporarily override FastAPI dependencies """
    original_overrides = api.dependency_overrides
    try:
        api.dependency_overrides = {
            **api.dependency_overrides,
            **overrides,
        }
        yield
    finally:
        api.dependency_overrides = original_overrides


def normalize_sql(sql: Any):
    """
    Normalizes a generated SQL query to make it easier to compare
    You can pass a regex as well to normalize it.
    """
    return re.sub(r"\s+", "", str(sql))


def assert_sql_match(actual: Any, expected: str):
    """ Assert sql structure comparison against a regex """
    norm_actual = normalize_sql(str(actual))
    norm_expected = normalize_sql(str(expected))
    message = "".join([
        "Expected:\n\n",
        str(actual).strip(),
        "\n\nto match\n\n",
        str(expected).strip(),
        "\n",
    ])
    assert re.fullmatch(norm_expected, norm_actual, re.I), message

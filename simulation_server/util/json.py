from typing import Callable, Union, Any
import copy
import jsonpath_ng.ext as jsonpath
from jsonpath_ng import DatumInContext

Replacer = Union[Callable[[DatumInContext], Any], Any]
REMOVE = object()
""" Special object to signify that update_jsonpaths"""


def update_jsonpaths(data, queries: dict[str, Replacer]):
    """
    Update a json object based on `jsonpath_ng.ext` queries.
    Pass a dict of jsonpath queries to "replacers" that will be run on all matches.
    A replacer can be:
    - a function that takes a DatumInContext match and return a new value
    - a constant value to replace all matches with
    - the special value `jsonpath_utils.REMOVE` which means the matches will be deleted from data (or 
      a function that returns `jsonpath_utils.REMOVE`)

    Returns a new object (does not mutate input).
    Queries are executed in order, and can match on the replacements made by previous queries.

    E.g.
    ```
    o = {'foo': 1, 'bar': 2}
    update_jsonpaths(o, {"$..foo": lambda m: m.value + 100})
    # {'foo': 101, 'bar': 2}
    ```
    """
    data = copy.deepcopy(data)

    for query, replacer in queries.items():
        matches: list[DatumInContext] = list(jsonpath.parse(query).find(data))
        for match in matches:
            replacement = replacer(match) if callable(replacer) else replacer

            if replacement is REMOVE:
                data = match.full_path.filter(lambda x: True, data)
            else:
                data = match.full_path.update(data, replacement)

    return data

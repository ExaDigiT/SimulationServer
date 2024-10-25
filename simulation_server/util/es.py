"""
Connection to Cadence ES
"""
import os
import urllib.parse
from datetime import datetime
import sqlalchemy as sqla
from sqlalchemy.engine import Engine, create_engine
from elasticsearch import Elasticsearch
from es.elastic.sqlalchemy import ESDialect


def get_nccs_cadence_engine(**kwargs) -> Engine:
    import sqlalchemy.types as types
    from sqlalchemy.ext.compiler import compiles

    # For some reason sqla/pydruid renders `cast(col, sqla.TIMESTAMP)` to `CAST(col AS LONG)`. This
    # is a manual override to make sqla render them properly.
    cast_fixes = {
        types.TIMESTAMP: "TIMESTAMP",
    }

    for (sqla_type, override) in cast_fixes.items():
        compiles(sqla_type, "elasticsearch")(lambda type_, compiler, override=override, **kw: override)

    # We need to set retry_on_status to work around intermittent 401 errors from Cadence ES.
    # The query params will get passed to the Elasticsearch client, but only some specific
    # ones get parsed and the rest are left as strings. This monkey patch hacks elasticsearch-dbapi
    # to parse retry_on_status. We can remove this if the AM team fixes the auth errors
    import es.basesqlalchemy
    es.basesqlalchemy.BaseESDialect._map_parse_connection_parameters['retry_on_status'] = json.loads

    URL = urllib.parse.urlparse(os.environ["NCCS_CADENCE_URL"])
    HOST, PORT = URL.netloc.split(":")
    USER = os.environ["NCCS_CADENCE_USER"]
    PASSWORD = os.environ["NCCS_CADENCE_PASSWORD"]
    # These get passed through to the internal Elasticsearch instance
    QUERY_PARAMS = 'use_ssl=false&ssl_show_warn=false&verify_certs=false&retry_on_status=[502,503,504,401]'

    engine = create_engine(f'elasticsearch+{URL.scheme}://{USER}:{PASSWORD}@{HOST}:{PORT}{URL.path}?{QUERY_PARAMS}', **kwargs)
    return engine


def to_timestamp(val: datetime):
    return sqla.func.convert(val.isoformat(), sqla.literal_column('TIMESTAMP'))

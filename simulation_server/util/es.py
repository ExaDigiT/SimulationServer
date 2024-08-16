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

    URL = urllib.parse.urlparse(os.environ["NCCS_CADENCE_URL"])
    HOST, PORT = URL.netloc.split(":")
    USER = os.environ["NCCS_CADENCE_USER"]
    PASSWORD = os.environ["NCCS_CADENCE_PASSWORD"]
    # These get passed through to the internal Elasticsearch instance
    QUERY_PARAMS = 'use_ssl=false&ssl_show_warn=false&verify_certs=false'

    engine = create_engine(f'elasticsearch+{URL.scheme}://{USER}:{PASSWORD}@{HOST}:{PORT}{URL.path}?{QUERY_PARAMS}', **kwargs)
    return engine


def to_timestamp(val: datetime):
    return sqla.func.convert(val.isoformat(), sqla.literal_column('TIMESTAMP'))

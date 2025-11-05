"""
Connection to Cadence ES
"""
import os
from elasticsearch import Elasticsearch
import tenacity


def get_nccs_cadence_es():
    URL = os.environ["NCCS_CADENCE_URL"]
    USER = os.environ["NCCS_CADENCE_USER"]
    PASSWORD = os.environ["NCCS_CADENCE_PASSWORD"]
    return Elasticsearch(
        URL,
        http_auth=(USER, PASSWORD),
        # TODO: we need to fix the self-signed certs on ES
        use_ssl=False,
        ssl_show_warn=False,
        verify_certs=False,
    )


def es_sql_query(client: Elasticsearch, query: str, params: list = [], fetch_size = 100):
    """
    Runs an SQL query against ES. Use `?` format for SQL params.
    """
    # Cadence ES is a bit flaky with intermittent 401 errors
    @tenacity.retry(
        stop = tenacity.stop_after_attempt(5),
        wait = tenacity.wait_exponential(multiplier=0.5, min=1, max=30),
        reraise = True,
    )
    def _retry_query(query, params, cursor = None):
        body = {
            "query": query,
            "params": params,
            "fetch_size": fetch_size,
        }
        if cursor:
            body["cursor"] = cursor
        return client.sql.query(format = 'json', body = body)

    response = _retry_query(query, params)
    rows = response['rows']
    cursor = response.get("cursor")
    columns = [c['name'] for c in response['columns']]
    while cursor:
        response = _retry_query(query, params, cursor)
        rows.extend(response['rows'])
        cursor = response.get("cursor")

    rows = [dict(zip(columns, row)) for row in rows]
    return rows

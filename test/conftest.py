import pytest
from simulation_server.server.main import app


@pytest.fixture(scope="module")
def api_client():
    from fastapi.testclient import TestClient

    with TestClient(app) as client:
        yield client


# @pytest.fixture(scope="module")
# def druid_engine():
#     """ Get a reference to the druid engine"""
#     return get_obs_druid_engine()



from datetime import datetime, timedelta, timezone
import uuid, time
import sqlalchemy as sqla
from loguru import logger
from ..models.sim import Sim
from .config import AppDeps
from ..util.k8s import submit_job
from ..util.druid import get_table


def wait_until_exists(stmt: sqla.Select, *, timeout: timedelta = timedelta(minutes=1), druid_engine: sqla.Engine):
    # Hack to block until row shows up in Druid (after being read from Kafka)
    # When we upgrade druid we may be able to use a SQL INSERT instead
    
    # Perhaps we should move the sim table table out of Druid and into our Postgres instance so we
    # can directly update it, and just use Druid for the large timeseries tables
    record = False
    with druid_engine.connect() as conn:
        start = time.time()
        record = conn.execute(stmt).first()
        while (time.time() - start) < timeout.total_seconds() and not record:
            time.sleep(0.1)
            record = conn.execute(stmt).first()

    if not record:
        logger.error("Timeout while waiting for record to be saved to druid")
        raise TimeoutError("Timeout while waiting for record to be saved to druid")

    return record



def run_simulation(sim_config, deps: AppDeps):
    sim = Sim(
        id = str(uuid.uuid4()),
        user = "unknown", # TODO pull this from cookie/auth header
        state = "running",
        logical_start = sim_config.start,
        logical_end = sim_config.end,
        run_start = datetime.now(timezone.utc),
        run_end = None,
        progress = 0,
        config = sim_config.model_dump(mode = 'json'),
    )
    deps.kafka_producer.send("svc-event-exadigit-sim", value = sim.serialize_for_druid())

    submit_job({
        "metadata": {
            "name": f"exadigit-simulation-server-{sim.id}",
            "labels": {"app": "exadigit-simulation-server"},
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "main",
                            "image": deps.settings.job_image,
                            "command": ['python3', "-m", "simulation_server.simulation.main", "background-job"],
                            "env": [
                                {"name": "SIM", "value": sim.model_dump_json()},
                            ],
                            "envFrom": [
                                {"secretRef": {'name': 'prod-infra-envconfig-service-sens-creds'}},
                            ],
                            "resources": {
                                "requests": {"cpu": "1000m", "memory": "512Mi"},
                                "limits": {"cpu": "4000m", "memory": "3Gi"},
                            },
                        }
                    ],
                    "restartPolicy": "Never",
                }
            },
            "backoffLimit": 0, # Don't retry on failure
        }
    })

    sim_table = get_table("svc-event-exadigit-sim", deps.druid_engine)
    stmt = sqla.select(sim_table.c.id).where(sim_table.c.id == sim.id)
    wait_until_exists(stmt, timeout = timedelta(minutes=1), druid_engine = deps.druid_engine)

    return sim


from typing import Optional
from datetime import datetime, timedelta, timezone
import uuid, time, json
import sqlalchemy as sqla
from loguru import logger
from ..models.sim import Sim
from .config import AppDeps
from ..util.misc import pick, omit
from ..util.k8s import submit_job
from ..util.druid import get_table, to_timestamp, any_value, latest
from .api_queries import Filters, Sort, DatetimeValidator


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


def query_sims(*,
    filters: Optional[Filters] = None, sort: Optional[Sort] = None,
    limit = 100, offset = 0,
    druid_engine: sqla.Engine,
) -> list[Sim]:
    filters = filters or Filters()
    sort = sort or Sort()
    sims = get_table("svc-event-exadigit-sim", druid_engine)
    sim_output_tables = [
        get_table("sens-svc-event-exadigit-scheduler-sim-job", druid_engine),
        get_table("svc-event-exadigit-scheduler-sim-system", druid_engine),
        get_table("svc-event-exadigit-cooling-sim-cdu", druid_engine),
    ]

    cols = {
        "id": sims.c.id,
        "user": sims.c.user,
        "state": sims.c.state,
        "logical_start": to_timestamp(sims.c.logical_start),
        "logical_end": to_timestamp(sims.c.logical_end),
        "run_start": sims.c['__time'],
        "run_end": to_timestamp(sims.c.run_end),
        "config": sims.c.config,
    }

    grouped_cols = {
        "id": any_value(sims.c.id, 40),
        "user": any_value(sims.c.user, 40),
        "state": latest(sims.c.state, 12),
        "logical_start": to_timestamp(any_value(sims.c.logical_start, 32)),
        "logical_end": to_timestamp(any_value(sims.c.logical_end, 32)),
        "run_start": to_timestamp(any_value(sims.c.run_start, 32)),
        "run_end": to_timestamp(latest(sims.c.run_end, 32)),
        "config": any_value(sims.c.config, 4 * 1024),
    }

    stmt = sqla.select(*[col.label(name) for name, col in grouped_cols.items()])
    stmt = stmt.where(*filters.filter_sql(omit(cols, ['state'])))
    stmt = stmt.group_by(cols['id'])
    if filters.get('state'):
        stmt = stmt.having(*filters.filter_sql(pick(grouped_cols, ['state'])))
    stmt = stmt.order_by(*sort.sort_sql(grouped_cols))
    stmt = stmt.limit(limit).offset(offset)

    with druid_engine.connect() as conn:
        results = [
            Sim.model_validate({**r._asdict(), "config": json.loads(r.config)})
            for r in conn.execute(stmt)
        ]

        incomplete = [sim.id for sim in results if not sim.run_end]
        progresses = {}

        if len(incomplete) > 0:
            for tbl in sim_output_tables:
                stmt = (
                    sqla.select(tbl.c.sim_id, sqla.func.max(to_timestamp(tbl.c['__time'])).label('progress'))
                        .where(tbl.c.sim_id.in_(incomplete))
                        .group_by(tbl.c.sim_id)
                )
                for r in conn.execute(stmt).all():
                    progress = DatetimeValidator.validate_strings(r.progress)
                    progresses[r.sim_id] = max(progress, progresses.get(r.sim_id, progress))

        for sim in results:
            if sim.id in progresses:
                progress = (progresses[sim.id] - sim.logical_start) / (sim.logical_end - sim.logical_start)
                # Never return 1 if incomplete
                sim.progress = round(min(max(0, progress), 0.99), 3)
            elif not sim.run_end:
                sim.progress = 0
            else:
                sim.progress = 1
    
    return results

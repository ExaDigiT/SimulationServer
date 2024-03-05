from typing import Optional
from datetime import datetime, timedelta, timezone
import uuid, time, json
import sqlalchemy as sqla
from loguru import logger
from ..models.sim import Sim, SIM_FIELD_SELECTORS
from .config import AppDeps
from ..models.base import ResponseFormat
from ..models.output import (
    COOLING_CDU_API_FIELDS, COOLING_CDU_FIELD_SELECTORS,
    SCHEDULER_SIM_JOB_API_FIELDS, SCHEDULER_SIM_JOB_FIELD_SELECTORS,
)
from ..util.misc import pick, omit
from ..util.k8s import submit_job
from ..util.druid import get_table, to_timestamp, any_value, latest
from .api_queries import (
    Filters, Sort, QuerySpan, Granularity, expand_field_selectors, DatetimeValidator,
)


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
        start = sim_config.start,
        end = sim_config.end,
        execution_start = datetime.now(timezone.utc),
        execution_end = None,
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
    fields: Optional[list[str]] = None,
    limit = 100, offset = 0,
    druid_engine: sqla.Engine,
) -> list[Sim]:
    filters = filters or Filters()
    sort = sort or Sort()
    fields = expand_field_selectors(fields, SIM_FIELD_SELECTORS)

    if 'progress' in fields:
        query_fields = [f for f in fields if f != 'progress']
        query_fields = [*dict.fromkeys(query_fields + [ # Need these to calculate progress
            'id', 'start', 'end', 'execution_start', 'execution_end',
        ])]
    else:
        query_fields = fields

    sims = get_table("svc-event-exadigit-sim", druid_engine)
    sim_output_tables = [
        get_table("sens-svc-event-exadigit-scheduler-sim-job", druid_engine),
        get_table("svc-ts-exadigit-scheduler-sim-system", druid_engine),
        get_table("svc-ts-exadigit-cooling-sim-cdu", druid_engine),
    ]

    cols = {
        "id": sims.c.id,
        "user": sims.c.user,
        "state": sims.c.state,
        "start": to_timestamp(sims.c.start),
        "end": to_timestamp(sims.c.end),
        "execution_start": sims.c.execution_start,
        "execution_end": to_timestamp(sims.c.execution_end),
        "config": sims.c.config,
    }

    grouped_cols = {
        "id": any_value(sims.c.id, 40),
        "user": any_value(sims.c.user, 40),
        "state": latest(sims.c.state, 12),
        "start": to_timestamp(any_value(sims.c.start, 32)),
        "end": to_timestamp(any_value(sims.c.end, 32)),
        "execution_start": to_timestamp(any_value(sims.c.execution_start, 32)),
        "execution_end": to_timestamp(latest(sims.c.execution_end, 32)),
        "config": any_value(sims.c.config, 4 * 1024),
    }

    stmt = sqla.select(*[grouped_cols[name].label(name) for name in query_fields])
    stmt = stmt.where(*filters.filter_sql(omit(cols, ['state'])))
    stmt = stmt.group_by(cols['id'])
    if filters.get('state'):
        stmt = stmt.having(*filters.filter_sql(pick(grouped_cols, ['state'])))
    stmt = stmt.order_by(*sort.sort_sql(grouped_cols))
    stmt = stmt.limit(limit).offset(offset)

    with druid_engine.connect() as conn:
        results = (r._asdict() for r in conn.execute(stmt))
        if 'config' in fields:
            results = ({**r, 'config': json.loads(r['config'])} for r in results)
        results = [Sim.model_validate(r) for r in results]

        if 'progress' in fields:
            incomplete = [sim.id for sim in results if not sim.execution_end]
            progresses = {}

            if len(incomplete) > 0:
                for tbl in sim_output_tables:
                    stmt = (
                        sqla.select(
                            tbl.c.sim_id,
                            sqla.func.max(to_timestamp(tbl.c['__time'])).label('progress')
                        )
                            .where(tbl.c.sim_id.in_(incomplete))
                            .group_by(tbl.c.sim_id)
                    )
                    for r in conn.execute(stmt).all():
                        progress = DatetimeValidator.validate_strings(r.progress)
                        progresses[r.sim_id] = max(progress, progresses.get(r.sim_id, progress))

            for sim in results:
                if sim.id in progresses:
                    progress = (progresses[sim.id] - sim.start) / (sim.end - sim.start)
                    # Never return 1 if incomplete
                    sim.progress = round(min(max(0, progress), 0.99), 3)
                elif not sim.execution_end:
                    sim.progress = 0
                else:
                    sim.progress = 1
        
        results = [Sim.model_validate(pick(r.model_dump(), fields)) for r in results]
    
    return results


def get_extent(tbl,
    id: str, start: Optional[datetime], end: Optional[datetime],
    druid_engine: sqla.engine.Engine,
) -> tuple[datetime, datetime]:
    if not start or not end:
        stmt = (
            sqla.select(
                sqla.func.min(tbl.c['__time']).label("start"),
                sqla.func.max(tbl.c['__time']).label("end")
            )
                .where(tbl.c['sim_id'] == id)
        )
        with druid_engine.connect() as conn:
            row = conn.execute(stmt).one()
            start = start or row.start
            end = end or row.end
    return (start, end)


def _split_list(l):
    return l.split(",") if l else []


def _build_ts_query(tbl, *,
    id: str, span: QuerySpan,
    fields: list[str], filters: Filters,
    group_cols: dict, agg_cols: dict, filter_cols: dict,
):
    all_cols = {**group_cols, **agg_cols}
    select_cols = [
        span.floor(tbl.c['__time']).label('timestamp'),
        *[all_cols[name].label(name) for name in fields],
    ]

    stmt = (
        sqla.select(*select_cols)
            .where(
                tbl.c['sim_id'] == id,
                *span.filter(tbl.c['__time']),
                *filters.filter_sql(filter_cols),
            )
            .group_by(sqla.text("1"), *group_cols.values())
            .order_by(sqla.text("1"), *group_cols.values())
    )
    fields = ['timestamp', *fields]
    return fields, stmt


def _run_ts_query(*, stmt,
    span: QuerySpan, fields: list[str], format: ResponseFormat,
    druid_engine: sqla.engine.Engine,
):
    response = span.model_dump(mode = 'json')
    with druid_engine.connect() as conn:
        if format == "array":
            response['fields'] = fields
            response['data'] = [list(r) for r in conn.execute(stmt)]
        else:
            response['data'] = [r._asdict() for r in conn.execute(stmt)]
    return response


def build_cooling_sim_cdu_query(*,
    id: str, span: QuerySpan,
    fields: Optional[list[str]] = None, filters: Optional[Filters] = None,
    druid_engine: sqla.engine.Engine,
):
    fields = expand_field_selectors(fields, COOLING_CDU_FIELD_SELECTORS)
    filters = filters or Filters()

    tbl = get_table('svc-ts-exadigit-cooling-sim-cdu', druid_engine).alias("cdus")
    filter_cols = {c: tbl.c[c] for c in COOLING_CDU_API_FIELDS}
    group_cols = {
        "xname": tbl.c['xname'],
    }
    agg_cols: dict[str, sqla.sql.ColumnElement] = {
        **{
            c: sqla.func.max(tbl.c[c]) for c in COOLING_CDU_API_FIELDS
            if c not in ['xname', 'row', 'col']
        },
        **{c: sqla.func.any_value(tbl.c[c]) for c in ['row', 'col']},
    }

    return _build_ts_query(tbl,
        id = id, span = span, fields = fields, filters = filters,
        group_cols = group_cols, agg_cols = agg_cols, filter_cols = filter_cols,
    )


def query_cooling_sim_cdu(*,
    id: str, 
    start: Optional[datetime] = None, end: Optional[datetime] = None, granularity: Granularity,
    fields: Optional[list[str]] = None, filters: Optional[Filters] = None,
    format: ResponseFormat = "object",
    druid_engine: sqla.engine.Engine,
):
    tbl = get_table('svc-ts-exadigit-cooling-sim-cdu', druid_engine)
    start, end = get_extent(tbl, id, start, end, druid_engine = druid_engine)
    span = QuerySpan(start = start, end = end, granularity = granularity.get(start, end))
    fields, stmt = build_cooling_sim_cdu_query(
        id = id, span = span, fields = fields, filters = filters,
        druid_engine = druid_engine,
    )
    return _run_ts_query(
        span = span, stmt = stmt, fields = fields,
        format = format, druid_engine = druid_engine,
    )


def build_scheduler_sim_jobs_query(*,
    id: str, start: Optional[datetime] = None, end: Optional[datetime] = None,
    time_travel: Optional[datetime] = None, limit = 100, offset = 0,
    fields: Optional[list[str]] = None, filters: Optional[Filters] = None,
    druid_engine: sqla.engine.Engine,
):
    fields = expand_field_selectors(fields, SCHEDULER_SIM_JOB_FIELD_SELECTORS)
    filters = filters or Filters()

    tbl = get_table('sens-svc-event-exadigit-scheduler-sim-job', druid_engine).alias("jobs")
    
    cols = {
        "job_id": tbl.c.job_id,
        "name": any_value(tbl.c.name, 256),
        "node_count": latest(tbl.c.node_count),
        "time_snapshot": sqla.func.max(tbl.c['__time']),
        "time_submission": to_timestamp(latest(tbl.c.time_submission)),
        "time_limit": latest(tbl.c.time_limit),
        "time_start": to_timestamp(latest(tbl.c.time_start)),
        "time_end": to_timestamp(latest(tbl.c.time_end)),
        "state_current": latest(tbl.c.state_current, 12),
        # TODO: These aggregations are going to have performance problems on larger datasets
        "node_ranges": latest(tbl.c.node_ranges, 20 * 1024),
        "xnames": latest(tbl.c.xnames, 128 * 1024),
    }


    # These fields won't change during the course of a job so we can filter using a WHERE
    where_filters = {
        **{f: tbl.c[f] for f in ['job_id', 'name', 'node_count', 'time_limit']},
        **{f: to_timestamp(tbl.c[f]) for f in ['time_submission', 'time_start']},
    }
    having_filters = {k: cols[k] for k in ["time_end", "state_current"]}
    

    stmt = sqla.select(*[cols[name].label(name) for name in fields])
    stmt = stmt.where(tbl.c['sim_id'] == id)
    if time_travel:
        stmt = stmt.where(tbl.c['__time'] <= to_timestamp(time_travel))

    # time_submission won't change over the course of a job_id, and time_end once set won't change
    # either. Technically Slurm can reschedule a job which updates time_submission, but we aren't
    # simulating that. And if we do, we'd just add an allocation_id like the telemetry data to
    # differentiate between requeueings.
    if start:
        stmt = stmt.where(
            to_timestamp(start) <= tbl.c['__time'], # Can ignore snapshots before start
            sqla.or_(tbl.c.time_end.is_(None), to_timestamp(start) < to_timestamp(tbl.c.time_end)),
        )
    if end:
        stmt = stmt.where(to_timestamp(tbl.c.time_submission) < to_timestamp(end))

    stmt = stmt.where(*filters.filter_sql(where_filters))
    stmt = stmt.group_by("job_id")
    stmt = stmt.having(*filters.filter_sql(having_filters))
    # TODO: Order by
    stmt = stmt.limit(limit).offset(offset)
    logger.info(f"STMT {stmt}")

    return fields, stmt


def query_scheduler_sim_jobs(*,
    id: str, start: Optional[datetime] = None, end: Optional[datetime] = None,
    time_travel: Optional[datetime] = None, limit = 100, offset = 0,
    fields: Optional[list[str]] = None, filters: Optional[Filters] = None,
    druid_engine: sqla.engine.Engine,
):
    fields, stmt = build_scheduler_sim_jobs_query(
        id = id, start = start, end = end,
        time_travel = time_travel, limit = limit, offset = offset,
        fields = fields, filters = filters,
        druid_engine = druid_engine,
    )

    with druid_engine.connect() as conn:
        results = (r._asdict() for r in conn.execute(stmt))
        if 'xnames' in fields:
            return [{
                **j,
                'xnames': _split_list(j['xnames']),
            } for j in results]
        else:
            return [j for j in results]


def build_scheduler_sim_system_query(*,
    id: str, start: Optional[datetime] = None, end: Optional[datetime] = None,
    druid_engine: sqla.engine.Engine,
):
    tbl = get_table('svc-ts-exadigit-scheduler-sim-system', druid_engine).alias("jobs")
    cols = {
        "down_nodes": tbl.c.down_nodes,
    }


    stmt = sqla.select(
        tbl.c['__time'].label('timestamp'),
        *[col.label(name) for name, col in cols.items()],
    )
    stmt = stmt.where(tbl.c['sim_id'] == id)
    if start:
        stmt.where(to_timestamp(start) <= tbl.c['__time'])
    if end:
        stmt.where(tbl.c['__time'] < to_timestamp(end))
    return ["timestamp", *cols.keys()], stmt


def query_scheduler_sim_system(*,
    id: str, 
    start: Optional[datetime] = None, end: Optional[datetime] = None,
    druid_engine: sqla.engine.Engine,
):
    fields, stmt = build_scheduler_sim_system_query(
        id = id, start = start, end = end,
        druid_engine = druid_engine,
    )

    with druid_engine.connect() as conn:
        results = (r._asdict() for r in conn.execute(stmt))
        if 'down_nodes' in fields:
            return [{
                **j,
                'down_nodes': _split_list(j['down_nodes']),
            } for j in results]
        else:
            return [j for j in results]

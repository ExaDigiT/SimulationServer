import pandas as pd
import numpy as np
import sqlalchemy as sqla
from loguru import logger
from datetime import datetime, timedelta
from .raps.raps.telemetry import Telemetry
from ..models.sim import SimConfig
from ..util.druid import get_druid_engine, get_table, to_timestamp
from ..util.es import get_nccs_cadence_engine
from . import SimException


def fetch_frontier_data(sim_config: SimConfig, raps_config: dict):
    """
    Fetch and parse real telemetry data
    """
    # TODO: Should consider using LVA API instead of directly querying the DB for this
    nccs_cadence_engine = get_nccs_cadence_engine()
    druid_engine = get_druid_engine()
    start, end = sim_config.start, sim_config.end

    job_query = sqla.text("""
        SELECT
            "allocation_id", "job_id", "slurm_version", "account", "group", "user", "name",
            "time_limit", "time_submission", "time_eligible", "time_start", "time_end", "time_elapsed",
            "node_count", xnames_str AS "xnames", "state_current", "state_reason",
            "time_snapshot"
        FROM "stf218.frontier.job-summary"
        WHERE
            (time_start IS NOT NULL AND time_start <= CONVERT(:end, TIMESTAMP)) AND
            (time_end IS NULL OR time_end > CONVERT(:start, TIMESTAMP))
    """).bindparams(
        start = start.isoformat(), end = end.isoformat(),
    )
    job_data = pd.read_sql_query(job_query, nccs_cadence_engine, parse_dates=[
        "time_snapshot", "time_submission", "time_eligible", "time_start", "time_end",
    ])
    # TODO: Even with sqlStringifyArrays: false, multivalue columns are returned as json strings.
    # And single rows are returned as raw strings. When we update Druid we can use ARRAYS and remove
    # this. Moving the jobs table to postgres would also fix this (and other issues).
    job_data['xnames'] = job_data['xnames'].map(lambda x: x.split(",") if x else [])

    job_profile_tbl = get_table("pub-ts-frontier-job-profile", druid_engine)
    job_profile_query = (
        sqla.select(
            job_profile_tbl.c['__time'].label("timestamp"),
            job_profile_tbl.c.allocation_id,
            job_profile_tbl.c.sum_cpu0_power,
            job_profile_tbl.c.sum_gpu_power,
        )
            .where(
                to_timestamp(start) <= job_profile_tbl.c['__time'],
                job_profile_tbl.c['__time'] < to_timestamp(end),
            )
    )
    job_profile_data = pd.read_sql(job_profile_query, druid_engine, parse_dates=[
        "timestamp",
    ])

    if (job_data.empty or job_profile_data.empty):
        raise SimException(f"No telemetry data for {start.isoformat()} -> {end.isoformat()}")

    telemetry = Telemetry(system = "frontier", config = raps_config)
    jobs = telemetry.load_data_from_df(job_data, job_profile_data,
        min_time = start,
        reschedule = sim_config.scheduler.reschedule,
        config = raps_config,
    )
    return jobs


def query_time_range(
    tbl_name: str, start: datetime, end: datetime, end_col: str, *,
    druid_engine, parse_dates: list[str],
):
    tbl = get_table(tbl_name, druid_engine)
    query = (
        sqla.select(sqla.text("*"))
            .where(
                (tbl.c['__time'] <= to_timestamp(end)) &
                (tbl.c['__time'] >= to_timestamp(start - timedelta(days=3))) &
                (tbl.c[end_col] >= to_timestamp(start))
            )
    )
    data = pd.read_sql(query, druid_engine, parse_dates=parse_dates)
    return data


def split_list(x):
    x = x.split(",") if x else []
    return np.array([int(x) for x in x])


def fetch_fugaku_data(sim_config: SimConfig, raps_config: dict):
    druid_engine = get_druid_engine()
    start, end = sim_config.start, sim_config.end

    data = query_time_range(
        "svc-ts-exadigit-data-fugaku", start, end, 'edt',
        druid_engine = druid_engine,
        parse_dates = ["adt", "qdt", "schedsdt", "deldt", "sdt", "edt"],
    )
    telemetry = Telemetry(system = "fugaku", config = raps_config)
    jobs = telemetry.load_data_from_df(data,
        min_time = start,
        reschedule = sim_config.scheduler.reschedule,
        config = raps_config,
    )
    return jobs


def fetch_marconi100_data(sim_config: SimConfig, raps_config: dict):
    druid_engine = get_druid_engine()
    start, end = sim_config.start, sim_config.end

    data = query_time_range(
        "svc-ts-exadigit-data-marconi100", start, end, 'end_time',
        druid_engine = druid_engine,
        parse_dates = ["submit_time", "start_time", "end_time", "eligible_time"],
    )

    data['nodes'] = data['nodes'].map(split_list)
    data['node_power_consumption'] = data['node_power_consumption'].map(split_list)
    data['mem_power_consumption'] = data['mem_power_consumption'].map(split_list)
    data['cpu_power_consumption'] = data['cpu_power_consumption'].map(split_list)

    telemetry = Telemetry(system = "marconi100", config = raps_config)
    jobs = telemetry.load_data_from_df(data,
        min_time = start,
        reschedule = sim_config.scheduler.reschedule,
        config = raps_config,
    )
    return jobs


def fetch_lassen_data(sim_config: SimConfig, raps_config: dict):
    druid_engine = get_druid_engine()
    start, end = sim_config.start, sim_config.end

    allocation_df = query_time_range(
        "svc-ts-exadigit-data-lassen-allocation-history", start, end, 'end_time',
        druid_engine = druid_engine,
        parse_dates = ["begin_time", "end_time", "job_submit_time"],
    )

    tbl = get_table("svc-ts-exadigit-data-lassen-node-history", druid_engine)
    node_query = (
        sqla.select(sqla.text("*"))
            .where(
                (tbl.c['__time'] <= to_timestamp(end)) &
                (tbl.c['__time'] >= to_timestamp(start - timedelta(days=3)))
            )
    )
    node_df = pd.read_sql(node_query, druid_engine)

    step_df = query_time_range(
        "svc-ts-exadigit-data-fugaku-lassen-step-history", start, end, 'end_time',
        druid_engine = druid_engine,
        parse_dates = ["begin_time", "end_time"],
    )

    telemetry = Telemetry(system = "lassen", config = raps_config)
    jobs = telemetry.load_data_from_df(
        allocation_df = allocation_df, node_df = node_df, step_df = step_df,
        min_time = start,
        reschedule = sim_config.scheduler.reschedule,
        config = raps_config,
    )
    return jobs


DATA_LOADERS = {
    "frontier": fetch_frontier_data,
    "fugaku": fetch_fugaku_data,
    "marconi100": fetch_marconi100_data,
    "lassen": fetch_lassen_data,
}
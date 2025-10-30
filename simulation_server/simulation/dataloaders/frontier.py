from ...util.druid import get_druid_engine, get_table, to_timestamp
from ...util.dataloader import query_time_range
from ...util.es import get_nccs_cadence_es, es_sql_query
from ...models.sim import ServerSimConfig
from .. import SimException
import sqlalchemy as sqla
import pandas as pd

# Re-use these from the raps dataloader
from raps.dataloaders.frontier import load_data_from_df, node_index_to_name, cdu_index_to_name, cdu_pos


def load_data(_paths, **kwargs):
    # TODO: Should consider using LVA API instead of directly querying the DB for this
    druid_engine = get_druid_engine()
    es = get_nccs_cadence_es()

    sim_config: ServerSimConfig = kwargs['sim_config']
    start, end = sim_config.start, sim_config.end

    job_query = """
        SELECT
            "allocation_id", "job_id", "slurm_version", "account", "group", "user", "name",
            "time_limit", "time_submission", "time_eligible", "time_start", "time_end", "time_elapsed",
            "node_count", xnames_str AS "xnames", "state_current", "state_reason",
            "time_snapshot"
        FROM "stf218.frontier.job-summary"
        WHERE
            (time_end IS NULL OR time_end > CONVERT(?, TIMESTAMP)) AND
            (time_start IS NOT NULL AND time_start <= CONVERT(?, TIMESTAMP))
    """
    job_query_params = [start.isoformat(), end.isoformat()]
    job_data = es_sql_query(es, job_query, job_query_params, fetch_size=500)

    job_df = pd.DataFrame(job_data)
    job_df['time_snapshot'] = pd.to_datetime(job_df['time_snapshot'])
    job_df["time_submission"] = pd.to_datetime(job_df["time_submission"])
    job_df["time_eligible"] = pd.to_datetime(job_df["time_eligible"])
    job_df["time_start"] = pd.to_datetime(job_df["time_start"])
    job_df["time_end"] = pd.to_datetime(job_df["time_end"])
    job_df['xnames'] = job_df['xnames'].map(lambda x: x.split(",") if x else [])

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
    job_profile_df = pd.read_sql(job_profile_query, druid_engine, parse_dates=[
        "timestamp",
    ])

    from loguru import logger
    logger.info(f"job_df {job_df}")
    logger.info(f"job_profile_df {job_profile_df}")

    if (job_df.empty or job_profile_df.empty):
        raise SimException(f"No telemetry data for {start.isoformat()} -> {end.isoformat()}")

    return load_data_from_df(job_df, job_profile_df, **kwargs)

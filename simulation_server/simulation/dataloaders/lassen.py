import pandas as pd
import sqlalchemy as sqla
from datetime import timedelta

from ...util.druid import get_druid_engine, get_table, to_timestamp
from ...util.dataloader import query_time_range
from ...models.sim import ServerSimConfig

# Re-use these from the raps dataloader
from raps.dataloaders.lassen import load_data_from_df, node_index_to_name, cdu_index_to_name, cdu_pos


def load_data(_paths, **kwargs):
    druid_engine = get_druid_engine()
    sim_config: ServerSimConfig = kwargs['sim_config']
    start, end = sim_config.start, sim_config.end

    allocation_df = query_time_range(
        "svc-ts-exadigit-data-lassen-allocation-history",
        start, end, 'begin_time', 'end_time',
        druid_engine = druid_engine,
        parse_dates = ["begin_time", "end_time", "job_submit_time"],
    )
    # load_data_from_df expects naive datetimes
    allocation_df["begin_time"] = allocation_df["begin_time"].dt.tz_localize(None)
    allocation_df["end_time"] = allocation_df["end_time"].dt.tz_localize(None)
    allocation_df["job_submit_time"] = allocation_df["job_submit_time"].dt.tz_localize(None)

    tbl = get_table("svc-ts-exadigit-data-lassen-node-history", druid_engine)
    node_query = (
        sqla.select(sqla.text("*"))
            .where(
                (tbl.c['__time'] <= to_timestamp(end)) &
                (tbl.c['__time'] >= to_timestamp(start - timedelta(days=3)))
            )
    )
    node_df = pd.read_sql(node_query, druid_engine)

    # step_df doesn't appear to actually be used by load_data_from_df?
    step_df = query_time_range(
        "svc-ts-exadigit-data-lassen-step-history", start, end, 'begin_time', 'end_time',
        druid_engine = druid_engine,
        parse_dates = ["begin_time", "end_time"],
    )
    step_df["begin_time"] = step_df["begin_time"].dt.tz_localize(None)
    step_df["end_time"] = step_df["end_time"].dt.tz_localize(None)

    return load_data_from_df(
        allocation_df = allocation_df, node_df = node_df, step_df = step_df,
        **kwargs,
    )

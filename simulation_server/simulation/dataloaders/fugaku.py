from ...util.druid import get_druid_engine
from ...util.dataloader import query_time_range
from ...models.sim import ServerSimConfig

# Re-use these from the raps dataloader
from raps.dataloaders.fugaku import load_data_from_df, node_index_to_name, cdu_index_to_name, cdu_pos


def load_data(_paths, **kwargs):
    druid_engine = get_druid_engine()
    sim_config: ServerSimConfig = kwargs['sim_config']
    start, end = sim_config.start, sim_config.end
    df = query_time_range(
        "svc-ts-exadigit-data-fugaku", start, end, 'sdt', 'edt',
        druid_engine = druid_engine,
        parse_dates = ["adt", "qdt", "schedsdt", "deldt", "sdt", "edt"],
    )
    return load_data_from_df(df, **kwargs)

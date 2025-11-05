from ...util.druid import get_druid_engine
from ...util.dataloader import query_time_range, split_list
from ...models.sim import ServerSimConfig

# Re-use these from the raps dataloader
from raps.dataloaders.marconi100 import load_data_from_df, node_index_to_name, cdu_index_to_name, cdu_pos


def load_data(_paths, **kwargs):
    druid_engine = get_druid_engine()
    sim_config: ServerSimConfig = kwargs['sim_config']
    start, end = sim_config.start, sim_config.end
    df = query_time_range(
        "svc-ts-exadigit-data-marconi100",
        start, end, 'start_time', 'end_time',
        druid_engine = druid_engine,
        parse_dates = ["submit_time", "start_time", "end_time", "eligible_time"],
    )
    df['nodes'] = df['nodes'].map(split_list)
    df['node_power_consumption'] = df['node_power_consumption'].map(split_list)
    df['mem_power_consumption'] = df['mem_power_consumption'].map(split_list)
    df['cpu_power_consumption'] = df['cpu_power_consumption'].map(split_list)

    return load_data_from_df(df, **kwargs)

from typing import NamedTuple
from datetime import datetime, timedelta
from pathlib import Path
import random, math, functools, json
import pandas as pd
import numpy as np
import sqlalchemy as sqla
from loguru import logger
from .raps.raps.config import initialize_config, get_config
initialize_config('frontier')
from .raps.raps.cooling import ThermoFluidsModel
from .raps.raps.power import PowerManager
from .raps.raps.flops import FLOPSManager
from .raps.raps.scheduler import Scheduler
from .raps.raps.telemetry import index_to_xname, xname_to_index, Telemetry
from .raps.raps.workload import Workload
from ..models.sim import SimConfig
from ..models.output import (
    JobStateEnum, SchedulerSimJob, SchedulerSimJobPowerHistory, SchedulerSimSystem, CoolingSimCDU,
)
from ..util.druid import get_druid_engine, get_table, to_timestamp
from .node_set import FrontierNodeSet


MODELS_PATH = Path(__file__).parent.parent.parent / 'models'

config = get_config()

SC_SHAPE = config.get("SC_SHAPE")
TOTAL_NODES = config.get("TOTAL_NODES")
DOWN_NODES = config.get("DOWN_NODES")
FMU_PATH = config.get("FMU_PATH")
# TODO: Fetch this from mlflow
FMU_PATH = MODELS_PATH / "DataCenterTH_Examples_fullSystem_Test1_FMU_export.fmu"

class SimException(Exception):
    pass


def _offset_to_time(start, offset):
    if offset is not None:
        return start + timedelta(seconds=offset)
    else:
        return None


@functools.lru_cache(maxsize = 65_536)
def _parse_nodes(node_indexes: tuple[int]):
    """
    Memoized function to convert raps indexes into xnames and node_ranges.
    Memo increases performance since it gets called on snapshots of the same job multiple times.
    """
    xnames = [index_to_xname(i) for i in node_indexes]
    hostnames = (FrontierNodeSet.xname_to_hostname(x) for x in xnames)
    node_ranges = str(FrontierNodeSet(','.join(hostnames))) # Collapse list into ranges
    return xnames, node_ranges


CDU_INDEXES = [
    'x2002c1', 'x2003c1', 'x2006c1', 'x2009c1', 'x2102c1', 'x2103c1', 'x2106c1', 'x2109c1',
    'x2202c1', 'x2203c1', 'x2206c1', 'x2209c1', 'x2302c1', 'x2303c1', 'x2306c1', 'x2309c1',
    'x2402c1', 'x2403c1', 'x2406c1', 'x2409c1', 'x2502c1', 'x2503c1', 'x2506c1', 'x2509c1',
    'x2609c1',
]
def _cdu_index_to_xname(index: int):
    return CDU_INDEXES[index - 1]


class SimOutput(NamedTuple):
    scheduler_sim_system: list[SchedulerSimSystem]
    scheduler_sim_jobs: list[SchedulerSimJob]
    cooling_sim_cdus: list[CoolingSimCDU]
    power_history: list[SchedulerSimJobPowerHistory]


def build_jobs_query(start: datetime, end: datetime, engine: sqla.engine.Engine):
    job_tbl = get_table("sens-svc-log-frontier-joblive", engine)

    # This is overly complicated. Should move jobs to postgres so we can do normal SQL style queries
    inner_query = (
        sqla.select(
            sqla.func.max(job_tbl.c['__time']).label('time_snapshot'), job_tbl.c.job_id,
        )
        .where(
            job_tbl.c.time_start != None,
            to_timestamp(start) < sqla.func.coalesce(to_timestamp(job_tbl.c.time_end), job_tbl.c['__time']),
            to_timestamp(job_tbl.c.time_start) < to_timestamp(end),
        )
        .group_by("job_id")
        .subquery().alias("inner")
    )

    outer_query = (
        sqla.select(
            job_tbl.c['__time'].label("time_snapshot"),
            *[v for k, v in job_tbl.c.items() if k != '__time'],
        )
        .join(inner_query,
            (inner_query.c.time_snapshot == job_tbl.c['__time']) &
            (inner_query.c.job_id == job_tbl.c.job_id)
        )
    )

    return outer_query


def fetch_telemetry_data(start: datetime, end: datetime):
    """
    Fetch and parse real telemetry data
    """
    engine = get_druid_engine()

    job_query = build_jobs_query(start, end, engine)
    job_data = pd.read_sql_query(job_query, engine, parse_dates=[
        "time_snapshot", "time_submission", "time_eligible", "time_start", "time_end",
        "batch_time_start", "batch_time_end",
    ])
    # TODO: Even with sqlStringifyArrays: false, multivalue columns are returned as json strings.
    # And single rows are returned as raw strings. When we update Druid we can use ARRAYS and remove
    # this. Moving the jobs table to postgres would also fix this (and other issues).
    def split_xnames(xnames):
        if not xnames:
            return []
        elif xnames.startswith('['):
            return json.loads(xnames)
        else:
            return [xnames]
    job_data['xnames'] = job_data['xnames'].map(split_xnames)

    job_profile_tbl = get_table("pub-ts-frontier-job-profile", engine)
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
    job_profile_data = pd.read_sql(job_profile_query, engine, parse_dates=[
        "timestamp",
    ])

    if (job_data.empty or job_profile_data.empty):
        raise SimException(f"No telemetry data for {start.isoformat()} -> {end.isoformat()}")

    telemetry = Telemetry()
    jobs = telemetry.parse_dataframes(job_data, job_profile_data, min_time=start)
    return jobs


def run_simulation(config: SimConfig):
    sample_scheduler_sim_system = timedelta(seconds = 1).total_seconds()
    sample_scheduler_sim_jobs = timedelta(seconds = 10).total_seconds()
    # Sample CDU as fast as it is available
    sample_cooling_sim_cdus = timedelta(seconds = 1).total_seconds()

    # Keep record of how many power history steps we've emitted for each job
    power_history_counts: dict[int, int] = {}

    if config.scheduler.enabled:
        if config.scheduler.seed:
            # TODO: This is globabl and should probably be done in RAPS
            random.seed(config.scheduler.seed)
            np.random.seed(config.scheduler.seed)

        timesteps = math.ceil((config.end - config.start).total_seconds())
        down_nodes = [*DOWN_NODES, *config.scheduler.down_nodes]

        if config.cooling.enabled:
            cooling_model = ThermoFluidsModel(str(FMU_PATH))
            cooling_model.initialize()
        else:
            cooling_model = None

        power_manager = PowerManager(SC_SHAPE, down_nodes)
        flops_manager = FLOPSManager(SC_SHAPE)

        sc = Scheduler(
            TOTAL_NODES, down_nodes,
            power_manager = power_manager,
            flops_manager = flops_manager,
            layout_manager = None,
            cooling_model = cooling_model,
            debug = False,
        )

        if config.scheduler.jobs_mode == "random":
            num_jobs = config.scheduler.num_jobs if config.scheduler.num_jobs is not None else 1000
            workload = Workload(sc) # Why does Workload take a Scheduler? It never uses it.
            jobs = workload.random(num_jobs=num_jobs)
        elif config.scheduler.jobs_mode == "test":
            workload = Workload(sc)
            jobs = workload.test()
        elif config.scheduler.jobs_mode == "replay":
            logger.info("Fetching telemetry data")
            jobs = fetch_telemetry_data(config.start, config.end)
        elif config.scheduler.jobs_mode == "custom":
            raise SimException("Custom not supported")
        else:
            raise SimException(f'Unknown jobs_mode "{config.scheduler.jobs_mode}"')

        for data in sc.run_simulation(jobs, timesteps=timesteps):
            timestamp: datetime = _offset_to_time(config.start, data.current_time)
            is_last_tick = (timestamp + timedelta(seconds=1) == config.end)

            unix_timestamp = int(timestamp.timestamp())

            scheduler_sim_system: list[SchedulerSimSystem] = []
            if unix_timestamp % sample_scheduler_sim_system == 0 or is_last_tick:
                down_nodes, _ = _parse_nodes(tuple(data.down_nodes))
                stats = sc.get_stats()

                scheduler_sim_system = [SchedulerSimSystem.model_validate(dict(
                    timestamp = timestamp,
                    down_nodes = down_nodes,
                    # TODO: Update sc.get_stats to return more easily parsable data
                    num_samples = int(stats['num_samples']),

                    jobs_completed = int(stats['jobs completed']),
                    jobs_running = len(stats['jobs still running']),
                    jobs_pending = len(stats['jobs still in queue']),

                    throughput = float(stats['throughput'].split(' ')[0]),
                    average_power = float(stats['average power'].split(' ')[0]) * 1_000_000,
                    min_loss = float(stats['min loss'].split(' ')[0]) * 1_000_000,
                    average_loss = float(stats['average loss'].split(' ')[0]) * 1_000_000,
                    max_loss = float(stats['max loss'].split(' ')[0]) * 1_000_000,
                    system_power_efficiency = float(stats['system power efficiency']),
                    total_energy_consumed = float(stats['total energy consumed'].split(' ')[0]),
                    carbon_emissions = float(stats['carbon emissions'].split(' ')[0]),
                    total_cost = float(stats['total cost'].removeprefix("$")),
                ))]

            scheduler_sim_jobs: list[SchedulerSimJob] = []
            if unix_timestamp % sample_scheduler_sim_jobs == 0 or is_last_tick:
                for job in data.jobs:
                    # end_time is set to its planned end once its scheduled. Set it to None for unfinished jobs here
                    time_end = _offset_to_time(config.start, job.end_time) if job.start_time else None

                    xnames, node_ranges = _parse_nodes(tuple(job.scheduled_nodes))
                    scheduler_sim_jobs.append(SchedulerSimJob.model_validate(dict(
                        job_id = str(job.id) if job.id else None,
                        name = job.name,
                        node_count = job.nodes_required,
                        time_snapshot = timestamp,
                        time_submission = _offset_to_time(config.start, job.submit_time),
                        time_limit = job.wall_time,
                        time_start = _offset_to_time(config.start, job.start_time),
                        time_end = time_end,
                        state_current = JobStateEnum(job.state.name),
                        node_ranges = node_ranges,
                        xnames = xnames,
                        # How does the new job.power attribute work? Is it total_energy?
                        # Or just the current wattage?
                        # power = job.power,
                    )))

            power_history: list[SchedulerSimJobPowerHistory] = []
            for job in data.jobs:
                if job.id and power_history_counts.get(job.id, 0) < len(job.power_history):
                    power_history.append(SchedulerSimJobPowerHistory(
                        timestamp = timestamp,
                        job_id = str(job.id),
                        power = job.power_history[-1],
                    ))
                    power_history_counts[job.id] = len(job.power_history)


            cooling_sim_cdus: list[CoolingSimCDU] = []
            if data.cooling_df is not None and (unix_timestamp % sample_cooling_sim_cdus == 0 or is_last_tick):
                for i, point in data.cooling_df.iterrows():
                    xname = _cdu_index_to_xname(int(point["CDU"]))
                    row, col = int(xname[2]), int(xname[3:5])
                    model = dict(
                        timestamp = timestamp,
                        xname = xname,
                        row = row,
                        col = col,

                        rack_1_power = point['Rack 1'],
                        rack_2_power = point['Rack 2'],
                        rack_3_power = point['Rack 3'],
                        total_power = point['Sum'],

                        rack_1_loss = point['Loss 1'],
                        rack_2_loss = point['Loss 2'],
                        rack_3_loss = point['Loss 3'],
                        total_loss = point['Loss'],
                    )

                    if config.cooling.enabled:
                        model.update(
                            work_done_by_cdup = point['W_CDUP_Out'],
                            rack_return_temp = point['Tr_sec_Out'],
                            rack_supply_temp = point['Ts_sec_Out'],
                            rack_supply_pressure = point['ps_sec_Out'],
                            rack_return_pressure = point['pr_sec_Out'],
                            rack_flowrate = point['Q_sec_Out'],
                            htw_ctw_flowrate = point['Q_fac_Out'],
                            htwr_htws_ctwr_ctws_pressure = point['p_fac_Out'],
                            htwr_htws_ctwr_ctws_temp = point['T_fac_Out'],
                            power_cunsumption_htwps = point['W_HTWP_Out'],
                            power_consumption_ctwps = point['W_CTWP_Out'],
                            power_consumption_fan = point['W_CT_Out'],
                            htwp_speed = point['N_HTWP_Out'],
                            nctwps_staged = point['n_CTWPs_Out'],
                            nhtwps_staged = point['n_HTWPs_Out'],
                            pue_output = point['PUE_Out'],
                            nehxs_staged = point['n_EHXs_Out'],
                            ncts_staged = point['n_CTs_Out'],
                            facility_return_temp = point['Tr_pri_Out'],
                            facility_supply_temp = point['Ts_pri_Out'],
                            facility_supply_pressure = point['ps_pri_Out'],
                            facility_return_pressure = point['pr_pri_Out'],
                            cdu_loop_bypass_flowrate = point['Q_bypass_Out'],
                            facility_flowrate = point['Q_pri_Out'],
                        )

                    cooling_sim_cdus.append(CoolingSimCDU.model_validate(model))

            yield SimOutput(
                scheduler_sim_system = scheduler_sim_system,
                scheduler_sim_jobs = scheduler_sim_jobs,
                cooling_sim_cdus = cooling_sim_cdus,
                power_history = power_history,
            )
    else:
        raise SimException("No simulations specified")

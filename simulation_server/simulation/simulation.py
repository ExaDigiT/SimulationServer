from typing import NamedTuple
from datetime import datetime, timedelta
from pathlib import Path
import random, math, functools, json
import pandas as pd
import numpy as np
import sqlalchemy as sqla
from loguru import logger
from .raps.raps.config import ConfigManager
from .raps.raps.cooling import ThermoFluidsModel
from .raps.raps.power import PowerManager
from .raps.raps.flops import FLOPSManager
from .raps.raps.scheduler import Scheduler
from .raps.raps.telemetry import Telemetry
from .raps.raps.dataloaders.frontier import index_to_xname, xname_to_index
from .raps.raps.workload import Workload
from ..models.sim import SimConfig
from ..models.output import (
    JobStateEnum, SchedulerSimJob, SchedulerSimJobPowerHistory, SchedulerSimSystem, CoolingSimCDU,
    CoolingSimCEP,
)
from ..util.druid import get_druid_engine, get_table, to_timestamp
from ..util.es import get_nccs_cadence_engine
from ..util.misc import nest_dict
from .node_set import FrontierNodeSet


PKG_PATH = Path(__file__).parent.parent.parent

class SimException(Exception):
    pass


def _offset_to_time(start, offset):
    if offset is not None:
        return start + timedelta(seconds=offset)
    else:
        return None


CDU_INDEXES = [
    'x2002c1', 'x2003c1', 'x2006c1', 'x2009c1', 'x2102c1', 'x2103c1', 'x2106c1', 'x2109c1',
    'x2202c1', 'x2203c1', 'x2206c1', 'x2209c1', 'x2302c1', 'x2303c1', 'x2306c1', 'x2309c1',
    'x2402c1', 'x2403c1', 'x2406c1', 'x2409c1', 'x2502c1', 'x2503c1', 'x2506c1', 'x2509c1',
    'x2609c1',
]
def _cdu_index_to_xname(index: int):
    return CDU_INDEXES[index - 1]


class SimOutput(NamedTuple):
    timestamp: datetime
    scheduler_sim_system: list[SchedulerSimSystem]
    scheduler_sim_jobs: list[SchedulerSimJob]
    cooling_sim_cdus: list[CoolingSimCDU]
    cooling_sim_cep: list[CoolingSimCEP]
    power_history: list[SchedulerSimJobPowerHistory]



def fetch_frontier_telemetry_data(sim_config: SimConfig, raps_config: dict):
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
            "node_count", "node_ranges", xnames_str AS "xnames", "state_current", "state_reason",
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


def get_scheduler(
    system = 'frontier',
    down_nodes = [], cooling_enabled = False, replay = False,
    schedule_policy = 'fcfs',
):
    raps_config = ConfigManager(system_name = system).get_config()
    raps_config['FMU_PATH'] = str(PKG_PATH / raps_config['FMU_PATH'])

    down_nodes = [*raps_config['DOWN_NODES'], *down_nodes]

    power_manager = PowerManager(**raps_config)
    flops_manager = FLOPSManager(**raps_config)
    if cooling_enabled:
        cooling_model = ThermoFluidsModel(**raps_config)
        cooling_model.initialize()
    else:
        cooling_model = None

    return Scheduler(
        power_manager = power_manager,
        flops_manager = flops_manager,
        layout_manager = None,
        cooling_model = cooling_model,
        debug = False, replay = replay,
        schedule = schedule_policy,
        config = raps_config,
    )


def get_job_state_hash(job: SchedulerSimJob):
    """ Return string that can be used to check if any meaningful state changed """
    return job.model_dump_json(exclude={"time_snapshot"})


def run_simulation(sim_config: SimConfig):
    sample_scheduler_sim_system = timedelta(seconds = 1).total_seconds()
    # Sample CDU as fast as it is available
    sample_cooling = timedelta(seconds = 1).total_seconds()

    # Keep record of how many power history steps we've emitted for each job
    power_history_counts: dict[int, int] = {}
    prev_jobs: dict[str, str] = {}

    if sim_config.scheduler.enabled:
        if sim_config.scheduler.seed:
            # TODO: This is globabl and should probably be done in RAPS
            random.seed(sim_config.scheduler.seed)
            np.random.seed(sim_config.scheduler.seed)

        timesteps = math.ceil((sim_config.end - sim_config.start).total_seconds())

        sc = get_scheduler(
            down_nodes = sim_config.scheduler.down_nodes,
            cooling_enabled = sim_config.cooling.enabled,
            replay = (sim_config.scheduler.jobs_mode == "replay"),
            schedule_policy = sim_config.scheduler.schedule_policy,
        )

        # Memoized function to convert raps indexes into xnames and node_ranges.
        # Memo increases performance since it gets called on snapshots of the same job multiple times.
        @functools.lru_cache(maxsize = 65_536)
        def _parse_nodes(node_indexes: tuple[int]):
            xnames = [index_to_xname(i, sc.config) for i in node_indexes]
            hostnames = (FrontierNodeSet.xname_to_hostname(x) for x in xnames)
            node_ranges = str(FrontierNodeSet(','.join(hostnames))) # Collapse list into ranges
            return xnames, node_ranges

        if sim_config.scheduler.jobs_mode == "random":
            num_jobs = sim_config.scheduler.num_jobs if sim_config.scheduler.num_jobs is not None else 1000
            workload = Workload(**sc.config)
            jobs = workload.random(num_jobs=num_jobs)
        elif sim_config.scheduler.jobs_mode == "test":
            workload = Workload(**sc.config)
            jobs = workload.test()
        elif sim_config.scheduler.jobs_mode == "replay" and sim_config.system == "frontier":
            logger.info("Fetching telemetry data")
            jobs = fetch_frontier_telemetry_data(sim_config, sc.config)
        elif sim_config.scheduler.jobs_mode == "custom":
            raise SimException("Custom not supported")
        else:
            raise SimException(f'Unknown jobs_mode "{sim_config.scheduler.jobs_mode}"')

        for data in sc.run_simulation(jobs, timesteps=timesteps):
            timestamp: datetime = _offset_to_time(sim_config.start, data.current_time)
            is_last_tick = (timestamp + timedelta(seconds=1) == sim_config.end)

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

                    p_flops = data.p_flops,
                    g_flops_w = data.g_flops_w,
                    system_util = data.system_util,
                ))]

            scheduler_sim_jobs: list[SchedulerSimJob] = []
            curr_jobs = {}
            for job in data.jobs:
                # end_time is set to its planned end once its scheduled. Set it to None for unfinished jobs here
                if job.start_time is not None:
                    time_end = _offset_to_time(sim_config.start, job.end_time)
                else:
                    time_end = None
                xnames, node_ranges = _parse_nodes(tuple(job.scheduled_nodes))
                parsed_job = SchedulerSimJob.model_validate(dict(
                    job_id = str(job.id),
                    name = job.name,
                    node_count = job.nodes_required,
                    time_snapshot = timestamp,
                    time_submission = _offset_to_time(sim_config.start, job.submit_time),
                    time_limit = job.wall_time,
                    time_start = _offset_to_time(sim_config.start, job.start_time),
                    time_end = time_end,
                    state_current = JobStateEnum(job.state.name),
                    node_ranges = node_ranges,
                    xnames = xnames,
                    # How does the new job.power attribute work? Is it total_energy?
                    # Or just the current wattage?
                    # power = job.power,
                ))
                job_state_hash = get_job_state_hash(parsed_job)

                # Output jobs if something other than time_snapshot changed
                if is_last_tick or prev_jobs.get(parsed_job.job_id) != job_state_hash:
                    scheduler_sim_jobs.append(parsed_job)
                curr_jobs[parsed_job.job_id] = job_state_hash
            prev_jobs = curr_jobs

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
            cooling_sim_cep: list[CoolingSimCEP] = []

            cooling_sim_cdu_map: dict[int, dict] = {}
            if data.power_df is not None and (unix_timestamp % sample_cooling == 0 or is_last_tick):
                for i, point in data.power_df.iterrows():
                    cooling_sim_cdu_map[int(point['CDU'])] = dict(
                        rack_1_power = point['Rack 1'],
                        rack_2_power = point['Rack 2'],
                        rack_3_power = point['Rack 3'],
                        total_power = point['Sum'],

                        rack_1_loss = point['Loss 1'],
                        rack_2_loss = point['Loss 2'],
                        rack_3_loss = point['Loss 3'],
                        total_loss = point['Loss'],
                    )

            if data.fmu_outputs:
                fmu_data = nest_dict({**data.fmu_outputs})

                # CDU columns are output in the dict with keys like this:
                # "simulator[1].datacenter[1].computeBlock[1].cdu[1].summary.m_flow_prim"
                # "simulator[1].datacenter[1].computeBlock[1].cdu[1].summary.V_flow_prim_GPM"
                # "simulator[1].datacenter[1].computeBlock[2].cdu[1].summary.m_flow_prim"
                # "simulator[1].datacenter[1].computeBlock[2].cdu[1].summary.V_flow_prim_GPM"
                # etc.

                cdus_data = fmu_data['simulator'][1]['datacenter'][1]['computeBlock']
                for cdu, cdu_data in cdus_data.items():
                    cdu_data = cdu_data['cdu'][1]['summary']
                    cooling_sim_cdu_map[cdu].update(
                        work_done_by_cdup = cdu_data['W_flow_CDUP_kW'],
                        rack_return_temp = cdu_data['T_sec_r_C'],
                        rack_supply_temp = cdu_data['T_sec_s_C'],
                        rack_supply_pressure = cdu_data['p_sec_s_psig'],
                        rack_return_pressure = cdu_data['p_sec_r_psig'],
                        rack_flowrate = cdu_data['V_flow_sec_GPM'],
                        facility_return_temp = cdu_data["T_prim_r_C"],
                        facility_supply_temp = cdu_data['T_prim_s_C'],
                        facility_supply_pressure = cdu_data['p_prim_s_psig'],
                        facility_return_pressure = cdu_data['p_prim_r_psig'],
                        facility_flowrate = cdu_data['V_flow_prim_GPM'],
                    )
                
                cep_data = fmu_data['simulator'][1]['centralEnergyPlant'][1]
                cooling_sim_cep = [CoolingSimCEP.model_validate(dict(
                    timestamp = timestamp,
                    htw_flowrate = cep_data['hotWaterLoop'][1]['summary']['V_flow_htw_GPM'],
                    ctw_flowrate = cep_data['coolingTowerLoop'][1]['summary']['V_flow_ctw_GPM'],
                    htw_return_pressure = cep_data['hotWaterLoop'][1]['summary']['p_fac_htw_r_psig'],
                    htw_supply_pressure = cep_data['hotWaterLoop'][1]['summary']['p_fac_htw_s_psig'],
                    ctw_return_pressure = cep_data['coolingTowerLoop'][1]['summary']['p_fac_ctw_r_psig'],
                    ctw_supply_pressure = cep_data['coolingTowerLoop'][1]['summary']['p_fac_ctw_s_psig'],
                    htw_return_temp = cep_data['hotWaterLoop'][1]['summary']['T_fac_htw_r_C'],
                    htw_supply_temp = cep_data['hotWaterLoop'][1]['summary']['T_fac_htw_s_C'],
                    ctw_return_temp = cep_data['coolingTowerLoop'][1]['summary']['T_fac_ctw_r_C'],
                    ctw_supply_temp = cep_data['coolingTowerLoop'][1]['summary']['T_fac_ctw_s_C'],
                    power_consumption_htwps = cep_data['hotWaterLoop'][1]['summary']['W_flow_HTWP_kW'],
                    power_consumption_ctwps = cep_data['coolingTowerLoop'][1]['summary']['W_flow_CTWP_kW'],
                    power_consumption_fan = cep_data['coolingTowerLoop'][1]['summary']['W_flow_CT_kW'],
                    htwp_speed = cep_data['hotWaterLoop'][1]['summary']['N_HTWP'],
                    nctwps_staged = cep_data['coolingTowerLoop'][1]['summary']['n_CTWPs'],
                    nhtwps_staged = cep_data['hotWaterLoop'][1]['summary']['n_HTWPs'],
                    pue_output = fmu_data['pue'],
                    nehxs_staged = cep_data['hotWaterLoop'][1]['summary']['n_EHXs'],
                    ncts_staged = cep_data['coolingTowerLoop'][1]['summary']['n_CTs'],
                    facility_return_temp = cep_data['hotWaterLoop'][1]['summary']['T_fac_htw_r_C'],
                    cdu_loop_bypass_flowrate = fmu_data['simulator'][1]['datacenter'][1]['summary']['V_flow_bypass_GPM'],
                ))]


            for cdu_index, cdu_data in cooling_sim_cdu_map.items():
                xname = _cdu_index_to_xname(cdu_index)
                row, col = int(xname[2]), int(xname[3:5])
                cdu_data.update(
                    timestamp = timestamp,
                    xname = xname,
                    row = row,
                    col = col,
                )
                cooling_sim_cdus.append(CoolingSimCDU.model_validate(cdu_data))

            yield SimOutput(
                timestamp = timestamp,
                scheduler_sim_system = scheduler_sim_system,
                scheduler_sim_jobs = scheduler_sim_jobs,
                cooling_sim_cdus = cooling_sim_cdus,
                cooling_sim_cep = cooling_sim_cep,
                power_history = power_history,
            )
    else:
        raise SimException("No simulations specified")

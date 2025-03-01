from typing import NamedTuple
from datetime import datetime, timedelta
from pathlib import Path
import random, math, functools
import numpy as np
from loguru import logger
from .raps.raps.config import ConfigManager
from .raps.raps.cooling import ThermoFluidsModel
from .raps.raps.power import PowerManager
from .raps.raps.flops import FLOPSManager
from .raps.raps.scheduler import Scheduler
from .raps.raps.telemetry import Telemetry
from .raps.raps.workload import Workload
from ..models.sim import SimConfig, SimSystem
from ..models.output import (
    JobStateEnum, SchedulerSimJob, SchedulerSimJobPowerHistory, SchedulerSimSystem, CoolingSimCDU,
    CoolingSimCEP,
)
from ..util.misc import nest_dict
from . import SimException
from .dataloaders import DATA_LOADERS


PKG_PATH = Path(__file__).parent.parent.parent


def _offset_to_time(start, offset):
    if offset is not None:
        return start + timedelta(seconds=offset)
    else:
        return None


class SimOutput(NamedTuple):
    timestamp: datetime
    scheduler_sim_system: list[SchedulerSimSystem]
    scheduler_sim_jobs: list[SchedulerSimJob]
    cooling_sim_cdus: list[CoolingSimCDU]
    cooling_sim_cep: list[CoolingSimCEP]
    power_history: list[SchedulerSimJobPowerHistory]


def get_scheduler(
    system: SimSystem,
    down_nodes = [], cooling_enabled = False, replay = False,
    schedule_policy = 'fcfs',
):
    if cooling_enabled and system != "frontier":
        raise SimException("Cooling sim only supported for frontier")

    raps_config = ConfigManager(system_name = system).get_config()
    if "FMU_PATH" in raps_config:
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
            system = sim_config.system,
            down_nodes = sim_config.scheduler.down_nodes,
            cooling_enabled = sim_config.cooling.enabled,
            replay = (sim_config.scheduler.jobs_mode == "replay"),
            schedule_policy = sim_config.scheduler.schedule_policy,
        )
        telemetry = Telemetry(system = sim_config.system, config = sc.config)

        # Memoized function to convert raps indexes into node names.
        # Memo increases performance since it gets called on snapshots of the same job multiple times.
        @functools.lru_cache(maxsize = 65_536)
        def _parse_nodes(node_indexes: tuple[int]):
            return [telemetry.node_index_to_name(i) for i in node_indexes]

        if sim_config.scheduler.jobs_mode == "random":
            num_jobs = sim_config.scheduler.num_jobs if sim_config.scheduler.num_jobs is not None else 1000
            workload = Workload(**sc.config)
            jobs = workload.random(num_jobs=num_jobs)
        elif sim_config.scheduler.jobs_mode == "test":
            workload = Workload(**sc.config)
            jobs = workload.test()
        elif sim_config.scheduler.jobs_mode == "replay":
            if sim_config.system not in DATA_LOADERS:
                raise SimException(f"Replay not supported for {sim_config.system}")
            logger.info("Fetching telemetry data...")
            jobs = DATA_LOADERS[sim_config.system](sim_config, sc.config)
            if len(jobs) == 0:
                raise SimException(f"No data for {sim_config.system} {sim_config.start.isoformat()} -> {sim_config.end.isoformat()}")
            logger.info(f"Fetched {len(jobs)} jobs")
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
                down_nodes = _parse_nodes(tuple(data.down_nodes))
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
            data_jobs = data.completed + data.running + data.queue
            for job in data_jobs:
                # end_time is set to its planned end once its scheduled. Set it to None for unfinished jobs here
                if job.start_time is not None:
                    time_end = _offset_to_time(sim_config.start, job.end_time)
                else:
                    time_end = None
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
                    nodes = _parse_nodes(tuple(job.scheduled_nodes)),
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
            for job in data_jobs:
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
                cdu_name = telemetry.cdu_index_to_name(cdu_index)
                row, col = telemetry.cdu_pos(cdu_index)
                cdu_data.update(
                    timestamp = timestamp,
                    name = cdu_name,
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

from typing import NamedTuple, Annotated as A
from datetime import timedelta
from pathlib import Path
import random, math, functools
import numpy as np
from .raps.raps.config import SC_SHAPE, TOTAL_NODES, DOWN_NODES, TRACE_QUANTA, FMU_PATH
from .raps.raps.cooling import ThermoFluidsModel
from .raps.raps.power import PowerManager
from .raps.raps.scheduler import Scheduler
from .raps.raps.telemetry import index_to_xname, xname_to_index, Telemetry
from ..models.sim import SimConfig
from ..models.output import JobStateEnum, SchedulerSimJob, SchedulerSimSystem, CoolingSimCDU
from .node_set import FrontierNodeSet


RAPS_PATH = Path(__file__).parent / 'raps'


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



def run_simulation(config: SimConfig):
    if config.scheduler.enabled:
        if config.scheduler.seed:
            # TODO: This is globabl and should probably be done in RAPS
            random.seed(config.scheduler.seed)
            np.random.seed(config.scheduler.seed)

        timesteps = math.ceil((config.end - config.start).total_seconds())
        down_nodes = [*DOWN_NODES, *config.scheduler.down_nodes]

        if config.cooling.enabled:
            cooling_model = ThermoFluidsModel(str(RAPS_PATH / FMU_PATH))
            cooling_model.initialize()
        else:
            cooling_model = None

        power_manager = PowerManager(SC_SHAPE, down_nodes)

        sc = Scheduler(
            TOTAL_NODES, down_nodes,
            power_manager = power_manager,
            layout_manager = None,
            cooling_model = cooling_model,
            debug = False,
        )

        if config.scheduler.jobs_mode == "replay":
            raise Exception("Replay not supported yet")
        elif config.scheduler.jobs_mode == "custom":
            raise Exception("Custom not supported")
        elif config.scheduler.jobs_mode == "random":
            num_jobs = config.scheduler.num_jobs if config.scheduler.num_jobs is not None else 1000
            jobs = sc.generate_random_jobs(num_jobs=num_jobs)

        for data in sc.run_simulation(jobs, timesteps=timesteps):
            timestamp = _offset_to_time(config.start, data.current_time)

            down_nodes = data.down_nodes # Convert to xnames
            scheduler_sim_system = [SchedulerSimSystem.model_validate(dict(
                timestamp = timestamp,
                down_nodes = [] # TODO
            ))]
            
            scheduler_sim_jobs: list[SchedulerSimJob] = []
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
                    time_end = _offset_to_time(config.start, job.end_time),
                    state_current = JobStateEnum(job.state.name),
                    node_ranges = node_ranges,
                    xnames = xnames,
                )))

            cooling_sim_cdus: list[CoolingSimCDU] = []
            if data.cooling_df is not None:
                for i, point in data.cooling_df.iterrows():
                    xname = _cdu_index_to_xname(int(point["CDU"]))
                    row, col = int(xname[2]), int(xname[3:5])
                    cooling_sim_cdus.append(CoolingSimCDU.model_validate(dict(
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
                        total_loss = point['Loss Sum'],

                        work_done_by_cdup = point['Work Done by CDUP'],
                        rack_return_temp = point['Rack Return Temperature'],
                        rack_supply_temp = point['Rack Supply Temperature'],
                        rack_supply_pressure = point['Rack Supply Pressure'],
                        rack_return_pressure = point['Rack Return Pressure'],
                        rack_flowrate = point['Rack Flowrate'],
                        htw_ctw_flowrate = point['HTW/CTW Flowrate'],
                        htwr_htws_ctwr_ctws_pressure = point['HTWR/HTWS/CTWR/CTWS Pressure'],
                        htwr_htws_ctwr_ctws_temp = point['HTWR/HTWS/CTWR/CTWS Temperature'],
                        power_cunsumption_htwps = point['Power Consumption HTWPS'],
                        power_consumption_ctwps = point['Power Consumption CTWPs'],
                        power_consumption_fan = point['Power Consumption Fan'],
                        htwp_speed = point['HTWP Speed'],
                        nctwps_staged = point['nCTWPs Staged'],
                        nhtwps_staged = point['nHTWPs Staged'],
                        pue_output = point['PUE Output'],
                        nehxs_staged = point['nEHXs Staged'],
                        ncts_staged = point['nCTs Staged'],
                        facility_return_temp = point['Facility Return Temperature'],
                        facility_supply_temp = point['Facility Supply Temperature'],
                        facility_supply_pressure = point['Facility Supply Pressure'],
                        facility_return_pressure = point['Facility Return Pressure'],
                        cdu_loop_bypass_flowrate = point['CDU Loop Bypass Flowrate'],
                        facility_flowrate = point['Facility Flowrate'],
                    )))

            yield SimOutput(
                scheduler_sim_system = scheduler_sim_system,
                scheduler_sim_jobs = scheduler_sim_jobs,
                cooling_sim_cdus = cooling_sim_cdus
            )
    else:
        raise Exception("No simulations specified")



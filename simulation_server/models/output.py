from typing import Optional
from pydantic import AwareDatetime

from ..util.misc import omit
from  ..util.api_queries import filter_params, sort_params
from .base import BaseModel, NumTimedelta
from .job_state import JobStateEnum



class SchedulerSimJob(BaseModel):
    job_id: Optional[str] = None
    """ Job id. Will be null if job is PENDING """

    name: Optional[str] = None

    node_count: Optional[int] = None
    """ Number of nodes required """

    time_snapshot: AwareDatetime
    """ Time in the Optional[simulation] of this job snapshot """
    time_submission: Optional[AwareDatetime] = None
    time_limit: Optional[NumTimedelta] = None
    time_start: Optional[AwareDatetime] = None
    time_end: Optional[AwareDatetime] = None
    state_current: Optional[JobStateEnum] = None

    node_ranges: Optional[str] = None
    """
    The Slurm hosts the job is running on as a Slurm hosts string.
    E.g. frontier[03629-03630,03633-03635]
    """
    xnames: Optional[list[str]] = None
    """
    The nodes the job is running on, but as a list of xnames rather than a slurm hostname string.
    E.g. ['x2307c3s0b1', 'x2408c5s2b1']
    """

    # Removing these for now, they are constant and just what you set in the input.
    # These may change in the future though
    # cpu_util: float
    # gpu_util: float
    # cpu_trace: list[float]
    # gpu_trace: list[float]

SCHEDULER_SIM_JOB_API_FIELDS = {
    'job_id': 'string',
    'name': 'string',
    'node_count': 'number',
    'time_snapshot': 'date',
    'time_submission': 'date',
    'time_limit': 'timedelta',
    'time_start': 'date',
    'time_end': 'date',
    'state_current': 'string',
    'node_ranges': 'string',
    'xnames': 'array[string]',
    # 'cpu_util': 'number',
    # 'gpu_util': 'number',
    # 'cpu_trace': 'array[number]',
    # 'gpu_trace': 'array[number]',
}

SCHEDULER_SIM_JOB_FIELD_SELECTORS = {
    "default": list(SchedulerSimJob.model_fields.keys()),
}

SCHEDULER_SIM_JOB_FILTERS = filter_params(
    omit(SCHEDULER_SIM_JOB_API_FIELDS, ['time_snapshot', 'node_ranges', 'xnames']) # TODO: Allow filtering on nodes
)
SCHEDULER_SIM_JOB_SORT = sort_params(
    omit(SCHEDULER_SIM_JOB_API_FIELDS, ['time_snapshot', 'node_ranges', 'xnames']),
    ["asc:time_start", "asc:time_end", "asc:job_id"],
)


class SchedulerSimJobPowerHistory(BaseModel):
    timestamp: AwareDatetime
    job_id: Optional[str] = None
    power: Optional[float] = None

SCHEDULER_SIM_JOB_POWER_HISTORY_API_FIELDS = {
    'job_id': 'string',
    'power': 'number',
}

SCHEDULER_SIM_JOB_POWER_HISTORY_FIELD_SELECTORS = {
    "default": ['power'],
    "all": ['job_id', 'power'],
}


class SchedulerSimSystem(BaseModel):
    timestamp: AwareDatetime
    
    down_nodes: list[str]
    """ List of xnames that are currently down in the simulation """
    
    num_samples: int

    jobs_completed: int
    jobs_running: int
    jobs_pending: int

    throughput: float
    """ jobs/hour """
    
    average_power: float
    """ In Watts """

    min_loss: float
    """ In watts """

    average_loss: float
    """ In watts """

    max_loss: float
    """ In watts """

    system_power_efficiency: float
    """ Percentage 0 -> 100 """
    
    total_energy_consumed: float
    """ In MW⋅hr """

    carbon_emissions: float
    """ metric tons of CO2 """

    total_cost: float
    """ Cost in US dollars """

    p_flops: Optional[float]
    """ Estimate of the floating operations per second in petaflops """

    g_flops_w: Optional[float]
    """ GigaFlops per watt energy efficiency estimate """


class CoolingSimCDU(BaseModel):
    timestamp: AwareDatetime
    xname: Optional[str] = None
    """
    Unique identifier for the CDU of the (simulated) measurement, e.g. x2007c1.
    The CDU xname is the xname of the neighboring cabinet.
    """
    row: Optional[int] = None
    """ Row index of the CDU """
    col: Optional[int] = None
    """ Col index of the cdu (Note this is the col of the neighboring cabinet.)"""

    rack_1_power: Optional[float] = None
    rack_2_power: Optional[float] = None
    rack_3_power: Optional[float] = None
    total_power: Optional[float] = None

    rack_1_loss: Optional[float] = None
    rack_2_loss: Optional[float] = None
    rack_3_loss: Optional[float] = None
    total_loss: Optional[float] = None

    work_done_by_cdup: Optional[float] = None
    rack_return_temp: Optional[float] = None
    rack_supply_temp: Optional[float] = None
    rack_supply_pressure: Optional[float] = None
    rack_return_pressure: Optional[float] = None
    rack_flowrate: Optional[float] = None
    htw_ctw_flowrate: Optional[float] = None
    htwr_htws_ctwr_ctws_pressure: Optional[float] = None
    htwr_htws_ctwr_ctws_temp: Optional[float] = None
    power_cunsumption_htwps: Optional[float] = None
    power_consumption_ctwps: Optional[float] = None
    power_consumption_fan: Optional[float] = None
    htwp_speed: Optional[float] = None
    nctwps_staged: Optional[float] = None
    nhtwps_staged: Optional[float] = None
    pue_output: Optional[float] = None
    nehxs_staged: Optional[float] = None
    ncts_staged: Optional[float] = None
    facility_return_temp: Optional[float] = None
    facility_supply_temp: Optional[float] = None
    facility_supply_pressure: Optional[float] = None
    facility_return_pressure: Optional[float] = None
    cdu_loop_bypass_flowrate: Optional[float] = None
    facility_flowrate: Optional[float] = None


COOLING_CDU_API_FIELDS = {
    "xname": 'string',
    "row": 'number',
    "col": 'number',
    "rack_1_power": 'number',
    "rack_2_power": 'number',
    "rack_3_power": 'number',
    "total_power": 'number',
    "rack_1_loss": 'number',
    "rack_2_loss": 'number',
    "rack_3_loss": 'number',
    "total_loss": 'number',
    "work_done_by_cdup": "number",
    "rack_return_temp": "number",
    "rack_supply_temp": "number",
    "rack_supply_pressure": "number",
    "rack_return_pressure": "number",
    "rack_flowrate": "number",
    "htw_ctw_flowrate": "number",
    "htwr_htws_ctwr_ctws_pressure": "number",
    "htwr_htws_ctwr_ctws_temp": "number",
    "power_cunsumption_htwps": "number",
    "power_consumption_ctwps": "number",
    "power_consumption_fan": "number",
    "htwp_speed": "number",
    "nctwps_staged": "number",
    "nhtwps_staged": "number",
    "pue_output": "number",
    "nehxs_staged": "number",
    "ncts_staged": "number",
    "facility_return_temp": "number",
    "facility_supply_temp": "number",
    "facility_supply_pressure": "number",
    "facility_return_pressure": "number",
    "cdu_loop_bypass_flowrate": "number",
    "facility_flowrate": "number",
}

COOLING_CDU_FIELD_SELECTORS = {
    "default": [*COOLING_CDU_API_FIELDS.keys()],
    "pos": ['row', 'col'],
}

COOLING_CDU_FILTERS = filter_params(COOLING_CDU_API_FIELDS)

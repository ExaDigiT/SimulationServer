from typing import Optional, Literal
from datetime import timedelta
from pydantic import AwareDatetime

from .base import BaseModel
from .job_state import JobStateEnum



class SchedulerSimJob(BaseModel):
    job_id: Optional[str]
    """ Job id. Will be null if job is PENDING """

    name: str

    node_count: int
    """ Number of nodes required """

    time_snapshot: AwareDatetime
    """ Time in the simulation of this job snapshot """
    time_submission: AwareDatetime
    time_limit: timedelta
    time_start: Optional[AwareDatetime] = None
    time_end: Optional[AwareDatetime] = None
    state_current: JobStateEnum

    node_ranges: Optional[str]
    """
    The Slurm hosts the job is running on as a Slurm hosts string.
    E.g. frontier[03629-03630,03633-03635]
    """
    xnames: list[str]
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




class SchedulerSimSystem(BaseModel):
    timestamp: AwareDatetime
    down_nodes: list[str]
    """ List of xnames that are currently down in the simulation """



class CoolingSimCDU(BaseModel):
    timestamp: AwareDatetime
    xname: str
    """
    Unique identifier for the CDU of the (simulated) measurement, e.g. x2007c1.
    The CDU xname is the xname of the neighboring cabinet.
    """
    row: int
    """ Row index of the CDU """
    col: int
    """ Col index of the cdu (Note this is the col of the neighboring cabinet.)"""

    rack_1_power: float
    rack_2_power: float
    rack_3_power: float
    total_power: float

    rack_1_loss: float
    rack_2_loss: float
    rack_3_loss: float
    total_loss: float

    work_done_by_cdup: float
    rack_return_temp: float
    rack_supply_temp: float
    rack_supply_pressure: float
    rack_return_pressure: float
    rack_flowrate: float
    htw_ctw_flowrate: float
    htwr_htws_ctwr_ctws_pressure: float
    htwr_htws_ctwr_ctws_temp: float
    power_cunsumption_htwps: float
    power_consumption_ctwps: float
    power_consumption_fan: float
    htwp_speed: float
    nctwps_staged: float
    nhtwps_staged: float
    pue_output: float
    nehxs_staged: float
    ncts_staged: float
    facility_return_temp: float
    facility_supply_temp: float
    facility_supply_pressure: float
    facility_return_pressure: float
    cdu_loop_bypass_flowrate: float
    facility_flowrate: float


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

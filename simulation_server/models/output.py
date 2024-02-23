from typing import Optional, Literal
from datetime import datetime, timedelta

from .base import BaseModel
from .job_state import JobStateEnum



class SchedulerSimJob(BaseModel):
    job_id: Optional[str]
    """ Job id. Will be null if job is PENDING """

    name: str

    node_count: int
    """ Number of nodes required """

    time_snapshot: datetime
    """ Time in the simulation of this job snapshot """
    time_submission: datetime
    time_limit: timedelta
    time_start: Optional[datetime] = None
    time_end: Optional[datetime] = None
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
    timestamp: datetime
    down_nodes: list[str]
    """ List of xnames that are currently down in the simulation """



class CoolingSimCDU(BaseModel):
    timestamp: datetime
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
 
    liquid_inlet_0_flow_primary: float
    liquid_inlet_0_temperature_primary: float
    liquid_outlet_0_temperature_primary: float
    liquid_inlet_0_pressure_primary: float
    liquid_outlet_0_pressure_primary: float
    liquid_outlet_0_flow_secondary: float
    liquid_inlet_1_temperature_secondary: float
    liquid_outlet_1_temperature_secondary: float
    liquid_inlet_1_pressure_secondary: float
    liquid_outlet_1_pressure_secondary: float
    pump_1_input_pressure_secondary: float


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
    "liquid_inlet_0_flow_primary": 'number',
    "liquid_inlet_0_temperature_primary": 'number',
    "liquid_outlet_0_temperature_primary": 'number',
    "liquid_inlet_0_pressure_primary": 'number',
    "liquid_outlet_0_pressure_primary": 'number',
    "liquid_outlet_0_flow_secondary": 'number',
    "liquid_inlet_1_temperature_secondary": 'number',
    "liquid_outlet_1_temperature_secondary": 'number',
    "liquid_inlet_1_pressure_secondary": 'number',
    "liquid_outlet_1_pressure_secondary": 'number',
    "pump_1_input_pressure_secondary": 'number',
}

COOLING_CDU_FIELD_SELECTORS = {
    "default": [
        'xname',
        'pow',
        'power',
        'loss',
        'liquid_inlet_0_flow_primary',
        'liquid_inlet_0_temperature_primary',
        'liquid_outlet_0_temperature_primary',
        'liquid_inlet_0_pressure_primary',
        'liquid_outlet_0_pressure_primary',
        'liquid_outlet_0_flow_secondary',
        'liquid_inlet_1_temperature_secondary',
        'liquid_outlet_1_temperature_secondary',
        'liquid_inlet_1_pressure_secondary',
        'liquid_outlet_1_pressure_secondary',
        'pump_1_input_pressure_secondary',
    ],
    "pos": ['row', 'col'],
    "power": ['rack_1_power', 'rack_2_power', 'rack_3_power', 'total_power'],
    "loss": ['rack_1_loss','rack_2_loss','rack_3_loss','total_loss'],
}


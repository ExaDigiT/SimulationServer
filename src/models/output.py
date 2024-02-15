from typing import Optional, Literal
from datetime import datetime, timedelta

from .base import BaseModel
from .job_state import JobStateEnum
from ..util.depends import ApiField
from ..util.misc import fields_from_selectors



class SchedulerSimJob(BaseModel):
    job_id: Optional[str]
    """ Job id. Will be null if job is PENDING """

    name: str

    allocation_nodes: int
    """ Number of nodes required """

    time_snapshot: datetime
    """ Time in the simulation of this job snapshot """
    time_submission: datetime
    time_limit: timedelta
    time_start: Optional[datetime] = None
    time_end: Optional[datetime] = None
    state_current: JobStateEnum

    nodes: Optional[str]
    """
    The Slurm hosts the job is running on as a Slurm hosts string.
    E.g. frontier[03629-03630,03633-03635]
    """
    xnames: list[str]
    """
    The nodes the job is running on, but as a list of xnames rather than a slurm hostname string.
    E.g. ['x2307c3s0b1', 'x2408c5s2b1']
    """

    cpu_util: float
    gpu_util: float
    cpu_trace: list[float]
    gpu_trace: list[float]

SCHEDULER_SIM_JOB_API_FIELDS = [
    ApiField('job_id', str, 'string'),
    ApiField('name', str, 'string'),
    ApiField('allocation_nodes', int, 'number'),
    ApiField('time_snapshot', datetime, 'date'),
    ApiField('time_submission', datetime, 'date'),
    # ApiField('time_limit', timedelta, 'number'), # TODO
    ApiField('time_start', datetime, 'date'),
    ApiField('time_end', datetime, 'date'),
    ApiField('state_current', str, 'string'),
    ApiField('nodes', str, 'string'),
    # ApiField('xnames', list[str], 'number'), # TODO
    # ApiField('cpu_util', float, 'number'),
    # ApiField('gpu_util', float, 'number'),
    # ApiField('cpu_trace', list[float], 'number'),
    # ApiField('gpu_trace', list[float], 'number'),
]

SCHEDULER_SIM_JOB_FIELD_SELECTORS = {
    "default": list(SchedulerSimJob.model_fields.keys()),
}

SchedulerSimJobFieldSelector = Literal[fields_from_selectors(SCHEDULER_SIM_JOB_FIELD_SELECTORS)]



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


COOLING_CDU_API_FIELDS: list[ApiField] = [
    ApiField("xname", str, 'string'),
    ApiField("row", int, 'number'),
    ApiField("col", int, 'number'),
    ApiField("rack_1_power", float, 'number'),
    ApiField("rack_2_power", float, 'number'),
    ApiField("rack_3_power", float, 'number'),
    ApiField("total_power", float, 'number'),
    ApiField("rack_1_loss", float, 'number'),
    ApiField("rack_2_loss", float, 'number'),
    ApiField("rack_3_loss", float, 'number'),
    ApiField("total_loss", float, 'number'),
    ApiField("liquid_inlet_0_flow_primary", float, 'number'),
    ApiField("liquid_inlet_0_temperature_primary", float, 'number'),
    ApiField("liquid_outlet_0_temperature_primary", float, 'number'),
    ApiField("liquid_inlet_0_pressure_primary", float, 'number'),
    ApiField("liquid_outlet_0_pressure_primary", float, 'number'),
    ApiField("liquid_outlet_0_flow_secondary", float, 'number'),
    ApiField("liquid_inlet_1_temperature_secondary", float, 'number'),
    ApiField("liquid_outlet_1_temperature_secondary", float, 'number'),
    ApiField("liquid_inlet_1_pressure_secondary", float, 'number'),
    ApiField("liquid_outlet_1_pressure_secondary", float, 'number'),
    ApiField("pump_1_input_pressure_secondary", float, 'number'),
]

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

CoolingSimCDUFieldSelector = Literal[fields_from_selectors(SCHEDULER_SIM_JOB_FIELD_SELECTORS)]

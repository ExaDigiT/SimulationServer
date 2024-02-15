from typing import Optional
from datetime import datetime, timedelta

from .base import BaseModel
from .job_state import JobStateEnum


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

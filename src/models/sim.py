from __future__ import annotations
from typing import Optional, Literal, Annotated as A
from datetime import datetime, timedelta
from pydantic import AwareDatetime, model_validator, Field

from .base import BaseModel
from .job_state import JobStateEnum


class Sim(BaseModel):
    """ Represents a single simulation run """

    id: str
    """ Unique id of the simulation """

    user: str
    """ User who launched the simulation """

    state: Literal['pending', 'success', 'fail']

    logical_start: datetime
    """
    The start of the date range the simulation is running over.

    If the simulation is configured to use real telemetry data (such as replaying jobs) then this is
    the start of the date range the telemetry will come from.
    If the simulation is simulating everything from scratch, then the start/end date range only 
    matters to set how long the simulation should run for.
    """

    logical_end: datetime
    """ See sim_start """

    run_start: datetime
    """ The real time the simulation started running """

    run_end: Optional[datetime]
    """ The real time the simulation finished """

    progress: A[float, Field(ge=0, le=1)]
    """ Float from 0 -> 1 summarizing the simulation progress """

    config: SimConfig


class SimConfig(BaseModel):
    start: AwareDatetime
    end: AwareDatetime

    scheduler: A[SchedulerSimConfig, Field(default_factory=lambda: SchedulerSimConfig())]
    cooling: A[CoolingSimConfig, Field(default_factory=lambda: CoolingSimConfig())]

    @model_validator(mode='after')
    def validate_model(self):
        if not any(m.enabled for m in [self.scheduler, self.cooling]):
            raise ValueError("Must enable one simulation")
        if self.cooling.enabled and not self.scheduler.enabled:
            raise ValueError("Currently can't run cooling simulation without the scheduler")
        return self


class SchedulerSimConfig(BaseModel):
    """
    Config for RAPS job simulation.
    There are 3 main "modes" for how to run the jobs.
    - replay: Replay data based on the real jobs run on Frontier during start/end
    - custom: Pass your own set of jobs to submit in the simulation in `jobs`
    - random: Run random jobs. You can pass `seed` and `num_jobs` to customize it.
    """

    enabled: bool = False
    down_nodes: list[str] = [] # List of hostnames. TODO: allow parsing from xnames/hostname or int maybe?
    
    jobs_mode: Literal['replay', 'custom', 'random'] = 'random'

    jobs: Optional[list[SchedulerSimCustomJob]] = None
    """
    The list of jobs.
    Only applicable if jobs_mode is "custom"
    """

    seed: Optional[int] = None
    """
    Random seed for consistent random job generation.
    Only applicable if jobs_mode is "random"
    """
    num_jobs: Optional[int] = None
    """
    Number of random jobs to generate.
    Only applicable if jobs_mode is "random"
    """


class SchedulerSimCustomJob(BaseModel):
    # This is mostly a subset of the SchedulerSimJob
    name: str
    allocation_nodes: int
    """ Number of nodes required """
    time_submission: datetime
    time_limit: timedelta

    cpu_util: float
    gpu_util: float
    cpu_trace: list[float]
    gpu_trace: list[float]

    end_state: JobStateEnum
    """ Slurm state job will end in """


class CoolingSimConfig(BaseModel):
    enabled: bool = False

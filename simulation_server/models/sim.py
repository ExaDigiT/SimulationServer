from __future__ import annotations
from typing import Optional, Literal, Annotated as A
from datetime import timedelta
import json
from pydantic import AwareDatetime, model_validator, Field

from .base import BaseModel
from .job_state import JobStateEnum


class Sim(BaseModel):
    """ Represents a single simulation run """

    id: Optional[str] = None
    """ Unique id of the simulation """

    user: Optional[str] = None
    """ User who launched the simulation """

    state: Optional[Literal['running', 'success', 'fail']] = None

    logical_start: Optional[AwareDatetime] = None
    """
    The start of the date range the simulation is running over.

    If the simulation is configured to use real telemetry data (such as replaying jobs) then this is
    the start of the date range the telemetry will come from.
    If the simulation is simulating everything from scratch, then the start/end date range only 
    matters to set how long the simulation should run for.
    """

    logical_end: Optional[AwareDatetime] = None
    """ See sim_start """

    run_start: Optional[AwareDatetime] = None
    """ The real time the simulation started running """

    run_end: Optional[AwareDatetime] = None
    """ The real time the simulation finished """

    progress: A[Optional[float], Field(ge=0, le=1)] = None
    """ Float from 0 -> 1 summarizing the simulation progress """

    config: Optional[dict] = None
    """ Original config used to run the simulation """


    def serialize_for_druid(self):
        timestamp = self.run_end or self.run_start
        return json.dumps({
            **self.model_dump(mode = 'json', exclude = {"progress"}),
            "timestamp": timestamp.isoformat(),
            "config": json.dumps(self.config),
        }).encode()


SIM_API_FIELDS = {
    'id': 'string',
    'user': 'string',
    'state': 'string',
    'logical_start': 'date',
    'logical_end': 'date',
    'run_start': 'date',
    'run_end': 'date',
    'progress': 'number',
    'config': 'string',
}
SIM_FIELD_SELECTORS = {
    "default": ["id", "user", "state", "logical_start", "logical_end", "run_start", "run_end", "progress"],
    "all": [*SIM_API_FIELDS.keys()],
}

class SimConfig(BaseModel):
    start: AwareDatetime
    end: AwareDatetime

    scheduler: A[SchedulerSimConfig, Field(default_factory=lambda: SchedulerSimConfig())]
    cooling: A[CoolingSimConfig, Field(default_factory=lambda: CoolingSimConfig())]

    @model_validator(mode='after')
    def validate_model(self):
        if self.end <= self.start:
            raise ValueError("Start must be less than end")

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
    time_submission: AwareDatetime
    time_limit: timedelta

    cpu_util: float
    gpu_util: float
    cpu_trace: list[float]
    gpu_trace: list[float]

    end_state: JobStateEnum
    """ Slurm state job will end in """


class CoolingSimConfig(BaseModel):
    enabled: bool = False

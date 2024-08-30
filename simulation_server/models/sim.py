from __future__ import annotations
from typing import Optional, Literal, Annotated as A
from datetime import timedelta, datetime, timezone
import json, math
from pydantic import AwareDatetime, model_validator, field_validator, Field

from .base import BaseModel
from .job_state import JobStateEnum
from ..util.misc import omit
from ..util.api_queries import filter_params, sort_params

class Sim(BaseModel):
    """ Represents a single simulation run """

    id: str
    """ Unique id of the simulation """

    user: Optional[str] = None
    """ User who launched the simulation """

    state: Optional[Literal['running', 'success', 'fail']] = None

    error_messages: Optional[str] = None
    """ Any warnings or error messages from the simulation run """

    start: Optional[AwareDatetime] = None
    """
    The start of the date range the simulation is running over.

    If the simulation is configured to use real telemetry data (such as replaying jobs) then this is
    the start of the date range the telemetry will come from.
    If the simulation is simulating everything from scratch, then the start/end date range only 
    matters to set how long the simulation should run for.
    """

    end: Optional[AwareDatetime] = None
    """ See sim_start """

    execution_start: Optional[AwareDatetime] = None
    """ The real time the simulation started running """

    execution_end: Optional[AwareDatetime] = None
    """ The real time the simulation finished """

    progress: A[Optional[float], Field(ge=0, le=1)] = None
    """ Float from 0 -> 1 summarizing the simulation progress """

    config: Optional[dict] = None
    """ Original config used to run the simulation """


    def serialize_for_druid(self):
        timestamp = self.execution_end or self.execution_start
        return json.dumps({
            **self.model_dump(mode = 'json', exclude = {"progress"}),
            "timestamp": timestamp.isoformat(),
            "config": json.dumps(self.config),
        }).encode()


SIM_API_FIELDS = {
    'id': 'string',
    'user': 'string',
    'state': 'string',
    'error_messages': 'string',
    'start': 'date',
    'end': 'date',
    'execution_start': 'date',
    'execution_end': 'date',
    'progress': 'number',
    'config': 'string',
}
SIM_FIELD_SELECTORS = {
    "default": [
        "user", "state", "error_messages", "start", "end", "execution_start", "execution_end",
        "progress",
    ],
    "all": ['default', 'config'],
}
SIM_FILTERS = filter_params(omit(SIM_API_FIELDS, ['progress', 'config']))
SIM_SORT = sort_params(omit(SIM_API_FIELDS, ['progress', 'config']), [
    "asc:execution_start", "asc:execution_end", "asc:start", "asc:end", "asc:id",
])

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

    @field_validator("start", mode="after")
    @classmethod
    def trunc_start(cls, v: datetime, info):
        return v.fromtimestamp(math.floor(v.timestamp()), tz=timezone.utc)

    @field_validator("end", mode="after")
    @classmethod
    def trunc_end(cls, v: datetime, info):
        return v.fromtimestamp(math.ceil(v.timestamp()), tz=timezone.utc)


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
    
    jobs_mode: Literal['replay', 'custom', 'random', 'test'] = 'random'
    schedule_policy: Literal['fcfs', 'sjf', 'prq'] ='fcfs'
    """"
    Policy to use when scheduling jobs.
    Replay mode will ignore this and use the real time jobs were scheduled unless you also set
    reschedule to true.
    """
    reschedule: bool = False
    """ If true, will apply schedule_policy in replay mode """

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

from typing import Optional, Literal
from datetime import timedelta
from pydantic import BaseModel as PydanticBaseModel, ConfigDict, AwareDatetime, model_validator


class BaseModel(PydanticBaseModel):
    model_config = ConfigDict(
        use_attribute_docstrings = True,
    )


class Job(BaseModel):
    node_count: int
    name: str
    cpu_util: float
    gpu_util: float
    cpu_trace: list[float]
    gpu_trace: list[float]
    wall_time: timedelta
    end_state: str
    # These aren't settable in custom mode, they will be set by the simulator.
    # scheduled_nodes
    # time_start: datetime
    # job_id: Optional[str] = None


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

    jobs: Optional[list[Job]] = None
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


class CoolingSimConfig(BaseModel):
    enabled: bool = False


class SimConfig(BaseModel):
    start: AwareDatetime
    end: AwareDatetime

    scheduler: SchedulerSimConfig = SchedulerSimConfig()
    cooling: CoolingSimConfig = CoolingSimConfig()

    @model_validator(mode='after')
    def validate_model(self):
        if not any(m.enabled for m in [self.scheduler, self.cooling]):
            raise ValueError("Must enable one simulation")
        if self.cooling.enabled and not self.scheduler.enabled:
            raise ValueError("Currently can't run cooling simulation without the scheduler")
        return self


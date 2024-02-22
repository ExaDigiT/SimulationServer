from typing import Optional, Union, TypeVar, Literal, Annotated as A
from datetime import datetime
from pydantic import BaseModel, AwareDatetime, model_validator
import pandas as pd
from raps.config import SC_SHAPE, TOTAL_NODES, DOWN_NODES, MAX_TIME, SEED, FMU_PATH
from raps.cooling import ThermoFluidsModel
from raps.power import PowerManager
from raps.scheduler import Scheduler
from raps.telemetry import Telemetry
from .models import SchedulerSimConfig


def run_simulation(config: SchedulerSimConfig):




# def run_raps(*,
#     start: datetime, end: datetime,
#     jobs_df: Optional[pd.DataFrame],
#     jobs_profile_df: Optional[pd.DataFrame],
#     simulate_cooling: bool,
# ):
#     """ Runs raps and the cooling model """
#     if (jobs_df is None) != (jobs_profile_df is None):
#         raise ValueError("Specify both jobs_df and jobs_profile_df or neither")
#     replay_jobs = (jobs_df is not None)

#     if simulate_cooling:
#         cooling_model = ThermoFluidsModel(FMU_PATH)
#         cooling_model.initialize()
#     else:
#         cooling_model = None

#     power_manager = PowerManager(SC_SHAPE, DOWN_NODES)
#     sc = Scheduler(
#         total_nodes = TOTAL_NODES,
#         down_nodes = DOWN_NODES,
#         power_manager = power_manager,
#         layout_manager = None,
#         cooling_model = cooling_model,
#         debug = False,
#         replay = replay_jobs,
#     )

#     if replay_jobs:
#         td = Telemetry()
#         # Jobs from telemetry data
#         jobs = td.read_data(jobs_df, jobs_profile_df)
#     else:
#         # Randomly generated jobs
#         jobs = sc.generate_random_jobs(num_jobs=args.numjobs)
    
#     timesteps = int((end - start).total_seconds())
#     sim_gen = sc.run_simulation(jobs, timesteps=timesteps)
#     for tick_data in sim_gen:
        



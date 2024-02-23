from typing import Optional, Union, TypeVar, Literal, Annotated as A
from datetime import datetime
from pydantic import BaseModel, AwareDatetime, model_validator
import pandas as pd
from pathlib import Path
import random, math
import numpy as np
from .raps.raps.config import SC_SHAPE, TOTAL_NODES, DOWN_NODES, TRACE_QUANTA, FMU_PATH
from .raps.raps.cooling import ThermoFluidsModel
from .raps.raps.power import PowerManager
from .raps.raps.scheduler import Scheduler
from .raps.raps.telemetry import Telemetry
from ..models.sim import SimConfig

RAPS_PATH = Path(__file__).parent / 'raps'

def run_simulation(config: SimConfig):
    if config.scheduler.enabled:
        if config.scheduler.seed:
            # TODO: This is globabl and should probably be done in RAPS
            random.seed(config.scheduler.seed)
            np.random.seed(config.scheduler.seed)

        # TODO: Is timesteps seconds or 15 second intervals?
        timesteps = math.ceil((config.end - config.start).total_seconds() / TRACE_QUANTA)
        down_nodes = [*DOWN_NODES, *config.scheduler.down_nodes]

        if config.cooling.enabled:
            cooling_model = ThermoFluidsModel(str(RAPS_PATH / FMU_PATH))
            cooling_model.initialize()
        else:
            cooling_model = None

        power_manager = PowerManager(SC_SHAPE, down_nodes)

        sc = Scheduler(
            TOTAL_NODES, down_nodes,
            power_manager = power_manager,
            layout_manager = None,
            cooling_model = cooling_model,
            debug = False,
        )

        if config.scheduler.jobs_mode == "replay":
            raise Exception("Replay not supported yet")
        elif config.scheduler.jobs_mode == "custom":
            raise Exception("Custom not supported")
        elif config.scheduler.jobs_mode == "random":
            num_jobs = config.scheduler.num_jobs if config.scheduler.num_jobs is not None else 1000
            jobs = sc.generate_random_jobs(num_jobs=num_jobs)

        yield from sc.run_simulation(jobs, timesteps=timesteps)
    else:
        raise Exception("No simulations specified")



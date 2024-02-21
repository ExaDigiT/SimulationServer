from typing import Annotated as A, Optional
from fastapi import APIRouter, Body, Depends
from datetime import datetime, timedelta
from ..models.base import ObjectTimeseries, Page
from ..models.output import (
    SchedulerSimJob, SCHEDULER_SIM_JOB_API_FIELDS, SCHEDULER_SIM_JOB_FIELD_SELECTORS,
    SchedulerSimJobFieldSelector, SchedulerSimSystem, CoolingSimCDU, COOLING_CDU_API_FIELDS,
    COOLING_CDU_FIELD_SELECTORS, CoolingSimCDUFieldSelector,
)
from ..models.sim import Sim, SIM_API_FIELDS, SimConfig
from ..util.api_queries import (
    Granularity, granularity_params, filter_params, Filters, sort_params, Sort, 
)
router = APIRouter(prefix="/frontier/simulation", tags=['frontier-simulation'])


GranularityDep = A[Granularity, Depends(granularity_params(default_granularity=timedelta(seconds=1)))]


@router.post("/run", response_model=Sim)
def run(*, sim_config: A[SimConfig, Body()]):
    """
    Start running a simulation in the background. POST the configuration for the simulation. Returns
    a Sim object containing an id you can use to query the results as they are generated.
    """
    return Sim(
        id = "0",
        user = "someone",
        state = "running",
        logical_start = sim_config.start,
        logical_end = sim_config.end,
        run_start = datetime.now(),
        run_end = None,
        progress = 0.0,
        config = sim_config,
    )



SimFilters = A[Filters, Depends(filter_params(SIM_API_FIELDS))]
SimSort = A[Sort, Depends(sort_params(SIM_API_FIELDS, [
    "asc:logical_start", "asc:logical_end", "asc:run_start", "asc:run_end", "asc:id",
]))]

@router.get("/list", response_model=Page[Sim])
def experiment_list(filters: SimFilters, sort: SimSort):
    """
    List all the simulations.
    You can add filters to get simulations by state, user, id, etc.
    """
    return []



@router.get("/{id}", response_model=Sim)
def get(id: str):
    """ Get simulation by id or 404 if not found. """
    return []



CoolingCDUFilters = A[Filters, Depends(filter_params(COOLING_CDU_API_FIELDS))]

@router.get("/{id}/cooling/cdu", response_model=ObjectTimeseries[CoolingSimCDU])
def cooling_cdu(*,
    id: str,
    start: Optional[datetime] = None, end: Optional[datetime] = None, granularity: GranularityDep,
    fields: CoolingSimCDUFieldSelector, filters: CoolingCDUFilters,
):
    return {
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "granularity": 1,
        "data": [],
    }



SchedulerSimJobFilters = A[Filters, Depends(filter_params(SCHEDULER_SIM_JOB_API_FIELDS))]

@router.get("/{id}/scheduler/jobs", response_model=list[SchedulerSimJob])
def scheduler_jobs(*,
    id: str,
    start: Optional[datetime] = None, end: Optional[datetime] = None,
    fields: SchedulerSimJobFieldSelector, filters: SchedulerSimJobFilters,
):
    return []



@router.get("/{id}/scheduler/system", response_model=list[SchedulerSimSystem])
def scheduler_system(*, id: str, start: datetime, end: datetime):
    return []

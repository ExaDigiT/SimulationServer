from typing import Annotated as A, Optional, Literal
from fastapi import APIRouter, Body, Depends, HTTPException
from datetime import datetime, timedelta
from ..models.base import ObjectTimeseries, Page
from ..models.output import (
    SchedulerSimJob, SCHEDULER_SIM_JOB_API_FIELDS, SCHEDULER_SIM_JOB_FIELD_SELECTORS,
    SchedulerSimSystem,
    CoolingSimCDU, COOLING_CDU_API_FIELDS, COOLING_CDU_FIELD_SELECTORS,
)
from ..models.sim import Sim, SIM_API_FIELDS, SimConfig
from .config import AppDeps
from .api_queries import (
    Granularity, granularity_params, filter_params, Filters, sort_params, Sort, get_selectors
)
from .service import run_simulation, query_sims

router = APIRouter(prefix="/frontier/simulation", tags=['frontier-simulation'])


GranularityDep = A[Granularity, Depends(granularity_params(default_granularity=timedelta(seconds=1)))]
SchedulerSimJobFieldSelector = Literal[get_selectors(SCHEDULER_SIM_JOB_FIELD_SELECTORS)]
CoolingSimCDUFieldSelector = Literal[get_selectors(COOLING_CDU_FIELD_SELECTORS)]


@router.post("/run", response_model=Sim)
def run(*, sim_config: A[SimConfig, Body()], deps: AppDeps):
    """
    Start running a simulation in the background. POST the configuration for the simulation. Returns
    a Sim object containing an id you can use to query the results as they are generated.
    """
    return run_simulation(sim_config, deps)

SIM_FILTERS = filter_params(SIM_API_FIELDS)
SimFilters = A[Filters, Depends(SIM_FILTERS)]
SimSort = A[Sort, Depends(sort_params(SIM_API_FIELDS, [
    "asc:run_start", "asc:run_end", "asc:logical_start", "asc:logical_end", "asc:id",
]))]

@router.get("/list", response_model=Page[Sim])
def sim_list(*, filters: SimFilters, sort: SimSort, limit = 100, offset = 0, deps: AppDeps):
    """
    List all the simulations.
    You can add filters to get simulations by state, user, id, etc.
    """
    results = query_sims(
        filters = filters, sort = sort, limit = limit, offset = offset,
        druid_engine = deps.druid_engine,
    )
    return Page(results = results, limit = limit, offset = offset, total_results = len(results))


@router.get("/{id}", response_model=Sim)
def get(id: str, deps: AppDeps):
    """ Get simulation by id or 404 if not found. """
    results = query_sims(
        filters = SIM_FILTERS(id=[f"eq:{id}"]), limit = 1,
        druid_engine = deps.druid_engine,
    )
    if len(results) == 0:
        raise HTTPException(404)
    else:
        return results[0]



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

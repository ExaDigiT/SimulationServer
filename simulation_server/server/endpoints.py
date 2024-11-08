from typing import Annotated as A, Optional, Literal
from fastapi import APIRouter, Query, Body, Depends, HTTPException
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
from ..models.base import ObjectTimeseries, Page, CommaSeparatedList
from ..models.output import (
    SchedulerSimJob, SCHEDULER_SIM_JOB_FILTERS, SCHEDULER_SIM_JOB_SORT, SCHEDULER_SIM_JOB_FIELD_SELECTORS,
    SchedulerSimJobPowerHistory,
    SchedulerSimSystem, SCHEDULER_SIM_JOB_POWER_HISTORY_FIELD_SELECTORS,
    CoolingSimCDU, COOLING_CDU_FILTERS, COOLING_CDU_FIELD_SELECTORS,
    CoolingSimCEP, COOLING_CEP_FIELD_SELECTORS,
)
from ..models.sim import Sim, SIM_FIELD_SELECTORS, SIM_FILTERS, SIM_SORT, SimConfig
from ..models.output import SystemInfo
from ..util.api_queries import Granularity, granularity_params, Filters, Sort, get_selectors
from .config import AppDeps
from .service import (
    run_simulation, query_sims, query_cooling_sim_cdu, query_scheduler_sim_jobs,
    query_scheduler_sim_system, query_scheduler_sim_power_history, query_cooling_sim_cep,
    get_system_info,
)

router = APIRouter(prefix="/frontier", tags=['frontier'])


GranularityDep = A[Granularity, Depends(granularity_params(default_granularity=timedelta(seconds=1)))]


@router.post("/simulation/run", response_model=Sim)
def run(*, sim_config: A[SimConfig, Body()], deps: AppDeps):
    """
    Start running a simulation in the background. POST the configuration for the simulation. Returns
    a Sim object containing an id you can use to query the results as they are generated.
    """
    return run_simulation(sim_config, deps)

SimFilters = A[Filters, Depends(SIM_FILTERS)]
SimSort = A[Sort, Depends(SIM_SORT)]
SimFieldSelector = Literal[get_selectors(SIM_FIELD_SELECTORS)] # type: ignore
SimFieldSelectors = A[CommaSeparatedList[SimFieldSelector], Query()]

@router.get("/simulation/list", response_model=Page[Sim], response_model_exclude_none=True)
def sim_list(*,
    filters: SimFilters, sort: SimSort,
    fields: SimFieldSelectors = None,
    limit: int = 100, offset: int = 0,
    deps: AppDeps,
):
    """
    List all the simulations.
    You can add filters to get simulations by state, user, id, etc.
    """
    results, total_results = query_sims(
        filters = filters, sort = sort, fields = fields, limit = limit, offset = offset,
        druid_engine = deps.druid_engine,
    )
    return Page(results = results, limit = limit, offset = offset, total_results = total_results)


@router.get("/simulation/{id}", response_model=Sim, response_model_exclude_none=True)
def get(id: str, deps: AppDeps):
    """ Get simulation by id or 404 if not found. """
    results, total_results = query_sims(
        filters = SIM_FILTERS(id=[f"eq:{id}"]), fields=['all'], limit = 1,
        druid_engine = deps.druid_engine,
    )
    if len(results) == 0:
        raise HTTPException(404)
    else:
        return results[0]



CoolingCDUFilters = A[Filters, Depends(COOLING_CDU_FILTERS)]
CoolingSimCDUFieldSelector = Literal[get_selectors(COOLING_CDU_FIELD_SELECTORS)] # type: ignore
CoolingSimCDUFieldSelectors = A[CommaSeparatedList[CoolingSimCDUFieldSelector], Query()]

@router.get("/simulation/{id}/cooling/cdu", response_model=ObjectTimeseries[CoolingSimCDU])
def cooling_cdu(*,
    id: str,
    start: Optional[datetime] = None, end: Optional[datetime] = None, granularity: GranularityDep,
    fields: CoolingSimCDUFieldSelectors = None, filters: CoolingCDUFilters, deps: AppDeps,
):
    """
    Query the CDU output from a simulation.
    """
    if start and end and end < start: raise HTTPException(422, "end must be >= start")
    result = query_cooling_sim_cdu(
        id = id, start = start, end = end, granularity = granularity,
        fields = fields, filters = filters, druid_engine = deps.druid_engine,
    )
    return JSONResponse(result)

CoolingSimCEPFieldSelector = Literal[get_selectors(COOLING_CEP_FIELD_SELECTORS)] # type: ignore
CoolingSimCEPFieldSelectors = A[CommaSeparatedList[CoolingSimCEPFieldSelector], Query()]

@router.get("/simulation/{id}/cooling/cep", response_model=ObjectTimeseries[CoolingSimCEP])
def cooling_cep(*,
    id: str, start: Optional[datetime] = None, end: Optional[datetime] = None,
    granularity: A[Granularity, Depends(granularity_params(default_resolution=1))],
    fields: CoolingSimCEPFieldSelectors = None,
    deps: AppDeps,
):
    """
    Returns stats of the simulation.
    By default will return just the most recent stats, but you can pass a granularity to get stats
    at different sampling rates.
    """
    if start and end and end < start: raise HTTPException(422, "end must be >= start")
    result = query_cooling_sim_cep(
        id = id, start = start, end = end, granularity = granularity,
        fields = fields, druid_engine = deps.druid_engine,
    )
    return result


# Don't allow direct filter by time_snapshot to avoid confusion with time_travel
SchedulerSimJobFilters = A[Filters, Depends(SCHEDULER_SIM_JOB_FILTERS)]
SchedulerSimJobSort = A[Sort, Depends(SCHEDULER_SIM_JOB_SORT)]
SchedulerSimJobFieldSelector = Literal[get_selectors(SCHEDULER_SIM_JOB_FIELD_SELECTORS)] # type: ignore
SchedulerSimJobFieldSelectors = A[CommaSeparatedList[SchedulerSimJobFieldSelector], Query()]

@router.get("/simulation/{id}/scheduler/jobs", response_model=Page[SchedulerSimJob])
def scheduler_jobs(*,
    id: str, start: Optional[datetime] = None, end: Optional[datetime] = None,
    time_travel: Optional[datetime] = None, limit: int = 100, offset: int = 0,
    fields: SchedulerSimJobFieldSelectors = None, filters: SchedulerSimJobFilters,
    sort:SchedulerSimJobSort,
    deps: AppDeps,
):
    """
    Query the simulated jobs.

    You can select a subset of the fields to return, and filter by most attributes.

    The optional `start` and `end` parameters select all jobs that were queued or running over the
    interval (including jobs that only partially overlap the interval). This is based on
    `time_submission` -> `time_end`.

    By default the endpoint will return the latest state of the job available, e.g. only jobs that
    are currently running in the simulation will return state=RUNNING. You can use the `time_travel`
    argument to scan back and see what the jobs looked like at a particular point in history.

    Examples:
    - Show the jobs currently running in the simulation
        - `?start=[now]&state=eq:RUNNING`
        - You could can omit `start` filter, but adding start will be faster as the table is indexed
          by time.
    - Show the jobs currently queued in the simulation
        - `?start=[now]&state=eq:PENDING`
    - Show jobs that were running at some point in time
        - `?time_travel=2024-01-01T00:00Z&state=eq:RUNNING`
    - Show only completed jobs
        - `?state=eq:COMPLETE`
    - Show all jobs run by a user
        - `?user=hinesjr`
    - Show all jobs that ran over a given interval
        - `?start=2024-03-01T00:00:00Z&end=2024-03-01T01:00:00Z`
    """
    if start and end and end < start: raise HTTPException(422, "end must be >= start")
    results, total_results = query_scheduler_sim_jobs(
        id = id, start = start, end = end,
        time_travel = time_travel, limit = limit, offset = offset,
        fields = fields, filters = filters, sort = sort,
        druid_engine = deps.druid_engine,
    )
    return JSONResponse({
        "results": results,
        "limit": limit,
        "offset": offset,
        "total_results": total_results,
    })



SchedulerSimPowerHistoryFieldSelector = Literal[get_selectors(SCHEDULER_SIM_JOB_POWER_HISTORY_FIELD_SELECTORS)] # type: ignore
SchedulerSimPowerHistoryFieldSelectors = A[CommaSeparatedList[SchedulerSimPowerHistoryFieldSelector], Query()]

@router.get("/simulation/{id}/scheduler/jobs/{job_id}/power-history", response_model=ObjectTimeseries[SchedulerSimJobPowerHistory])
def power_history(*,
    id: str, job_id: str,
    start: Optional[datetime] = None, end: Optional[datetime] = None, granularity: GranularityDep,
    fields: SchedulerSimPowerHistoryFieldSelectors = None, deps: AppDeps,
):
    """
    Query power history of a single job
    """
    if start and end and end < start: raise HTTPException(422, "end must be >= start")
    result = query_scheduler_sim_power_history(
        id = id, job_id = job_id, start = start, end = end, granularity = granularity,
        fields = fields, druid_engine = deps.druid_engine,
    )
    return JSONResponse(result)


@router.get("/simulation/{id}/scheduler/system", response_model=ObjectTimeseries[SchedulerSimSystem])
def scheduler_system(*,
    id: str, start: Optional[datetime] = None, end: Optional[datetime] = None,
    granularity: A[Granularity, Depends(granularity_params(default_resolution=1))],
    deps: AppDeps,
):
    """
    Returns stats of the simulation.
    By default will return just the most recent stats, but you can pass a granularity to get stats
    at different sampling rates.
    """
    if start and end and end < start: raise HTTPException(422, "end must be >= start")
    result = query_scheduler_sim_system(
        id = id, start = start, end = end, granularity = granularity,
        druid_engine = deps.druid_engine,
    )
    return result


@router.get("/system-info", response_model=SystemInfo)
def system_info():
    return get_system_info()

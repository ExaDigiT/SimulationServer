from typing import Annotated as A, Optional, Literal
from fastapi import APIRouter, Body, Depends, HTTPException
from datetime import datetime, timedelta, timezone
import uuid, time
import sqlalchemy as sqla
from loguru import logger
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
from ..util.k8s import submit_job, get_job
from ..util.druid import get_table


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
    sim = Sim(
        id = str(uuid.uuid4()),
        user = "unknown", # TODO pull this from cookie/auth header
        state = "running",
        logical_start = sim_config.start,
        logical_end = sim_config.end,
        run_start = datetime.now(timezone.utc),
        run_end = None,
        progress = 0,
        config = sim_config.model_dump(mode = 'json'),
    )
    deps.kafka_producer.send("svc-event-exadigit-sim", value = sim.serialize_for_druid())

    submit_job({
        "metadata": {
            "name": f"exadigit-simulation-server-{sim.id}",
            "labels": {"app": "exadigit-simulation-server"},
        },
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "main",
                            "image": deps.settings.job_image,
                            "command": ['python3', "-m", "simulation_server.simulation.main", "background-job"],
                            "env": [
                                {"name": "SIM", "value": sim.model_dump_json()},
                            ],
                            "envFrom": [
                                {"secretRef": {'name': 'prod-infra-envconfig-service-sens-creds'}},
                            ],
                            "resources": {
                                "requests": {"cpu": "1000m", "memory": "512Mi"},
                                "limits": {"cpu": "4000m", "memory": "3Gi"},
                            },
                        }
                    ],
                    "restartPolicy": "Never",
                }
            },
            "backoffLimit": 0, # Don't retry on failure
        }
    })

    # Block until row actually shows up in druid
    # When we upgrade druid we could use a SQL INSERT instead
    # Perhaps we should move this table out of Druid and into our Postgres instance and just use
    # Druid for the large timeseries tables
    sim_table = get_table("svc-event-exadigit-sim", deps.druid_engine)
    stmt = sqla.select(sim_table.c.id).where(sim_table.c.id == sim.id)
    TIME_OUT = 60
    sim_saved = False
    with deps.druid_engine.connect() as conn:
        start = time.time()
        sim_saved = bool(conn.execute(stmt).scalar())
        while (time.time() - start) < TIME_OUT and not sim_saved:
            time.sleep(0.1)
            sim_saved = bool(conn.execute(stmt).scalar())

    if not sim_saved:
        logger.error(f"Timeout while waiting for sim {sim.id} to be saved to druid")
        raise HTTPException(500)

    return sim



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

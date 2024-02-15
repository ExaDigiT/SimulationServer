from contextlib import asynccontextmanager
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from loguru import logger

from .config import AppSettings

settings = AppSettings()


@asynccontextmanager
async def lifespan(api: FastAPI):
    # Force initializing deps on startup so database connection errors etc. show up immediately
    deps = [] # TODO: Add Druid DB
    for dep in deps:
        api.dependency_overrides.get(dep, dep)()
    yield


app = FastAPI(
    title = "LVA Backend",
    version = "0.1.0",
    # Simplify ids and names in generated clients a bit
    # NOTE: This means we need one tag defined (or inherited from the APIRouter object) on every route
    generate_unique_id_function = lambda route: f"{route.tags[0]}_{route.name}",

    # FastAPI/Pydantic 2 will create two schemas if a model has defaults, since the input to the server
    # doesn't need the fields, but the output will always have them populated. Pydantic 1 didn't make
    # this distinction however, so I'm reverting it here so we don't have to adjust clientside code.
    # TODO: Eventually we should update the clientside code to use the separate, more accurate, schemas.
    separate_input_output_schemas = False,

    root_path = settings.root_path,
    lifespan = lifespan,
    debug = settings.debug_mode,
)


@app.exception_handler(404)
async def http_404_handler(request: Request, exc: HTTPException) -> Response:
    message = {
        "detail": exc.detail,
        "openapi.json": str(request.url_for('openapi')),
        "docs": str(request.url_for("swagger_ui_html")),
    }
    return JSONResponse(message, status_code=404, headers=exc.headers)

app.add_middleware(GZipMiddleware, compresslevel = 5)


from .endpoints import simulator
app.include_router(simulator.router)

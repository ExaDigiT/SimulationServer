""" A simple REST API for triggering and querying the results from the digital twin """

import subprocess, asyncio, functools
from contextlib import asynccontextmanager
from starlette.exceptions import HTTPException
from starlette.requests import Request
from starlette.responses import Response, JSONResponse
from starlette.concurrency import run_in_threadpool
from fastapi import FastAPI
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
import uvicorn
from ..util.json import update_jsonpaths
from .service import cleanup_jobs
from .config import AppSettings, get_app_settings, get_druid_engine, get_kafka_producer

settings = AppSettings()


def repeat_task(func, seconds):
    if not asyncio.iscoroutinefunction(func):
        func = functools.partial(run_in_threadpool, func)

    async def loop() -> None:
        while True:
            await func()
            await asyncio.sleep(seconds)

    return asyncio.create_task(loop())


@asynccontextmanager
async def lifespan(api: FastAPI):
    # Force initializing deps on startup so database connection errors etc. show up immediately
    deps = [get_app_settings, get_druid_engine, get_kafka_producer]
    for dep in deps:
        api.dependency_overrides.get(dep, dep)()

    background_task_loop = repeat_task(
        lambda: cleanup_jobs(druid_engine = get_druid_engine(), kafka_producer = get_kafka_producer()),
        seconds = 5 * 60,
    )

    yield

    background_task_loop.cancel()


app = FastAPI(
    title = "ExaDigiT Simulation Server",
    version = "0.1.0",
    # Simplify ids and names in generated clients a bit
    # NOTE: This means we need one tag defined (or inherited from the APIRouter object) on every route
    generate_unique_id_function = lambda route: f"{route.tags[0]}_{route.name}",
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


def custom_openapi():
    if not app.openapi_schema:
        schema = get_openapi(
            openapi_version = "3.1.0",
            title=app.title, version=app.version,
            description=app.description, routes=app.routes, servers=app.servers,
        )

        # Pydantic 2 `anyOf: [{type: "foo"}, type: "null"]` causes issues in the generated docs
        # See https://github.com/pydantic/pydantic/issues/6647
        def replace_null_unions(match):
            types = [
                i for i in match.value['anyOf']
                if not (isinstance(i, dict) and i.get('type') == 'null')
            ]
            if len(types) == 1:
                value = {
                    **match.value,
                    **types[0],
                }
                value.pop("anyOf")
                return value
            else:
                return {
                    **match.value,
                    "anyOf": types,
                }

        schema = update_jsonpaths(schema, {"$..* where anyOf": replace_null_unions})

        app.openapi_schema = schema

    return app.openapi_schema
app.openapi = custom_openapi


app.add_middleware(GZipMiddleware, compresslevel = 5)
if settings.allow_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.allow_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
)

from .endpoints import router
app.include_router(router)


if __name__ == "__main__":
    if settings.debug_mode:
        uvicorn.run(app,
            host='localhost',
            port=settings.http_port,
            reload=False,
        )
    else:
        subprocess.run(["gunicorn",
            "simulation_server.server.main:app",
            "--bind", f"0.0.0.0:{settings.http_port}",
            "--worker-class", "uvicorn.workers.UvicornWorker",
        ], check=True)

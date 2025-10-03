""" A script to run the ExaDigiT simulation """
from collections.abc import Iterable
import argparse, os, orjson
from pathlib import Path
from datetime import datetime, timezone
from loguru import logger
import yaml
from ..models.sim import Sim, ServerSimConfig
from .simulation import run_simulation
from ..util.kafka import get_kafka_producer


def run_simulation_serialized(sim: Sim) -> Iterable[dict[str, list[bytes]]]:
    sim = sim.model_copy()

    def serialize_rows(rows):
        return [
            orjson.dumps({"sim_id": sim.id, **row.model_dump(mode='json')})
            for row in rows
        ]

    logger.info(f"Starting simulation: {sim.model_dump_json(indent = 4)}")
    config = ServerSimConfig.model_validate(sim.config)
    progress_date = sim.start

    try:
        for data in run_simulation(config):
            yield {
                "svc-ts-exadigit-schedulersimsystem": serialize_rows(data.scheduler_sim_system),
                "svc-event-exadigit-schedulersimjob": serialize_rows(data.scheduler_sim_jobs),
                "svc-ts-exadigit-coolingsimcdu": serialize_rows(data.cooling_sim_cdus),
                "svc-ts-exadigit-coolingsimcep": serialize_rows(data.cooling_sim_cep),
                "svc-ts-exadigit-jobpowerhistory": serialize_rows(data.power_history),
            }
            progress_date = data.timestamp
            if data.timestamp.second == 0:
                logger.info(f"progress: {data.timestamp.isoformat()} / {sim.end.isoformat()}")
    except BaseException as e:
        sim.state = "fail"
        sim.execution_end = datetime.now(timezone.utc)
        sim.error_messages = str(e)
        sim.progress_date = progress_date
        yield {"svc-event-exadigit-sim": [sim.serialize_for_druid()]}
        logger.info(f"Simulation {sim.id} failed")
        raise e
    
    sim.state = "success"
    sim.execution_end = datetime.now(timezone.utc)
    sim.progress_date = sim.end
    yield {"svc-event-exadigit-sim": [sim.serialize_for_druid()]}
    logger.info(f"Simulation {sim.id} finished")


def write_sim_to_kafka(sim: Sim):
    kafka_producer = get_kafka_producer(
        linger_ms = 2 * 1000,
        batch_size = 65536,
        compression_type = "snappy",
    )
    try:
        for data in run_simulation_serialized(sim):
            # kafka_producer does its own buffering of output so we don't need to worry about batching
            for topic, rows in data.items():
                for row in rows:
                    kafka_producer.send(topic=topic, value=row)
    finally:
        kafka_producer.close()


def write_sim_to_disk(sim: Sim, dest: str):
    Path(dest).mkdir(exist_ok=True)
    files = {}
    try:
        for data in run_simulation_serialized(sim):
            for topic, rows in data.items():
                if topic not in files:
                    files[topic] = open(Path(dest) / f"{topic}.jsonl", 'ab')
                files[topic].writelines(l + b"\n" for l in rows)
    finally:
        for file in files.values():
            file.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = __doc__.strip(),
        allow_abbrev = False,
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--sim", type=str, help="Sim json")
    parser.add_argument("--dest", default=None)

    args = parser.parse_args()

    if args.sim:
        sim = args.sim
    elif os.environ.get("SIM"):
        sim = os.environ["SIM"]
    else:
        raise Exception("No configuration passed")

    sim = Sim.model_validate(yaml.safe_load(sim))

    if args.dest:
        write_sim_to_disk(sim, args.dest)
    else:
        write_sim_to_kafka(sim)

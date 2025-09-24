""" A script to run the ExaDigiT simulation """
from typing import Callable
import argparse, os, json
from pathlib import Path
from datetime import datetime, timezone
from loguru import logger
import yaml
from ..models.sim import Sim, ServerSimConfig
from .simulation import run_simulation
from ..util.kafka import get_kafka_producer


def write_sim(sim: Sim, writer: Callable[[str, bytes], None]):
    sim = sim.model_copy()

    def output_rows(topic, rows):
        for row in rows:
            value = json.dumps({"sim_id": sim.id, **row.model_dump(mode='json')}).encode()
            writer(topic, value)

    logger.info(f"Starting simulation {sim.model_dump_json()}")
    config = ServerSimConfig.model_validate(sim.config)
    progress_date = sim.start

    try:
        for data in run_simulation(config):
            output_rows("svc-ts-exadigit-schedulersimsystem", data.scheduler_sim_system)
            output_rows("svc-event-exadigit-schedulersimjob", data.scheduler_sim_jobs)
            output_rows("svc-ts-exadigit-coolingsimcdu", data.cooling_sim_cdus)
            output_rows("svc-ts-exadigit-coolingsimcep", data.cooling_sim_cep)
            output_rows("svc-ts-exadigit-jobpowerhistory", data.power_history)
            progress_date = data.timestamp
            if data.timestamp.second == 0:
                logger.info(f"progress: {data.timestamp.isoformat()} / {sim.end.isoformat()}")
    except BaseException as e:
        sim.state = "fail"
        sim.execution_end = datetime.now(timezone.utc)
        sim.error_messages = str(e)
        sim.progress_date = progress_date
        writer("svc-event-exadigit-sim", sim.serialize_for_druid())
        logger.info(f"Simulation {sim.id} failed")
        raise e
    
    sim.state = "success"
    sim.execution_end = datetime.now(timezone.utc)
    sim.progress_date = sim.end
    writer("svc-event-exadigit-sim", sim.serialize_for_druid())
    logger.info(f"Simulation {sim.id} finished")


def write_sim_to_kafka(sim: Sim):
    kafka_producer = get_kafka_producer()
    def writer(topic: str, value: bytes):
        kafka_producer.send(topic=topic, value=value)
    try:
        write_sim(sim, writer=writer)
    finally:
        kafka_producer.close()


def write_sim_to_disk(sim: Sim, dest: str):
    Path(dest).mkdir(exist_ok=True)
    def writer(topic: str, value: bytes):
        with open(Path(dest) / f"{topic}.jsonl", 'ab') as f:
            f.write(value + b"\n")
    write_sim(sim, writer=writer)


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

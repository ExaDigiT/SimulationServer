""" A script to run the ExaDigiT simulation """
import argparse, os, json
from pathlib import Path
from datetime import datetime, timezone
from loguru import logger
import yaml
from ..models.sim import Sim, SimConfig
from .simulation import run_simulation
from ..util.kafka import get_kafka_producer


def cli_run(config: SimConfig):
    for data in run_simulation(config):
        print("TICK")


def background_job(sim: Sim):
    sim = sim.model_copy()
    kafka_producer = get_kafka_producer()

    def output_rows(topic, rows):
        for row in rows:
            value = json.dumps({"sim_id": sim.id, **row.model_dump(mode='json')}).encode()
            kafka_producer.send(topic=topic, value=value)

    logger.info(f"Starting simulation {sim.model_dump_json()}")
    config = SimConfig.model_validate(sim.config)

    try:
        for data in run_simulation(config):
            output_rows("svc-ts-exadigit-schedulersimsystem", data.scheduler_sim_system)
            output_rows("svc-event-exadigit-schedulersimjob", data.scheduler_sim_jobs)
            output_rows("svc-ts-exadigit-coolingsimcdu", data.cooling_sim_cdus)
    except BaseException as e:
        sim.state = "fail"
        sim.run_end = datetime.now(timezone.utc)
        kafka_producer.send("svc-event-exadigit-sim", value = sim.serialize_for_druid())
        logger.info(f"Simulation {sim.id} failed")
        raise e
    
    sim.state = "success"
    sim.run_end = datetime.now(timezone.utc)
    kafka_producer.send(topic = "svc-event-exadigit-sim", value = sim.serialize_for_druid())
    kafka_producer.close() # Close and wait for messages to be sent
    logger.info(f"Simulation {sim.id} finished")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = __doc__.strip(),
        allow_abbrev = False,
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )
    subparsers = parser.add_subparsers(required=True, dest="action")

    parser_cli_run = subparsers.add_parser('run')
    parser_cli_run.add_argument("--config", type=str, help="JSON config string")
    parser_cli_run.add_argument("--config-file", type=Path, help="Path to a yaml or json file contain the config")

    parser_cli_run = subparsers.add_parser('background-job')
    parser_cli_run.add_argument("--sim", type=str, help="JSON config string")

    args = parser.parse_args()


    if args.action == "run":
        if args.config and args.config_file:
            raise Exception("You can only specify either config or config-file")
        
        if args.config:
            config = yaml.safe_load(args.config)
        elif args.config_file:
            config = yaml.safe_load(args.config_file.read_text())
        elif "SIM_CONFIG" in os.environ:
            config = yaml.safe_load(os.environ["SIM_CONFIG"])
        elif "SIM_CONFIG_FILE" in os.environ:
            config = yaml.safe_load(Path(os.environ["SIM_CONFIG_FILE"]).read_text())
        else:
            raise Exception("No configuration passed")
        config = SimConfig.model_validate(config)
        cli_run(config)
    elif args.action == "background-job":
        if args.sim:
            sim = yaml.safe_load(args.sim)
        elif "SIM" in os.environ:
            sim = yaml.safe_load(os.environ["SIM"])
        else:
            raise Exception("No sim passed")
        sim = Sim.model_validate(sim)
        background_job(sim)

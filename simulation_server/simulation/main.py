""" A script to run the ExaDigiT simulation """
import argparse, os
from datetime import timedelta
from pathlib import Path
from loguru import logger
import yaml
from ..models.sim import SimConfig
from ..models.output import JobStateEnum, SchedulerSimJob, SchedulerSimSystem, CoolingSimCDU
from .simulation import run_simulation


def offset_to_time(start, offset):
    if offset is not None:
        return start + timedelta(seconds=offset)
    else:
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = __doc__.strip(),
        allow_abbrev = False,
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--config", type=str, help="JSON config string")
    parser.add_argument("--config-path", type=Path, help="Path to a yaml or json file contain the config")
    args = parser.parse_args()

    if args.config and args.config_path:
        raise Exception("You can only specify either config or config-path")
    
    config = None
    if args.config:
        config = yaml.safe_load(args.config)
    elif args.config_path:
        config = yaml.safe_load(args.config_path.read_text())
    elif "SIM_CONFIG" in os.environ:
        config = yaml.safe_load(os.environ["SIM_CONFIG"])
    elif "SIM_CONFIG_FILE" in os.environ:
        config = yaml.safe_load(Path(os.environ["SIM_CONFIG_FILE"]).read_text())
    else:
        raise Exception("No configuration passed")

    config = SimConfig.model_validate(config)

    for data in run_simulation(config):
        timestamp = offset_to_time(config.start, data.current_time)
        print("HERE", timestamp)

        down_nodes = data.down_nodes # Convert to xnames
        scheduler_sim_system = [SchedulerSimSystem(
            timestamp= timestamp,
            down_nodes = [] # TODO
        )]
        
        scheduler_sim_jobs: list[SchedulerSimJob] = []
        for job in data.jobs:
            # end_time is set to its planned end once its scheduled. Set it to None for unfinished jobs here
            time_end = offset_to_time(config.start, job.end_time) if job.start_time else None

            scheduler_sim_jobs.append(SchedulerSimJob.model_validate(dict(
                job_id = str(job.id) if job.id else None,
                name = job.name,
                node_count = job.nodes_required,
                time_snapshot = timestamp,
                time_submission = offset_to_time(config.start, job.submit_time),
                time_limit = job.wall_time,
                time_start = offset_to_time(config.start, job.start_time),
                time_end = offset_to_time(config.start, job.end_time),
                state_current = JobStateEnum(job.state.name),
                node_ranges = None, # TODO
                xnames = [], # TODO
                cpu_util = job.cpu_util,
                gpu_util = job.gpu_util,
                # I'm not sure if we actually need these? I think they are constant throughout a job
                cpu_trace = job.cpu_trace,
                gpu_trace = job.gpu_trace,
            )))

        cooling_sim_cdus: list[CoolingSimCDU] = []
        if data.cooling_df:
            pass # TODO

        for job in scheduler_sim_jobs:
            print()
            print(repr(job))

""" A script to run the ExaDigiT simulation """
import argparse, os
from pathlib import Path
from loguru import logger
import yaml
from ..models.sim import SimConfig
from .simulation import run_simulation

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
    print(config)

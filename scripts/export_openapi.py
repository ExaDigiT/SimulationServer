#!/usr/bin/env python3
"""
Export the openapi.json file from a FastAPI application.
"""
import sys, json, argparse
import importlib.util
from pathlib import Path
import fastapi


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = __doc__.strip(),
        allow_abbrev = False,
        formatter_class = argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument('module',
        help = 'Python module containing the FastAPI, e.g. simulation_server.server.main',
    )
    parser.add_argument('var',
        help = 'Name of FastAPI app within the module, or a function that returns a FastAPI app',
    )
    parser.add_argument('--out', default = "openapi.json", help = 'Path to output the json.')
    args = parser.parse_args()

    module_import_path = args.module
    var = args.var
    out = Path(args.out).resolve()

    module = importlib.import_module(module_import_path)

    if not hasattr(module, var):
        print(f"{module_import_path} has no attribute named {var}")
        sys.exit(1)
    
    app = getattr(module, var)

    if type(app) != fastapi.FastAPI:
        if callable(app):
            app = app()
            if type(app) != fastapi.FastAPI:
                print(f"{module_import_path}:{var}() does not return a FastAPI instance")
                sys.exit(1)
        else:
            print(f"{module_import_path}:{var} is not a FastAPI instance")
            sys.exit(1)

    openapi = app.openapi()
    out.parent.mkdir(exist_ok = True, parents = True)
    out.write_text(json.dumps(openapi, indent="  "))

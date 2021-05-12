from typing import List, Dict, Tuple, Iterable

import jsonschema
import os
import click

from .__version__ import __version__
from jetavator.App import App
from .config import AppConfig


def validate_assignment_expressions(expressions: Iterable[str]) -> List[Tuple[str, str]]:
    assignment_tuples = [tuple(expression.split("=", 1)) for expression in expressions]

    if not all(len(x) == 2 for x in assignment_tuples):
        raise click.BadParameter("format must be 'x=y'")

    return [(x[0], x[1]) for x in assignment_tuples]


def assignments_to_dict(
    context: click.core.Context, parameter: click.core.Option, expressions: Tuple[str]
) -> Dict[str, str]:
    return dict(validate_assignment_expressions(expressions))


def multiple_assignments_to_dict(
    context: click.core.Context, parameter: click.core.Option, expressions: Tuple[str]
) -> Dict[str, List[str]]:
    assignment_dict = {}
    for key, value in validate_assignment_expressions(expressions):
        assignment_dict.setdefault(key, []).append(value)
    return assignment_dict


def create_app_object() -> App:

    try:
        AppConfig.make_config_dir()
        app_config = AppConfig.from_yaml_file(AppConfig.config_file())
    except jsonschema.exceptions.ValidationError:
        click.echo("")
        app_config = AppConfig({})

    app_config.reset_session()
    return App(app_config)


@click.group()
def cli():
    pass


@cli.command()
def version():
    """Display the application version."""
    click.echo(__version__)


@cli.command()
@click.option("--config-file", type=click.Path(exists=True, dir_okay=False), help="YAML config file.")
@click.option(
    "--set",
    "set_values",
    multiple=True,
    callback=assignments_to_dict,
    help="Provide options in the form --set <option>=<value>.",
)
def config(config_file: str, set_values: Dict[str, str]):
    """Configure the application's settings."""
    if config_file:
        app_config = AppConfig.from_yaml_file(config_file)
    else:
        try:
            AppConfig.make_config_dir()
            app_config = AppConfig.from_yaml_file(AppConfig.config_file())
        except jsonschema.exceptions.ValidationError:
            raise click.ClickException("Stored configuration is invalid. Please overwrite it using --config-file.")
    app_config.update(set_values)
    app_config.save()


@cli.command()
@click.option("-d", "--drop-if-exists", is_flag=True, help="Drop the current database schema if it exists.")
def deploy(drop_if_exists: bool):
    """Deploy the current project to a fresh database"""
    app = create_app_object()
    app.config.drop_schema_if_exists = drop_if_exists
    app.deploy()


@cli.command()
def update():
    """Update the currently deployed project to a new version"""
    app = create_app_object()
    app.update()


@cli.command()
@click.option(
    "--csv",
    multiple=True,
    callback=multiple_assignments_to_dict,
    help="Provide CSV file paths in the form --csv <source>=<path-to-csv>.",
)
@click.option(
    "--folder",
    type=click.Path(exists=True),
    help=""
)
def run(csv: Dict[str, List[str]], folder: str):
    """Runs the pipeline(s) for the current project with the specified data files"""

    app = create_app_object()

    if csv:
        for table_name, csv_files in csv.items():
            click.echo(f"Loading {len(csv_files)} CSVs " f"into table: {table_name}")
            app.loaded_project.sources[table_name].load_csvs(csv_files)

    if folder:
        for dir_entry in os.scandir(folder):
            filename, file_extension = os.path.splitext(dir_entry.name)
            if file_extension == ".csv" and dir_entry.is_file():
                app.loaded_project.sources[filename].load_csvs([dir_entry.path])

    app.run()

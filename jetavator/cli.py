"""
jetavator

Usage:
  jetavator config [--test] [--config-file=<config_file>]
    [--set <option>=<value>]...
  jetavator deploy [-d|--drop-if-exists] [--set <option>=<value>]...
  jetavator update [--set <option>=<value>]...
  jetavator run [delta|full] [--csv <target_table>=<source_csv>]...
  jetavator run [delta|full] --folder=<csv_folder>
  jetavator drop satellite <name> [--set <option>=<value>]...
  jetavator show project [name|version|history]
    [--set <option>=<value>]...
    [--pivot=[rows|duration]] [--outfile=<csv_file>] [--no-print]
  jetavator -h | --help
  jetavator --version

Options:

  -d --drop-if-exists   Drop and recreate the SQL database if it already exists
                        (use with caution!)

  -n --nodeploy         Run tests only, using an existing deployment.

  --behave=BEHAVE_OPTS  Options to pass to the behave tests
                        (e.g. --behave='--wip --no-summary')

  -h --help             Show this screen.
  --version             Show version.

Help:
  For help using this tool, please open an issue on the Github repository:
  https://github.com/jetavator/jetavator
"""

import traceback
import jsonschema
import os

from docopt import docopt
from textwrap import indent

from . import __version__ as version
from .default_logger import default_logger
from jetavator.App import App
from jetavator import LoadType
from .config import AppConfig


def main(argv=None, exit_callback=None):
    """Main CLI entrypoint."""

    docopt_kwargs = {'version': version}

    if argv:
        # Simulating CLI entry point call from Python
        docopt_kwargs['argv'] = argv

    options = {}

    try:
        options = docopt(__doc__, **docopt_kwargs)
    except SystemExit as e:
        default_logger.error(str(e))
        if exit_callback:
            exit_callback(1)
            return
        else:
            exit(1)

    cli_config_values = dict(
        tuple(option_string.split("=", 1))
        for option_string in options["<option>=<value>"]
    )

    # TODO: replace with simpler --file?
    if options.get("--config-file"):
        config = AppConfig.from_yaml_file(options["--config-file"])
        config.update(cli_config_values)
    else:
        try:
            AppConfig.make_config_dir()
            config = AppConfig.from_yaml_file(AppConfig.config_file())
            config.update(cli_config_values)
        except jsonschema.exceptions.ValidationError:
            config = AppConfig(cli_config_values)

    config.reset_session()
    app = App(config)

    printable_options = [
        k
        for k, v in options.items()
        if v is True
    ] + [
        f'{k}={v}'
        for k, v in options.items()
        if v and v is not True and not k == '--password'
    ]

    default_logger.info(
        f'''
        Jetavator {version} - running command:
        {' '.join(printable_options)}

        Jetavator config:
{indent(str(app.config), '        ')}
        '''
    )

    if options.get('-d') or options.get('--drop-if-exists'):
        app.config.drop_schema_if_exists = True

    if options.get('-n') or options.get('--nodeploy'):
        app.config.skip_deploy = True

    if options.get('--behave'):
        app.config.behave_options = options.get('--behave')

    try:

        # TODO: refactor this if block into separate functions

        if options['config']:
            if options['--test']:
                test_app = App(config=config)
                if test_app.engine.compute_service.test():
                    default_logger.info(
                        'Successfully logged in and connected to compute service.'
                    )
                    config.save()
                else:
                    default_logger.error(
                        'Unable to log in or connect to compute service.'
                    )
            else:
                config.save()

        elif options['deploy']:
            app.deploy()

        elif options['update']:
            app.update()

        elif options['drop'] and options['satellite']:
            app.drop('satellite', options['<name>'])

        elif options['run']:
            load_type = (LoadType.FULL if options['full'] else LoadType.DELTA)
            default_logger.info(f'Engine: Performing {load_type} load.')
            # TODO: Refactor this out of cli.py to elsewhere - this is core functionality
            if options['<target_table>=<source_csv>']:
                table_csvs = {}
                for option in options['<target_table>=<source_csv>']:
                    table_name, csv_file = option.split('=')
                    table_csvs.setdefault(table_name, []).append(csv_file)
                for table_name, csv_files in table_csvs.items():
                    default_logger.info(
                        f'Loading {len(csv_files)} CSVs '
                        f'into table: {table_name}'
                    )
                    app.loaded_project.sources[table_name].load_csvs(csv_files)
            elif options['--folder']:
                for dir_entry in os.scandir(options['--folder']):
                    filename, file_extension = os.path.splitext(dir_entry.name)
                    if file_extension == ".csv" and dir_entry.is_file():
                        app.loaded_project.sources[filename].load_csvs([dir_entry.path])
            app.run(load_type=load_type)

    except RuntimeError:
        default_logger.error(traceback.format_exc())
        if exit_callback:
            exit_callback(1)
        else:
            exit(1)


if __name__ == '__main__':
    main()

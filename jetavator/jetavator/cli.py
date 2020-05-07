"""
jetavator

Usage:
  jetavator config [--test] [--config-file=<config_file>]
    [--set <option>=<value>]...
  jetavator build
  jetavator deploy [-d|--drop-if-exists] [--set <option>=<value>]...
  jetavator update [--set <option>=<value>]...
  jetavator run [delta|full] [--csv <target_table>=<source_csv>]...
  jetavator run [delta|full] --folder=<csv_folder>
  jetavator drop satellite <name> [--set <option>=<value>]...
  jetavator performance [--set <option>=<value>]...
    [--pivot=[rows|duration]] [--outfile=<csv_file>] [--no-print]
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
import logging

from docopt import docopt
from tabulate import tabulate
from textwrap import indent

from . import __version__ as VERSION
from .default_logger import default_logger
from .Engine import Engine
from .config import FileConfig, CommandLineConfig


def main(argv=None, exit_callback=None):
    """Main CLI entrypoint."""

    docopt_kwargs = {'version': VERSION}

    if argv:
        # Simulating CLI entry point call from Python
        docopt_kwargs['argv'] = argv

    try:
        options = docopt(__doc__, **docopt_kwargs)
    except SystemExit as e:
        default_logger.error(str(e))
        if exit_callback:
            exit_callback(1)
            return
        else:
            exit(1)

    try:
        config = FileConfig.load(
            CommandLineConfig(options)
        )
    except jsonschema.exceptions.ValidationError:
        config = CommandLineConfig(options)

    config.reset_session()
    engine = Engine(config)

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
        Jetavator {VERSION} - running command:
        {' '.join(printable_options)}

        Jetavator config:
{indent(str(engine.config), '        ')}
        '''
    )

    if options.get('-d') or options.get('--drop-if-exists'):
        engine.config.drop_schema_if_exists = True

    if options.get('-n') or options.get('--nodeploy'):
        engine.config.skip_deploy = True

    if options.get('--behave'):
        engine.config.behave_options = options.get('--behave')

    try:

        # TODO: allow an corrupt or invalid config to be overwritten
        #       using this command
        if options['config']:
            FileConfig.command_line_options_to_keyring(options)
            test_config = FileConfig.load()
            if options['--test']:
                test_engine = Engine(config=test_config)
                if test_engine.connection.test(master=True):
                    default_logger.info(
                        'Successfully logged in and connected to '
                        f'[{engine.config.environment_type}]'
                    )
                else:
                    default_logger.error(
                        'Unable to log in or connect to '
                        f'[{engine.config.environment_type}]'
                    )

        elif options['build']:
            engine.build_wheel()

        elif options['deploy']:
            engine.deploy()

        elif options['update']:
            engine.update()

        elif options['drop'] and options['satellite']:
            engine.drop('satellite', options['<name>'])

        elif options['run']:
            load_type=('full' if options['full'] else 'delta')
            default_logger.info(f'Engine: Performing {load_type} load.')
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
                    engine.load_csvs(table_name, csv_files)
            if options['--folder']:
                engine.load_csv_folder(folder_path=options['--folder'])
            engine.run(load_type='full')

        elif options['performance']:
            df = engine.get_performance_data()

            if options['--pivot']:
                default_logger.info(
                    f'Displaying report for: {options["--pivot"]}'
                )
                df = df.pivot_table(
                    index=['name'],
                    columns='stage',
                    values=options['--pivot'],
                    aggfunc='sum',
                    margins=True
                )
            if options['--outfile']:
                df.to_csv(options['--outfile'])

            if not options['--no-print']:
                default_logger.info(
                    tabulate(df, headers='keys', tablefmt='grid')
                )

        elif options['show']:
            if options['project']:
                if options['name']:
                    default_logger.info(engine.project.name)
                elif options['version']:
                    default_logger.info(engine.project.version)
                elif options['history']:
                    version_history = [
                        (
                            version.name,
                            version.version,
                            version.deployed_time,
                            version.is_latest_version
                        )
                        for version
                        in engine.project_history.values()
                    ]
                    default_logger.info(
                        tabulate(
                            version_history,
                            headers=[
                                'name',
                                'version',
                                'deployed_time',
                                'latest_version'
                            ]))

    except Exception as e:
        default_logger.error(traceback.format_exc())
        if exit_callback:
            exit_callback(1)
        else:
            exit(1)


if __name__ == '__main__':
    main()

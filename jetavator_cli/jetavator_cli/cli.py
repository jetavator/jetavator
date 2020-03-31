'''
jetavator

Usage:
  jetavator config [--test] [--config-file=<config_file>]
    [--set <option>=<value>]...
  jetavator build
  jetavator test [-d | --drop-if-exists] [-n | --nodeploy]
    [-s | --deploy-scripts-only]
    [--behave=<behave_opts>]
    [--set <option>=<value>]...
  jetavator deploy [-d|--drop-if-exists] [--wheel-only]
    [--set <option>=<value>]...
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
'''

import os
import pathlib
import traceback
import jsonschema

from docopt import docopt
from tabulate import tabulate
from textwrap import indent

from . import __version__ as VERSION
from .Client import Client
from .testing.behave import BehaveClient
from .config import KeyringConfig, CommandLineConfig
from .utils import print_to_console


def main(argv=None, exit_callback=None):
    '''Main CLI entrypoint.'''

    docopt_kwargs = {'version': VERSION}

    if argv:
        print_to_console(f'calling docopt with argv')
        docopt_kwargs['argv'] = argv

    try:
        options = docopt(__doc__, **docopt_kwargs)
    except SystemExit as e:
        print_to_console(str(e))
        if exit_callback:
            exit_callback(1)
            return
        else:
            exit(1)

    try:
        config = KeyringConfig.load(
            CommandLineConfig(options)
        )
    except jsonschema.exceptions.ValidationError:
        config = CommandLineConfig(options)

    config.reset_session()
    client = Client(config)

    printable_options = [
        k
        for k, v in options.items()
        if v is True
    ] + [
        f'{k}={v}'
        for k, v in options.items()
        if v and v is not True and not k == '--password'
    ]

    print_to_console(
        f'''
        Jetavator {VERSION} - running command:
        {' '.join(printable_options)}

        Jetavator config:
{indent(str(client.config), '        ')}
        '''
    )

    if options.get('-d') or options.get('--drop-if-exists'):
        client.config.drop_schema_if_exists = True

    if options.get('-n') or options.get('--nodeploy'):
        client.config.skip_deploy = True

    if options.get('--behave'):
        client.config.behave_options = options.get('--behave')

    try:

        if options['config']:
            KeyringConfig.command_line_options_to_keyring(options)
            test_config = KeyringConfig.load()
            if options['--test']:
                test_engine = Client(config=test_config)
                if test_engine.connection.test(master=True):
                    print_to_console(
                        'Successfully logged in and connected to '
                        f'[{client.config.environment_type}]'
                    )
                else:
                    print_to_console(
                        'Unable to log in or connect to '
                        f'[{client.config.environment_type}]'
                    )

        elif options['build']:
            client.build_wheel()

        elif options['deploy']:
            client.deploy(wheel_only=bool(options['--wheel-only']))

        elif options['test']:
            BehaveClient(client.config).test()

        elif options['update']:
            client.update()

        elif options['drop'] and options['satellite']:
            client.drop('satellite', options['<name>'])

        elif options['run']:
            load_type=('full' if options['full'] else 'delta')
            print_to_console(f'Client: Performing {load_type} load.')
            if options['<target_table>=<source_csv>']:
                table_csvs = {}
                for option in options['<target_table>=<source_csv>']:
                    table_name, csv_file = option.split('=')
                    table_csvs.setdefault(table_name, []).append(csv_file)
                for table_name, csv_files in table_csvs.items():
                    print_to_console(
                        f'Loading {len(csv_files)} CSVs '
                        f'into table: {table_name}'
                    )
                    client.engine.load_csvs(table_name, csv_files)
            if options['--folder']:
                client.engine.load_csv_folder(folder_path=options['--folder'])
            client.run(load_type='full')

        elif options['performance']:
            df = client.get_performance_data()

            if options['--pivot']:
                print_to_console(
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
                print_to_console(
                    tabulate(df, headers='keys', tablefmt='grid')
                )

        elif options['show']:
            if options['project']:
                if options['name']:
                    print_to_console(client.project.name)
                elif options['version']:
                    print_to_console(client.project.version)
                elif options['history']:
                    version_history = [
                        (
                            version.name,
                            version.version,
                            version.deployed_time,
                            version.latest_version
                        )
                        for version
                        in client.project_history.values()
                    ]
                    print_to_console(
                        tabulate(
                            version_history,
                            headers=[
                                'name',
                                'version',
                                'deployed_time',
                                'latest_version'
                            ]))

    except Exception as e:
        print_to_console(traceback.format_exc())
        if exit_callback:
            exit_callback(1)
        else:
            exit(1)


if __name__ == '__main__':
    main()

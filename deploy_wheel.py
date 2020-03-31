import os
import shutil
import jetavator

from setuptools import sandbox

from jetavator_cli import Client
from jetavator_cli.config import KeyringConfig, EnvironmentConfig


def build_wheel():
    shutil.rmtree(os.path.join(os.getcwd(), 'dist'))
    sandbox.run_setup('setup.py', ['sdist', 'bdist_wheel'])


def get_wheel_path():
    dist_dir = os.path.join(os.getcwd(), 'dist')

    wheel_files = list(filter(
        lambda x: os.path.splitext(x)[1] == '.whl',
        os.listdir(dist_dir)
    ))

    assert len(wheel_files) == 1, (
        f'Expected exactly one .whl file to be built, got {wheel_files}.'
    )

    return os.path.join(dist_dir, wheel_files[0])


os.chdir('jetavator')

jetavator_client = Client(
    KeyringConfig(
        EnvironmentConfig()
    )
)

build_wheel()
jetavator_client.deploy_wheel(
    get_wheel_path()
)

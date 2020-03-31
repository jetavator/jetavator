import os
import glob

from setuptools import sandbox


def build_wheel(source_path):
    sandbox.run_setup(
        os.path.join(source_path, 'setup.py'),
        ['sdist', 'bdist_wheel']
    )
    version_script = {}
    with open(os.path.join(source_path, 'jetavator', '__version__.py')) as f:
        exec(f.read(), version_script)
        version = version_script['__version__']
    wheels = glob.glob(
        os.path.join(source_path, 'dist', f'jetavator-{version}-*.whl')
    )
    if len(wheels) < 1:
        raise Exception(
            f'No wheel found matching "jetavator-{version}-*.whl"')
    if len(wheels) > 1:
        raise Exception(
            f'Cannot determine correct wheel from ambiguous set: {wheels}')
    return wheels[0]

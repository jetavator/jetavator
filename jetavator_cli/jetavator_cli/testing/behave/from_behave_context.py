from .BehaveConfig import BehaveConfig
from .BehaveClient import BehaveClient


def from_behave_context(context, config=None, **kwargs):

    instance = BehaveClient(
        config=(
            config
            or getattr(context, "jetavator_config", None)
            or BehaveConfig(context, **kwargs)
        )
    )

    return instance

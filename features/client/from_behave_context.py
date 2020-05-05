from .BehaveConfig import BehaveConfig
from .BehaveEngine import BehaveEngine


def from_behave_context(context, config=None, **kwargs):

    instance = BehaveEngine(
        config=(
            config
            or getattr(context, "jetavator_config", None)
            or BehaveConfig(context, **kwargs)
        )
    )

    return instance

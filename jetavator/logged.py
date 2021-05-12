from functools import wraps

from typing import Callable


def logged(function_to_decorate: Callable) -> Callable:
    @wraps(function_to_decorate)
    def decorated_function(self, *args, **kwargs):
        try:
            return function_to_decorate(self, *args, **kwargs)
        except Exception as e:
            self.logger.exception(str(e))
            raise

    return decorated_function

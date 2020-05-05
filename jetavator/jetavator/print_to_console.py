def print_to_console(print_output, *args, **kwargs):
    if callback:
        callback(str(print_output))
    print(print_output, *args, flush=True, **kwargs)


def listen_to_console(set_callback):
    global callback
    callback = set_callback


def unlisten_to_console():
    global callback
    callback = None


callback = None
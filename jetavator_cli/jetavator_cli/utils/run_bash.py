import subprocess

from .print_to_console import print_to_console


def run_bash(
    bash_script,
    print_output=True,
    retry_limit=1,
    break_on_error=True
):

    assert retry_limit > 0, "retry_limit must be greater than zero"

    for attempts in range(0, retry_limit):
        try:
            script = bash_script.replace("\\\r\n", "").replace("\\\n", "")
            script_output = subprocess.check_output(
                f"bash -c '{script}'",
                shell=True
            )
            break
        except subprocess.CalledProcessError as process_error:
            if attempts + 1 == retry_limit:
                error_output = process_error.output.decode("ascii", "ignore")
                if break_on_error:
                    raise Exception(
                        f"""
                        Error running bash command: {bash_script}
                        Return code: {process_error.returncode}
                        Output: {error_output}
                        """
                    )
                else:
                    return error_output, process_error.returncode

    if print_output:
        print_to_console(script_output.decode("ascii", "ignore"))

    return script_output.decode("utf-8"), 0

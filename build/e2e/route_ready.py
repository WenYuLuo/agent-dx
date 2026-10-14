"""Wait for the subscribed data-plane route before injecting a file fault."""

import time

from adx_environment import EnvironmentError


def wait_for_route(environment, *, timeout=10, clock=time.monotonic, sleep=time.sleep):
    """Use a read-only command query; retry only a missing synchronized route."""
    deadline = clock() + timeout
    while True:
        try:
            environment.commands.list()
            return
        except EnvironmentError as error:
            if 'route absent from synchronized cache' not in str(error):
                raise
            if clock() >= deadline:
                raise TimeoutError('instance route did not synchronize') from error
            sleep(0.1)

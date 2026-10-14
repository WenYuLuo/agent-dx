#!/usr/bin/env python3
"""Exit a separate SDK client while its detached command remains active."""

import json
import logging
import os
import sys
from pathlib import Path

from adx_environment import CommandStatus, ConnectionConfig, Environment


def main(image, output):
    secrets = Path('/secrets')
    connection = ConnectionConfig(
        server_address='127.0.0.1:8443',
        token=(secrets / 'api-key').read_text().strip(),
        use_tls=True,
        verify_tls=True,
    )
    environment = Environment(
        image=image,
        runtime='runc',
        cpu=500,
        memory=512,
        idle_timeout=6,
        detached=True,
        node_id='node1',
        connection=connection,
        create_timeout=150,
    )
    try:
        command = environment.commands.run('sleep 120', background=True, command_id='idle-client-exit')
        if not (command.poll() == CommandStatus.RUNNING):
            raise AssertionError()
        output.write_text(
            json.dumps(
                {
                    'instance_id': environment.id,
                    'command_id': command.id,
                    'command_running_before_exit': True,
                    'client_pid': os.getpid(),
                }
            )
            + '\n'
        )
    except Exception:
        try:
            environment.kill()
        except Exception:
            logging.getLogger(__name__).debug("Best-effort operation failed", exc_info=True)
        raise
    finally:
        environment.close()


if __name__ == '__main__':
    main(sys.argv[1], Path(sys.argv[2]))

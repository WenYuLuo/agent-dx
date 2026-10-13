"""Creation-time configuration for Execd-managed workload probes."""
from dataclasses import dataclass
from typing import Optional, Sequence, Tuple


def _integer(name, value, minimum, maximum):
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")


@dataclass(frozen=True)
class _Probe:
    kind: str
    port: Optional[int] = None
    path: str = "/"
    command: Tuple[str, ...] = ()
    initial_delay_seconds: int = 0
    period_seconds: int = 2
    timeout_seconds: int = 1

    def __post_init__(self):
        _integer("initial_delay_seconds", self.initial_delay_seconds, 0, 86400)
        _integer("period_seconds", self.period_seconds, 1, 86400)
        _integer("timeout_seconds", self.timeout_seconds, 1, 3600)
        if self.kind in ("httpGet", "tcpSocket"):
            _integer("port", self.port, 1, 65535)
            if self.command:
                raise ValueError("network probe cannot contain a command")
        elif self.kind == "exec":
            if isinstance(self.command, (str, bytes)) or not isinstance(self.command, Sequence):
                raise TypeError("command must be an argv sequence")
            command = tuple(self.command)
            if not command or not command[0] or len(command) > 256 or any(
                not isinstance(arg, str) or "\0" in arg for arg in command
            ) or sum(len(arg.encode()) for arg in command) > 16384:
                raise ValueError("command must be a bounded nonempty argv without NUL")
            if self.port is not None:
                raise ValueError("exec probe cannot contain a port")
            object.__setattr__(self, "command", command)
        else:
            raise ValueError("unsupported probe action")
        if self.kind == "httpGet" and (
            not isinstance(self.path, str) or not self.path.startswith("/")
            or self.path.startswith("//") or len(self.path.encode()) > 2048
            or any(ord(char) <= 32 or ord(char) == 127 for char in self.path)
        ):
            raise ValueError("path must be an origin-form HTTP path without whitespace")

    @classmethod
    def http_get(cls, *, port: int, path: str = "/", **timing):
        return cls(kind="httpGet", port=port, path=path, **timing)

    @classmethod
    def tcp_socket(cls, *, port: int, **timing):
        return cls(kind="tcpSocket", port=port, **timing)

    @classmethod
    def exec(cls, *, command: Sequence[str], **timing):
        return cls(kind="exec", command=command, **timing)

    def to_dict(self):
        if self.kind == "httpGet":
            action = {"port": self.port, "path": self.path}
        elif self.kind == "tcpSocket":
            action = {"port": self.port}
        else:
            action = {"command": list(self.command)}
        return {
            self.kind: action,
            "initialDelaySeconds": self.initial_delay_seconds,
            "periodSeconds": self.period_seconds,
            "timeoutSeconds": self.timeout_seconds,
        }


@dataclass(frozen=True)
class ReadinessProbe(_Probe):
    """Retry inside Execd until first success or the initialization deadline."""


@dataclass(frozen=True)
class HealthcheckProbe(_Probe):
    """Periodic check armed after startup; terminal failures are handled by Adxlet."""
    period_seconds: int = 10
    timeout_seconds: int = 2
    failure_threshold: int = 3

    def __post_init__(self):
        super().__post_init__()
        _integer("failure_threshold", self.failure_threshold, 1, (1 << 32) - 1)

    def to_dict(self):
        return {**super().to_dict(), "failureThreshold": self.failure_threshold}

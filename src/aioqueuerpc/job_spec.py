from asyncio import Future
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Optional


class JobError(Exception):
    def __init__(
        self, msg: str = "", cancelled: bool = False, exc: Exception = None
    ) -> None:
        self.message = msg
        self.cancelled = cancelled
        self.coro_exc = exc
        super().__init__(msg)


@dataclass
class JobSpec:
    name: str
    coro: Callable[[Any], Awaitable[Any]]
    params: Optional[dict[str, Any]] = None
    future: Optional[Future] = None

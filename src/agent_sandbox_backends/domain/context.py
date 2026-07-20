from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from contextvars import ContextVar, Token

from agent_sandbox_backends._internal.ids import uuid7
from agent_sandbox_backends.domain.base import DomainModel


class ActorContext(DomainModel):
    actor_type: str = "system"
    actor_id: str | None = None
    thread_id: str | None = None
    run_id: str | None = None
    correlation_id: str
    parent_operation_id: str | None = None


_current_actor_context: ContextVar[ActorContext | None] = ContextVar(
    "agent_sandbox_actor_context",
    default=None,
)


def get_actor_context() -> ActorContext:
    current = _current_actor_context.get()
    if current is not None:
        return current
    return ActorContext(correlation_id=str(uuid7()))


@contextmanager
def actor_context(
    *,
    actor_type: str = "agent",
    actor_id: str | None = None,
    thread_id: str | None = None,
    run_id: str | None = None,
    correlation_id: str | None = None,
    parent_operation_id: str | None = None,
) -> Generator[ActorContext, None, None]:
    context = ActorContext(
        actor_type=actor_type,
        actor_id=actor_id,
        thread_id=thread_id,
        run_id=run_id,
        correlation_id=correlation_id or str(uuid7()),
        parent_operation_id=parent_operation_id,
    )
    token: Token[ActorContext | None] = _current_actor_context.set(context)
    try:
        yield context
    finally:
        _current_actor_context.reset(token)

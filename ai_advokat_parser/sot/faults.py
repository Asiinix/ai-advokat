"""Failure classification shared by the SOT scanner and Railway supervisor."""

from __future__ import annotations


TRANSIENT_DATABASE_SQLSTATES = frozenset({"53300", "57P01", "57P02", "57P03"})
PERMANENT_DATABASE_SQLSTATE_PREFIXES = ("28", "3D", "3F")
PERMANENT_DATABASE_ERROR_NAMES = frozenset(
    {"InvalidAuthorizationSpecification", "InvalidCatalogName", "InvalidPassword"}
)
PERMANENT_DATABASE_MESSAGE_MARKERS = (
    "password authentication failed",
    "no password supplied",
    "invalid connection option",
    "invalid dsn",
)
TRANSIENT_DATABASE_MESSAGE_MARKERS = (
    "connection is closed",
    "connection already closed",
    "connection refused",
    "could not connect",
    "could not translate host name",
    "not connected",
    "server closed",
    "closed the connection",
    "unexpected eof",
    "ssl syscall error",
    "timeout expired",
    "connection timeout",
    "connection timed out",
    "network is unreachable",
    "no route to host",
)


def _is_psycopg_error(exc: BaseException, class_name: str) -> bool:
    """Recognize psycopg errors without making psycopg a local-test dependency."""
    return any(
        base.__name__ == class_name and base.__module__.split(".", 1)[0] == "psycopg"
        for base in type(exc).__mro__
    )


def _looks_like_psycopg_error(exc: BaseException) -> bool:
    """Recognize the psycopg base error, including lightweight test doubles."""
    module_root = type(exc).__module__.split(".", 1)[0]
    return _is_psycopg_error(exc, "Error") or (
        module_root == "psycopg" and type(exc).__name__.endswith("Error")
    )


def find_database_error(exc: BaseException) -> BaseException | None:
    """Return any psycopg failure from a possibly wrapped exception."""
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if _looks_like_psycopg_error(current):
            return current
        current = current.__cause__ or current.__context__
    return None


def is_database_error(exc: BaseException) -> bool:
    return find_database_error(exc) is not None


def find_transient_database_error(exc: BaseException) -> BaseException | None:
    """Return the recoverable Postgres cause from a possibly wrapped error.

    A fresh CLI run is safe, while replaying an individual SQL statement is
    not: some store methods update counters and an ambiguous commit could be
    counted twice. Authentication, database-name and schema/config errors stay
    fatal instead of creating an endless reconnect loop.
    """
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        error_name = type(current).__name__
        sqlstate = str(getattr(current, "sqlstate", "") or "").upper()
        detail = str(current).lower()

        permanent_message = any(
            marker in detail for marker in PERMANENT_DATABASE_MESSAGE_MARKERS
        ) or (
            "does not exist" in detail
            and ("database " in detail or "role " in detail)
        )
        if (
            error_name in PERMANENT_DATABASE_ERROR_NAMES
            or sqlstate.startswith(PERMANENT_DATABASE_SQLSTATE_PREFIXES)
            or permanent_message
        ):
            return None
        if _is_psycopg_error(current, "OperationalError"):
            if not sqlstate:
                return (
                    current
                    if any(marker in detail for marker in TRANSIENT_DATABASE_MESSAGE_MARKERS)
                    else None
                )
            if sqlstate.startswith(("08", "40")) or sqlstate in TRANSIENT_DATABASE_SQLSTATES:
                return current
            return None
        if _is_psycopg_error(current, "InterfaceError"):
            detail = str(current).lower()
            if any(marker in detail for marker in TRANSIENT_DATABASE_MESSAGE_MARKERS):
                return current
            return None

        current = current.__cause__ or current.__context__
    return None


def is_transient_database_error(exc: BaseException) -> bool:
    return find_transient_database_error(exc) is not None

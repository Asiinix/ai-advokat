"""Small allowlisted HTTP CONNECT proxy for Railway private networking.

The proxy deliberately implements only two operations:

* ``GET /healthz`` for Railway's deployment health check;
* ``CONNECT`` tunnels to the two HTTPS origins needed by the PRG.SOT client.

It never terminates TLS, records request headers, or logs destination names.
There is no public Railway domain on the service; callers reach it through the
project's ``*.railway.internal`` network.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
from dataclasses import dataclass


DEFAULT_ALLOWED_HOSTS = frozenset({"auth.zakon.kz", "sb.prg.kz"})
HTTPS_PORT = 443


def _bounded_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = (os.environ.get(name) or str(default)).strip()
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


@dataclass(frozen=True)
class ProxyConfig:
    listen_host: str
    port: int
    allowed_hosts: frozenset[str]
    allowed_ports: frozenset[int]
    max_connections: int
    max_header_bytes: int
    header_timeout_seconds: int
    connect_timeout_seconds: int
    idle_timeout_seconds: int
    max_tunnel_lifetime_seconds: int

    @classmethod
    def from_env(cls) -> "ProxyConfig":
        raw_hosts = os.environ.get("EGRESS_PROXY_ALLOWED_HOSTS") or ",".join(
            sorted(DEFAULT_ALLOWED_HOSTS)
        )
        allowed_hosts = frozenset(
            host.strip().lower().rstrip(".") for host in raw_hosts.split(",") if host.strip()
        )
        if not allowed_hosts:
            raise ValueError("EGRESS_PROXY_ALLOWED_HOSTS must name at least one host")
        if any(
            not host
            or "/" in host
            or ":" in host
            or "@" in host
            or host.startswith(".")
            for host in allowed_hosts
        ):
            raise ValueError("EGRESS_PROXY_ALLOWED_HOSTS contains an invalid hostname")
        if not allowed_hosts.issubset(DEFAULT_ALLOWED_HOSTS):
            raise ValueError("EGRESS_PROXY_ALLOWED_HOSTS may only narrow the built-in PRG allowlist")
        return cls(
            listen_host="0.0.0.0",
            port=_bounded_int("PORT", 8080, 1, 65535),
            allowed_hosts=allowed_hosts,
            # Production has no environment override for ports. PRG traffic is
            # HTTPS-only, so an operator cannot accidentally turn this into a
            # general TCP relay. Tests inject an ephemeral local port directly.
            allowed_ports=frozenset({HTTPS_PORT}),
            max_connections=_bounded_int("EGRESS_PROXY_MAX_CONNECTIONS", 16, 1, 256),
            max_header_bytes=_bounded_int("EGRESS_PROXY_MAX_HEADER_BYTES", 16_384, 1_024, 65_536),
            header_timeout_seconds=_bounded_int("EGRESS_PROXY_HEADER_TIMEOUT_SECONDS", 5, 1, 30),
            connect_timeout_seconds=_bounded_int("EGRESS_PROXY_CONNECT_TIMEOUT_SECONDS", 15, 1, 60),
            idle_timeout_seconds=_bounded_int("EGRESS_PROXY_IDLE_TIMEOUT_SECONDS", 120, 15, 900),
            max_tunnel_lifetime_seconds=_bounded_int(
                "EGRESS_PROXY_MAX_TUNNEL_LIFETIME_SECONDS", 3_600, 60, 86_400
            ),
        )


class ConnectProxy:
    def __init__(self, config: ProxyConfig) -> None:
        self.config = config
        self._slots = asyncio.Semaphore(config.max_connections)

    async def start(self) -> asyncio.AbstractServer:
        return await asyncio.start_server(
            self._handle,
            self.config.listen_host,
            self.config.port,
            limit=self.config.max_header_bytes + 1,
            backlog=max(16, self.config.max_connections * 2),
        )

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        upstream_writer: asyncio.StreamWriter | None = None
        acquired = False
        tunnel_started = False
        try:
            try:
                await asyncio.wait_for(self._slots.acquire(), timeout=0.05)
                acquired = True
            except TimeoutError:
                await self._respond(writer, 503, "Service Unavailable")
                return

            request_line = await self._read_request_line(reader)
            if request_line is None:
                await self._respond(writer, 431, "Request Header Fields Too Large")
                return
            method, target, version = request_line
            if version not in {"HTTP/1.0", "HTTP/1.1"}:
                await self._respond(writer, 400, "Bad Request")
                return
            if method == "GET" and target == "/healthz":
                await self._respond(writer, 200, "OK", body=b"ok\n")
                return
            if method != "CONNECT":
                await self._respond(writer, 405, "Method Not Allowed")
                return

            authority = self._allowed_authority(target)
            if authority is None:
                await self._respond(writer, 403, "Forbidden")
                return

            host, port = authority
            try:
                upstream_reader, upstream_writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=self.config.connect_timeout_seconds,
                )
            except (OSError, TimeoutError):
                await self._respond(writer, 502, "Bad Gateway")
                return

            writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await writer.drain()
            tunnel_started = True
            await self._tunnel(reader, writer, upstream_reader, upstream_writer)
        except (ConnectionError, TimeoutError, asyncio.IncompleteReadError):
            if not tunnel_started:
                with contextlib.suppress(Exception):
                    await self._respond(writer, 408, "Request Timeout")
        except Exception:
            # Deliberately generic: exception strings from socket libraries can
            # contain destination or header data and must never enter logs.
            if not tunnel_started:
                with contextlib.suppress(Exception):
                    await self._respond(writer, 500, "Internal Server Error")
        finally:
            if acquired:
                self._slots.release()
            if upstream_writer is not None:
                await self._close(upstream_writer)
            await self._close(writer)

    async def _read_request_line(
        self, reader: asyncio.StreamReader
    ) -> tuple[str, str, str] | None:
        try:
            header = await asyncio.wait_for(
                reader.readuntil(b"\r\n\r\n"),
                timeout=self.config.header_timeout_seconds,
            )
        except asyncio.LimitOverrunError:
            return None
        if len(header) > self.config.max_header_bytes:
            return None
        try:
            first_line = header.split(b"\r\n", 1)[0].decode("ascii")
        except UnicodeDecodeError:
            return ("", "", "")
        parts = first_line.split()
        if len(parts) != 3:
            return ("", "", "")
        return parts[0].upper(), parts[1], parts[2].upper()

    def _allowed_authority(self, target: str) -> tuple[str, int] | None:
        if target.count(":") != 1 or any(marker in target for marker in ("/", "@", "[", "]")):
            return None
        raw_host, raw_port = target.rsplit(":", 1)
        host = raw_host.lower().rstrip(".")
        try:
            port = int(raw_port)
        except ValueError:
            return None
        if host not in self.config.allowed_hosts or port not in self.config.allowed_ports:
            return None
        return host, port

    async def _tunnel(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        upstream_reader: asyncio.StreamReader,
        upstream_writer: asyncio.StreamWriter,
    ) -> None:
        directions = {
            asyncio.create_task(self._relay(client_reader, upstream_writer)),
            asyncio.create_task(self._relay(upstream_reader, client_writer)),
        }
        group = asyncio.gather(*directions, return_exceptions=True)
        try:
            await asyncio.wait_for(
                group,
                timeout=self.config.max_tunnel_lifetime_seconds,
            )
        finally:
            if not group.done():
                group.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await group

    async def _relay(self, source: asyncio.StreamReader, destination: asyncio.StreamWriter) -> None:
        while True:
            data = await asyncio.wait_for(
                source.read(65_536),
                timeout=self.config.idle_timeout_seconds,
            )
            if not data:
                if destination.can_write_eof():
                    with contextlib.suppress(Exception):
                        destination.write_eof()
                        await destination.drain()
                return
            destination.write(data)
            await destination.drain()

    @staticmethod
    async def _respond(
        writer: asyncio.StreamWriter,
        status: int,
        reason: str,
        body: bytes = b"",
    ) -> None:
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Content-Type: text/plain; charset=utf-8\r\n"
            "Connection: close\r\n"
            "Cache-Control: no-store\r\n"
            "\r\n".encode("ascii")
            + body
        )
        await writer.drain()

    @staticmethod
    async def _close(writer: asyncio.StreamWriter) -> None:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def _serve(config: ProxyConfig) -> None:
    proxy = ConnectProxy(config)
    server = await proxy.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    print(
        "egress proxy ready "
        f"port={config.port} allowlisted_hosts={len(config.allowed_hosts)} "
        f"max_connections={config.max_connections}",
        flush=True,
    )
    async with server:
        serve_task = asyncio.create_task(server.serve_forever())
        await stop.wait()
        server.close()
        await server.wait_closed()
        serve_task.cancel()
        await asyncio.gather(serve_task, return_exceptions=True)


def main() -> None:
    asyncio.run(_serve(ProxyConfig.from_env()))


if __name__ == "__main__":
    main()

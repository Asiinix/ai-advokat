from __future__ import annotations

import asyncio
import os
import unittest
from unittest import mock

from egress_proxy.server import ConnectProxy, ProxyConfig


class ConnectProxyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            try:
                while data := await reader.read(65_536):
                    writer.write(data)
                    await writer.drain()
            finally:
                writer.close()
                await writer.wait_closed()

        self.echo_server = await asyncio.start_server(echo, "127.0.0.1", 0)
        self.echo_port = self.echo_server.sockets[0].getsockname()[1]
        config = ProxyConfig(
            listen_host="127.0.0.1",
            port=0,
            allowed_hosts=frozenset({"127.0.0.1"}),
            allowed_ports=frozenset({self.echo_port}),
            max_connections=2,
            max_header_bytes=4096,
            header_timeout_seconds=2,
            connect_timeout_seconds=2,
            idle_timeout_seconds=2,
            max_tunnel_lifetime_seconds=10,
        )
        self.proxy_server = await ConnectProxy(config).start()
        self.proxy_port = self.proxy_server.sockets[0].getsockname()[1]

    async def asyncTearDown(self) -> None:
        self.proxy_server.close()
        self.echo_server.close()
        await self.proxy_server.wait_closed()
        await self.echo_server.wait_closed()

    async def request(self, payload: bytes) -> bytes:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.proxy_port)
        writer.write(payload)
        await writer.drain()
        response = await reader.read()
        writer.close()
        await writer.wait_closed()
        return response

    async def test_health_endpoint(self) -> None:
        response = await self.request(b"GET /healthz HTTP/1.1\r\nHost: proxy\r\n\r\n")
        self.assertIn(b"200 OK", response)
        self.assertTrue(response.endswith(b"ok\n"))

    async def test_plain_http_proxying_is_rejected(self) -> None:
        response = await self.request(b"GET http://sb.prg.kz/ HTTP/1.1\r\n\r\n")
        self.assertIn(b"405 Method Not Allowed", response)

    async def test_non_allowlisted_connect_is_rejected(self) -> None:
        response = await self.request(b"CONNECT example.com:443 HTTP/1.1\r\n\r\n")
        self.assertIn(b"403 Forbidden", response)

    async def test_connect_tunnels_bytes_without_tls_interception(self) -> None:
        reader, writer = await asyncio.open_connection("127.0.0.1", self.proxy_port)
        writer.write(
            f"CONNECT 127.0.0.1:{self.echo_port} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{self.echo_port}\r\n\r\n".encode("ascii")
        )
        await writer.drain()
        response = await reader.readuntil(b"\r\n\r\n")
        self.assertIn(b"200 Connection Established", response)
        writer.write(b"opaque-tls-bytes")
        await writer.drain()
        self.assertEqual(await reader.readexactly(16), b"opaque-tls-bytes")
        writer.close()
        await writer.wait_closed()

    async def test_client_half_close_does_not_truncate_the_response_tail(self) -> None:
        async def reply_after_eof(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            body = await reader.read()
            writer.write(b"tail:" + body)
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        tail_server = await asyncio.start_server(reply_after_eof, "127.0.0.1", 0)
        tail_port = tail_server.sockets[0].getsockname()[1]
        config = ProxyConfig(
            listen_host="127.0.0.1",
            port=0,
            allowed_hosts=frozenset({"127.0.0.1"}),
            allowed_ports=frozenset({tail_port}),
            max_connections=1,
            max_header_bytes=4096,
            header_timeout_seconds=2,
            connect_timeout_seconds=2,
            idle_timeout_seconds=2,
            max_tunnel_lifetime_seconds=10,
        )
        proxy_server = await ConnectProxy(config).start()
        proxy_port = proxy_server.sockets[0].getsockname()[1]
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", proxy_port)
            writer.write(f"CONNECT 127.0.0.1:{tail_port} HTTP/1.0\r\n\r\n".encode("ascii"))
            await writer.drain()
            self.assertIn(b"200 Connection Established", await reader.readuntil(b"\r\n\r\n"))
            writer.write(b"request")
            await writer.drain()
            writer.write_eof()
            self.assertEqual(await reader.read(), b"tail:request")
            writer.close()
            await writer.wait_closed()
        finally:
            proxy_server.close()
            tail_server.close()
            await proxy_server.wait_closed()
            await tail_server.wait_closed()

    def test_environment_cannot_expand_the_prg_allowlist(self) -> None:
        with mock.patch.dict(
            os.environ,
            {"PORT": "8080", "EGRESS_PROXY_ALLOWED_HOSTS": "sb.prg.kz,example.com"},
            clear=True,
        ):
            with self.assertRaisesRegex(ValueError, "only narrow"):
                ProxyConfig.from_env()


if __name__ == "__main__":
    unittest.main()

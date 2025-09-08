import struct
import socket
import asyncio


class AsyncSocket:
    __slots__ = ("_sock", "_loop", "_addr", "_header_size", "_buffer_size", "_closed")
    def __init__(self, sock, *, loop=None):
        self._sock = sock
        if self._sock.getblocking():
            self._sock.setblocking(False)
        self._loop = loop or asyncio.get_running_loop()
        self._header_size = 4
        self._buffer_size = 2 ** 17
        self._closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.close()
        return False

    async def accept(self):
        conn, addr = await self._loop.sock_accept(self._sock)
        client = AsyncSocket(conn)
        return client, addr

    async def connect(self, addr):
        return await self._loop.sock_connect(self._sock, addr)

    async def recv(self, nbytes):
        return await self._loop.sock_recv(self._sock, nbytes)

    async def recv_into(self, buf):
        return await self._loop.sock_recv_into(self._sock, buf)

    async def recvfrom(self, bufsize):
        return await self._loop.sock_recvfrom(self._sock, bufsize)

    async def recvfrom_into(self, buf, nbytes=0):
        return await self._loop.sock_recvfrom_into(self._sock, buf, nbytes)

    async def sendall(self, data):
        return await self._loop.sock_sendall(self._sock, data)

    async def sendfile(self, file, offset=0, count=None, *, fallback=True):
        return await self._loop.sock_sendfile(self._sock, file, offset, count, fallback=fallback)

    async def sendto(self, data, address):
        return await self._loop.sock_sendto(self._sock, data, address)

    async def recv_packet(self):
        header = await self.recv(self._header_size)
        size, = struct.unpack("<I", header)
        data = bytearray(size)
        view = memoryview(data)
        total = 0
        while total < size:
            total += await self.recv_into(view[total:])
        view.release()
        return data

    async def send_packet(self, data):
        header = struct.pack("<I", len(data))
        await self.sendall(header)
        await self.sendall(data)

    def is_closed(self):
        return self._closed

    def close(self):
        if self.is_closed():
            self._sock.shutdown(2)
            self._sock.close()
            self._closed = True
            return True
        return False


def create_server(*args, **kwargs):
    return AsyncSocket(socket.create_server(*args, **kwargs))


def create_connection(*args, **kwargs):
    return AsyncSocket(socket.create_connection(*args, **kwargs))


def create_unix_server(path, backlog=None):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(path)
    if backlog is not None:
        sock.listen(backlog)
    else:
        sock.listen()
    return AsyncSocket(sock)


def create_unix_connection(path):
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(path)
    return AsyncSocket(sock)

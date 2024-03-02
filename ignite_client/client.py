import asyncio

from ignite_client.protocol import HandshakeRequest, HandshakeResponse, decode_handshake_response, \
    QuerySqlFieldsRequest, QuerySqlFieldsResponse, Request, Response
from ignite_client.utils import AtomicInteger


class IgniteClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.request_id = AtomicInteger()
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def _read_response(self, decode_function):
        length_bytes = await self.reader.readexactly(4)
        response_length = int.from_bytes(length_bytes, byteorder='little')

        response_data = await self.reader.readexactly(response_length)
        return decode_function(response_data)

    async def handshake(self, request: HandshakeRequest) -> HandshakeResponse:
        if self.writer is None or self.reader is None:
            raise ConnectionError("Client is not connected")

        self.writer.write(request.encode())
        await self.writer.drain()

        return await self._read_response(decode_handshake_response)

    async def query_sql_fields(self, request: QuerySqlFieldsRequest) -> QuerySqlFieldsResponse:
        if self.writer is None or self.reader is None:
            raise ConnectionError("Client is not connected")

        request_id = self.request_id.increment()
        encoded_request = Request.new_query_sql_fields(request_id, request).encode()
        self.writer.write(encoded_request)
        await self.writer.drain()

        response = await self._read_response(
            lambda data: Response.decode_query_sql_fields(data, includes_field_names=request.include_field_names))
        if response.status_code != 0:
            raise Exception(f"Error: {response.error_message}")

        if isinstance(response.body, QuerySqlFieldsResponse):
            return response.body
        raise Exception("Unexpected response type")

    async def query_sql_fields_cursor_get_page(self, cursor_id: int, column_count: int):
        if self.writer is None or self.reader is None:
            raise ConnectionError("Client is not connected")

        request_id = self.request_id.increment()
        encoded_request = Request.new_query_sql_fields_cursor_get_page(request_id, cursor_id).encode()
        self.writer.write(encoded_request)
        await self.writer.drain()

        response = await self._read_response(
            lambda data: Response.decode_query_sql_fields_cursor_get_page(data, column_count)
        )
        if response.status_code != 0:
            raise Exception(f"Error: {response.error_message}")

        return response.body

    async def resource_close(self, resource_id: int):
        if self.writer is None or self.reader is None:
            raise ConnectionError("Client is not connected")

        request_id = self.request_id.increment()
        encoded_request = Request.new_resource_close(request_id, resource_id).encode()
        self.writer.write(encoded_request)
        await self.writer.drain()

        response = await self._read_response(Response.decode_resource_close)
        if response.status_code != 0:
            raise Exception(f"Error: {response.error_message}")

    async def close(self):
        if self.writer:
            self.writer.close()

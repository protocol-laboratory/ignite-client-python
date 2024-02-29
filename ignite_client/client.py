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

    async def handshake(self, request: HandshakeRequest) -> HandshakeResponse:
        if self.writer is None or self.reader is None:
            raise ConnectionError("Client is not connected")

        self.writer.write(request.encode())
        await self.writer.drain()

        length_bytes = await self.reader.readexactly(4)
        response_length = int.from_bytes(length_bytes, byteorder='little')

        response_data = await self.reader.readexactly(response_length)

        return decode_handshake_response(response_data)

    async def query_sql_fields(self, request: QuerySqlFieldsRequest) -> QuerySqlFieldsResponse:
        if self.writer is None or self.reader is None:
            raise ConnectionError("Client is not connected")

        request_id = self.request_id.increment()
        encoded_request = Request.new_query_sql_fields(request_id, request).encode()
        self.writer.write(encoded_request)
        await self.writer.drain()

        length_bytes = await self.reader.readexactly(4)
        response_length = int.from_bytes(length_bytes, byteorder='little')

        response_data = await self.reader.readexactly(response_length)

        response = Response.decode_query_sql_fields(response_data, includes_field_names=True)
        if response.status_code != 0:
            raise Exception(f"Error: {response.error_message}")

        if isinstance(response.body, QuerySqlFieldsResponse):
            return response.body
        else:
            raise Exception("Unexpected response type")

    async def close(self):
        if self.writer:
            self.writer.close()

import unittest

from ignite_client.client import IgniteClient
from ignite_client.protocol import HandshakeRequest, HandshakeSuccess, HandshakeFailed, QuerySqlFieldsRequest, \
    StatementType


class TestIgniteClient(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = IgniteClient('127.0.0.1', 10800)
        await self.client.connect()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_handshake_success(self):
        request = HandshakeRequest(major_version=1, minor_version=0, patch_version=0, username="", password="")
        response = await self.client.handshake(request)
        self.assertIsInstance(response, HandshakeSuccess)

    async def test_handshake_fail(self):
        request = HandshakeRequest(major_version=2, minor_version=15, patch_version=0, username="", password="")
        response = await self.client.handshake(request)
        self.assertIsInstance(response, HandshakeFailed)

    async def test_query_sql_fields_success(self):
        request = HandshakeRequest(major_version=1, minor_version=0, patch_version=0, username="", password="")
        response = await self.client.handshake(request)
        self.assertIsInstance(response, HandshakeSuccess)
        request = QuerySqlFieldsRequest(
            cache_id=0,
            schema="PUBLIC",
            cursor_page_size=1024,
            max_rows=65535,
            sql="SELECT * FROM SYS.SCHEMAS",
            query_arg_count=0,
            query_args=[],
            statement_type=StatementType.SELECT,
            distributed_join=False,
            local_query=False,
            replicated_only=False,
            enforce_join_order=False,
            collocated=False,
            lazy=False,
            timeout_milliseconds=30_000,
            include_field_names=True
        )
        response = await self.client.query_sql_fields(request)
        assert len(response.column_names) > 0


if __name__ == '__main__':
    unittest.main()

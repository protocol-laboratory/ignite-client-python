from dataclasses import dataclass
from enum import Enum
from typing import Union, List, Any

from ignite_client.constants import LEN_CACHE_ID, LEN_CURSOR_PAGE_SIZE, LEN_MAX_ROWS, LEN_QUERY_ARG_COUNT, \
    LEN_STATEMENT_TYPE, LEN_DISTRIBUTED_JOIN, LEN_LOCAL_QUERY, LEN_REPLICATED_ONLY, LEN_ENFORCE_JOIN_ORDER, \
    LEN_COLLOCATED, LEN_LAZY, LEN_TIMEOUT, LEN_INCLUDE_FIELD_NAMES, QUERY_SQL_FIELDS


@dataclass
class HandshakeRequest:
    major_version: int
    minor_version: int
    patch_version: int
    username: str
    password: str

    def length(self) -> int:
        return 1 + 2 + 2 + 2 + 1 + 4 + len(self.username) + 4 + len(self.password)

    def encode(self) -> bytes:
        encoded = bytearray(self.length() + 4)
        encoded[0:4] = (self.length()).to_bytes(4, byteorder='little', signed=True)
        encoded[4] = 1
        encoded[5:7] = self.major_version.to_bytes(2, byteorder='little', signed=True)
        encoded[7:9] = self.minor_version.to_bytes(2, byteorder='little', signed=True)
        encoded[9:11] = self.patch_version.to_bytes(2, byteorder='little', signed=True)
        encoded[11] = 2
        encoded[12:16] = len(self.username).to_bytes(4, byteorder='little', signed=True)
        encoded[16:16 + len(self.username)] = self.username.encode('utf-8')
        encoded[16 + len(self.username):20 + len(self.username)] = len(self.password).to_bytes(4, byteorder='little',
                                                                                               signed=True)
        encoded[20 + len(self.username):] = self.password.encode('utf-8')
        return bytes(encoded)


@dataclass
class HandshakeSuccess:
    @staticmethod
    def decode() -> 'HandshakeSuccess':
        return HandshakeSuccess()


@dataclass
class HandshakeFailed:
    major_version: int
    minor_version: int
    patch_version: int
    error_message: str

    @staticmethod
    def decode(data: bytes) -> 'HandshakeFailed':
        major_version = int.from_bytes(data[0:2], byteorder='little', signed=True)
        minor_version = int.from_bytes(data[2:4], byteorder='little', signed=True)
        patch_version = int.from_bytes(data[4:6], byteorder='little', signed=True)
        error_message_length = int.from_bytes(data[6:10], byteorder='little', signed=True)
        error_message = data[10:10 + error_message_length].decode('utf-8')
        return HandshakeFailed(major_version, minor_version, patch_version, error_message)


HandshakeResponse = Union[HandshakeSuccess, HandshakeFailed]


def decode_handshake_response(data: bytes) -> HandshakeResponse:
    if data[0] == 1:
        return HandshakeSuccess.decode()
    else:
        return HandshakeFailed.decode(data[1:])


class StatementType(Enum):
    ANY = 0
    SELECT = 1
    UPDATE = 2


@dataclass
class QuerySqlFieldsRequest:
    cache_id: int
    schema: str
    cursor_page_size: int
    max_rows: int
    sql: str
    query_arg_count: int
    query_args: List[Any]
    statement_type: StatementType
    distributed_join: bool
    local_query: bool
    replicated_only: bool
    enforce_join_order: bool
    collocated: bool
    lazy: bool
    timeout_milliseconds: int
    include_field_names: bool

    def length(self) -> int:
        total_length = 0
        total_length += LEN_CACHE_ID
        total_length += 1
        total_length += 1 + 4 + len(self.schema)
        total_length += LEN_CURSOR_PAGE_SIZE
        total_length += LEN_MAX_ROWS
        total_length += 1 + 4 + len(self.sql)
        total_length += LEN_QUERY_ARG_COUNT
        total_length += LEN_STATEMENT_TYPE
        total_length += LEN_DISTRIBUTED_JOIN
        total_length += LEN_LOCAL_QUERY
        total_length += LEN_REPLICATED_ONLY
        total_length += LEN_ENFORCE_JOIN_ORDER
        total_length += LEN_COLLOCATED
        total_length += LEN_LAZY
        total_length += LEN_TIMEOUT
        total_length += LEN_INCLUDE_FIELD_NAMES
        return total_length

    def encode(self) -> bytes:
        encoded = bytearray(self.length())
        offset = 0

        offset = put_int(encoded, offset, self.cache_id)
        offset = put_byte(encoded, offset, 0)
        offset = put_string(encoded, offset, self.schema)
        offset = put_int(encoded, offset, self.cursor_page_size)
        offset = put_int(encoded, offset, self.max_rows)
        offset = put_string(encoded, offset, self.sql)
        offset = put_int(encoded, offset, self.query_arg_count)
        # TODO: Encode query arguments
        offset = put_statement_type(encoded, offset, self.statement_type)
        offset = put_bool(encoded, offset, self.distributed_join)
        offset = put_bool(encoded, offset, self.local_query)
        offset = put_bool(encoded, offset, self.replicated_only)
        offset = put_bool(encoded, offset, self.enforce_join_order)
        offset = put_bool(encoded, offset, self.collocated)
        offset = put_bool(encoded, offset, self.lazy)
        offset = put_long(encoded, offset, self.timeout_milliseconds)
        put_bool(encoded, offset, self.include_field_names)
        return bytes(encoded)


@dataclass
class QuerySqlFieldsResponse:
    cursor_id: int
    column_count: int
    column_names: List[str]
    first_page_row_count: int
    has_more: bool

    @staticmethod
    def decode(data: bytes, has_field_names: bool) -> 'QuerySqlFieldsResponse':
        if not data:
            raise ValueError("Empty response")

        offset = 0
        cursor_id, offset = read_long(data, offset)
        column_count, offset = read_int(data, offset)

        column_names = []
        if has_field_names:
            for _ in range(column_count):
                column_name, offset = read_string(data, offset)
                column_names.append(column_name)

        first_page_row_count, offset = read_int(data, offset)
        has_more, offset = read_bool(data, offset)

        return QuerySqlFieldsResponse(
            cursor_id=cursor_id,
            column_count=column_count,
            column_names=column_names,
            first_page_row_count=first_page_row_count,
            has_more=has_more
        )


@dataclass
class Request:
    op_code: int
    request_id: int
    body: Union[QuerySqlFieldsRequest]

    @staticmethod
    def new_query_sql_fields(request_id: int, query_sql_fields_request: QuerySqlFieldsRequest) -> 'Request':
        return Request(
            op_code=QUERY_SQL_FIELDS,
            request_id=request_id,
            body=query_sql_fields_request
        )

    def encode(self) -> bytes:
        encoded = bytearray()
        encoded += (self.body.length() + 10).to_bytes(4, byteorder='little', signed=True)
        encoded += self.op_code.to_bytes(2, byteorder='little', signed=True)
        encoded += self.request_id.to_bytes(8, byteorder='little', signed=True)
        encoded += self.body.encode()
        return bytes(encoded)


@dataclass
class Response:
    request_id: int
    status_code: int
    error_message: str
    body: Union[QuerySqlFieldsResponse]

    @staticmethod
    def decode_query_sql_fields(data: bytes, includes_field_names: bool) -> 'Response':
        if not data:
            raise ValueError("Empty response")

        request_id = int.from_bytes(data[0:8], byteorder='little', signed=True)
        status_code = int.from_bytes(data[8:12], byteorder='little', signed=True)

        if status_code != 0:
            error_message_length = int.from_bytes(data[12:16], byteorder='little', signed=True)
            error_message = data[16:16 + error_message_length].decode('utf-8')
            body = None
        else:
            error_message = ""
            body = QuerySqlFieldsResponse.decode(data[12:], has_field_names=includes_field_names)

        return Response(
            request_id=request_id,
            status_code=status_code,
            error_message=error_message,
            body=body
        )


def put_string(buffer: bytearray, offset: int, value: str) -> int:
    encoded_string = value.encode('utf-8')
    offset = put_byte(buffer, offset, 9)
    offset = put_int(buffer, offset, len(encoded_string))
    offset = put_bytes(buffer, offset, encoded_string)
    return offset


def read_string(data: bytes, offset: int) -> (str, int):
    _, offset = read_byte(data, offset)
    length, offset = read_int(data, offset)
    value = data[offset:offset + length].decode('utf-8')
    return value, offset + length


def put_bytes(buffer: bytearray, offset: int, value: bytes) -> int:
    buffer[offset:offset + len(value)] = value
    return offset + len(value)


def put_long(buffer: bytearray, offset: int, value: int) -> int:
    buffer[offset:offset + 8] = value.to_bytes(8, byteorder='little', signed=True)
    return offset + 8


def read_long(data: bytes, offset: int) -> (int, int):
    value = int.from_bytes(data[offset:offset + 8], byteorder='little', signed=True)
    return value, offset + 8


def put_int(buffer: bytearray, offset: int, value: int) -> int:
    buffer[offset:offset + 4] = value.to_bytes(4, byteorder='little', signed=True)
    return offset + 4


def read_int(data: bytes, offset: int) -> (int, int):
    value = int.from_bytes(data[offset:offset + 4], byteorder='little', signed=True)
    return value, offset + 4


def put_bool(buffer: bytearray, offset: int, value: bool) -> int:
    buffer[offset] = int(value)
    return offset + 1


def read_bool(data: bytes, offset: int) -> (bool, int):
    value, offset = read_byte(data, offset)
    return bool(value), offset


def put_statement_type(buffer: bytearray, offset: int, value: StatementType) -> int:
    buffer[offset] = value.value
    return offset + 1


def put_byte(buffer: bytearray, offset: int, value: int) -> int:
    buffer[offset] = value
    return offset + 1


def read_byte(data: bytes, offset: int) -> (int, int):
    value = data[offset]
    return value, offset + 1

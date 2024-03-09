import struct
from dataclasses import dataclass
from enum import Enum
from typing import Union, List, Any, Optional

from ignite_client.constants import LenConst, OpConst


@dataclass
class HandshakeRequest:
    major_version: int
    minor_version: int
    patch_version: int
    username: str
    password: str

    def length(self) -> int:
        total_length = 0
        total_length += LenConst.HANDSHAKE_CODE
        total_length += LenConst.MAJOR_VERSION
        total_length += LenConst.MINOR_VERSION
        total_length += LenConst.PATCH_VERSION
        total_length += 1
        total_length += 1 + 4 + len(self.username)
        total_length += 1 + 4 + len(self.password)
        return total_length

    def encode(self) -> bytes:
        encoded = bytearray(self.length() + 4)
        offset = 0

        offset = put_int(encoded, offset, self.length())
        offset = put_byte(encoded, offset, 1)
        offset = put_short(encoded, offset, self.major_version)
        offset = put_short(encoded, offset, self.minor_version)
        offset = put_short(encoded, offset, self.patch_version)
        offset = put_byte(encoded, offset, 2)
        offset = put_string(encoded, offset, self.username)
        put_string(encoded, offset, self.password)

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
        offset = 0
        major_version, offset = read_short(data, offset)
        minor_version, offset = read_short(data, offset)
        patch_version, offset = read_short(data, offset)
        error_message, offset = read_string(data, offset)
        return HandshakeFailed(major_version, minor_version, patch_version, error_message)


HandshakeResponse = Union[HandshakeSuccess, HandshakeFailed]


def decode_handshake_response(data: bytes) -> HandshakeResponse:
    if data[0] == 1:
        return HandshakeSuccess.decode()
    return HandshakeFailed.decode(data[1:])


class StatementType(Enum):
    ANY = 0
    SELECT = 1
    UPDATE = 2


@dataclass
class ResourceCloseRequest:
    resource_id: int

    @staticmethod
    def length() -> int:
        return LenConst.RESOURCE_ID

    def encode(self) -> bytes:
        encoded = bytearray(self.length())
        put_long(encoded, 0, self.resource_id)
        return bytes(encoded)


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
        total_length += LenConst.CACHE_ID
        total_length += 1
        total_length += 1 + 4 + len(self.schema)
        total_length += LenConst.CURSOR_PAGE_SIZE
        total_length += LenConst.MAX_ROWS
        total_length += 1 + 4 + len(self.sql)
        total_length += LenConst.QUERY_ARG_COUNT
        total_length += LenConst.STATEMENT_TYPE
        total_length += LenConst.DISTRIBUTED_JOIN
        total_length += LenConst.LOCAL_QUERY
        total_length += LenConst.REPLICATED_ONLY
        total_length += LenConst.ENFORCE_JOIN_ORDER
        total_length += LenConst.COLLOCATED
        total_length += LenConst.LAZY
        total_length += LenConst.TIMEOUT
        total_length += LenConst.INCLUDE_FIELD_NAMES
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
    data: List[List[Any]]
    has_more: bool

    @staticmethod
    def decode(data: bytes, has_field_names: bool) -> 'QuerySqlFieldsResponse':
        offset = 0
        cursor_id, offset = read_long(data, offset)
        column_count, offset = read_int(data, offset)

        column_names = []
        if has_field_names:
            for _ in range(column_count):
                column_name, offset = read_string(data, offset)
                column_names.append(column_name)

        first_page_row_count, offset = read_int(data, offset)
        matrix, offset = read_matrix(data, offset, first_page_row_count, column_count)
        has_more, offset = read_bool(data, offset)

        return QuerySqlFieldsResponse(
            cursor_id=cursor_id,
            column_count=column_count,
            column_names=column_names,
            first_page_row_count=first_page_row_count,
            data=matrix,
            has_more=has_more
        )


@dataclass
class QuerySqlFieldsCursorGetPageRequest:
    cursor_id: int

    @staticmethod
    def length() -> int:
        return LenConst.CURSOR_ID

    def encode(self) -> bytes:
        encoded = bytearray(self.length())
        put_long(encoded, 0, self.cursor_id)
        return bytes(encoded)


@dataclass
class QuerySqlFieldsCursorGetPageResponse:
    row_count: int
    data: List[List[Any]]
    has_more: bool

    @staticmethod
    def decode(data: bytes, column_count: int) -> 'QuerySqlFieldsCursorGetPageResponse':
        offset = 0
        row_count, offset = read_int(data, offset)
        matrix, offset = read_matrix(data, offset, row_count, column_count)
        has_more, offset = read_bool(data, offset)
        return QuerySqlFieldsCursorGetPageResponse(row_count, matrix, has_more)


@dataclass
class Request:
    op_code: int
    request_id: int
    body: Union[ResourceCloseRequest, QuerySqlFieldsRequest, QuerySqlFieldsCursorGetPageRequest]

    @staticmethod
    def new_resource_close(request_id: int, resource_id: int) -> 'Request':
        return Request(
            op_code=OpConst.RESOURCE_CLOSE,
            request_id=request_id,
            body=ResourceCloseRequest(resource_id),
        )

    @staticmethod
    def new_query_sql_fields(request_id: int, query_sql_fields_request: QuerySqlFieldsRequest) -> 'Request':
        return Request(
            op_code=OpConst.QUERY_SQL_FIELDS,
            request_id=request_id,
            body=query_sql_fields_request
        )

    @staticmethod
    def new_query_sql_fields_cursor_get_page(request_id: int, cursor_id: int) -> 'Request':
        return Request(
            op_code=OpConst.QUERY_SQL_FIELDS_CURSOR_GET_PAGE,
            request_id=request_id,
            body=QuerySqlFieldsCursorGetPageRequest(cursor_id)
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
    body: Optional[Union[QuerySqlFieldsResponse]] = None

    @staticmethod
    def decode_common(data: bytes):
        request_id, offset = read_long(data, 0)
        status_code, offset = read_int(data, offset)

        if status_code != 0:
            error_message, offset = read_string(data, offset)
        else:
            error_message = ""

        return request_id, status_code, error_message, offset

    @staticmethod
    def decode_resource_close(data: bytes):
        request_id, status_code, error_message, _ = Response.decode_common(data)

        return Response(
            request_id=request_id,
            status_code=status_code,
            error_message=error_message,
        )

    @staticmethod
    def decode_query_sql_fields(data: bytes, includes_field_names: bool) -> 'Response':
        request_id, status_code, error_message, offset = Response.decode_common(data)

        if status_code != 0:
            body = None
        else:
            body = QuerySqlFieldsResponse.decode(data[offset:], has_field_names=includes_field_names)

        return Response(
            request_id=request_id,
            status_code=status_code,
            error_message=error_message,
            body=body
        )

    @staticmethod
    def decode_query_sql_fields_cursor_get_page(data: bytes, column_count: int) -> 'Response':
        request_id, status_code, error_message, offset = Response.decode_common(data)

        if status_code != 0:
            body = None
        else:
            body = QuerySqlFieldsCursorGetPageResponse.decode(data[offset:], column_count)

        return Response(
            request_id=request_id,
            status_code=status_code,
            error_message=error_message,
            body=body
        )


def read_matrix(data: bytes, offset: int, first_page_row_count: int, column_count: int) -> (list, int):
    matrix = []
    for _ in range(first_page_row_count):
        row = []
        for _ in range(column_count):
            type_code, offset = read_byte(data, offset)
            if type_code == 1:
                value, offset = read_byte(data, offset)
                row.append(value)
            elif type_code == 2:
                value, offset = read_short(data, offset)
                row.append(value)
            elif type_code == 3:
                value, offset = read_int(data, offset)
                row.append(value)
            elif type_code == 4:
                value, offset = read_long(data, offset)
                row.append(value)
            elif type_code == 5:
                value, offset = read_float(data, offset)
                row.append(value)
            elif type_code == 6:
                value, offset = read_double(data, offset)
                row.append(value)
            elif type_code == 8:
                value, offset = read_bool(data, offset)
                row.append(value)
            elif type_code == 9:
                value, offset = read_string_no_type(data, offset)
                row.append(value)
            elif type_code == 12:
                value, offset = read_bytes(data, offset)
                row.append(value)
            elif type_code == 101:
                row.append(None)
            else:
                raise Exception(f"Unexpected type code: {type_code}")
        matrix.append(row)
    return matrix, offset


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


def read_string_no_type(data: bytes, offset: int) -> (str, int):
    length, offset = read_int(data, offset)
    value = data[offset:offset + length].decode('utf-8')
    return value, offset + length


def put_bytes(buffer: bytearray, offset: int, value: bytes) -> int:
    buffer[offset:offset + len(value)] = value
    return offset + len(value)


def read_bytes(data: bytes, offset: int) -> (bytes, int):
    length, offset = read_int(data, offset)
    value = data[offset:offset + length]
    return value, offset + length


def put_short(buffer: bytearray, offset: int, value: int) -> int:
    buffer[offset:offset + 2] = value.to_bytes(2, byteorder='little', signed=True)
    return offset + 2


def read_short(data: bytes, offset: int) -> (int, int):
    value = int.from_bytes(data[offset:offset + 2], byteorder='little', signed=True)
    return value, offset + 2


def put_int(buffer: bytearray, offset: int, value: int) -> int:
    buffer[offset:offset + 4] = value.to_bytes(4, byteorder='little', signed=True)
    return offset + 4


def read_int(data: bytes, offset: int) -> (int, int):
    value = int.from_bytes(data[offset:offset + 4], byteorder='little', signed=True)
    return value, offset + 4


def put_long(buffer: bytearray, offset: int, value: int) -> int:
    buffer[offset:offset + 8] = value.to_bytes(8, byteorder='little', signed=True)
    return offset + 8


def read_long(data: bytes, offset: int) -> (int, int):
    value = int.from_bytes(data[offset:offset + 8], byteorder='little', signed=True)
    return value, offset + 8


def read_float(data: bytes, offset: int) -> (float, int):
    value = struct.unpack('<f', data[offset:offset + 4])[0]
    return value, offset + 4


def read_double(data: bytes, offset: int) -> (float, int):
    value = struct.unpack('<d', data[offset:offset + 8])[0]
    return value, offset + 8


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

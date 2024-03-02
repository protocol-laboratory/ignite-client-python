class LenConst:
    CACHE_ID: int = 4
    COLLOCATED: int = 1
    CURSOR_ID: int = 8
    CURSOR_PAGE_SIZE: int = 4
    DISTRIBUTED_JOIN: int = 1
    ENFORCE_JOIN_ORDER: int = 1
    HANDSHAKE_CODE: int = 1
    HAS_MORE: int = 1
    INCLUDE_FIELD_NAMES: int = 1
    LAZY: int = 1
    LOCAL_QUERY: int = 1
    MAJOR_VERSION: int = 2
    MAX_ROWS: int = 4
    MINOR_VERSION: int = 2
    PATCH_VERSION: int = 2
    RESOURCE_ID: int = 8
    QUERY_ARG_COUNT: int = 4
    REPLICATED_ONLY: int = 1
    ROW_COUNT: int = 4
    STATEMENT_TYPE: int = 1
    TIMEOUT: int = 8


class OpConst:
    RESOURCE_CLOSE: int = 0
    QUERY_SCAN: int = 2000
    QUERY_SCAN_CURSOR_GET_PAGE: int = 2001
    QUERY_SQL: int = 2002
    QUERY_SQL_CURSOR_GET_PAGE: int = 2003
    QUERY_SQL_FIELDS: int = 2004
    QUERY_SQL_FIELDS_CURSOR_GET_PAGE: int = 2005

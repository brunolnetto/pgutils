# Constants

# 
PAGINATION_BATCH_SIZE=1000
DEFAULT_POOL_SIZE = 20
DEFAULT_MAX_OVERFLOW = 10
NOT_EMPTY_STR_COUNT = 1
DEFAULT_MINIMUM_PASSWORD_SIZE = 1
DEFAULT_ADMIN_USERNAME='postgres'
DEFAULT_ADMIN_PASSWORD='postgres'

DEFAULT_HEALTHCHECK_TIMEOUT_S = 5
DEFAULT_RETRY_TIMEOUT_S = 5
MAX_RETRIES = 3
INDEX_CREATION_BATCH_SIZE = 10

# Valid database schemes
VALID_SYNC_SCHEMES = [
    "postgres",
    "postgresql", 
    "postgresql+psycopg", 
    "postgresql+psycopg2",
    "postgresql+psycopg2cffi"
]

VALID_ASYNC_SCHEMES = [
    "postgresql+asyncpg"
]
VALID_SCHEMES = VALID_SYNC_SCHEMES + VALID_ASYNC_SCHEMES

# Indexes
VALID_INDEX_TYPES = {
    'btree': {'columns': True},
    'gin': {'columns': True},
    'gist': {'columns': True},
    'hash': {'columns': True},
    'spgist': {'columns': True},
    'brin': {'columns': True},
    'expression': {'columns': True, 'expression': True},
    'partial': {'columns': True, 'condition': True},
}


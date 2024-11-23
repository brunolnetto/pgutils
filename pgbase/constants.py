# Constants

# Database configuration
PAGINATION_BATCH_SIZE=100
DEFAULT_HEALTHCHECK_TIMEOUT_S = 5

# Available schemes 
VALID_SYNC_SCHEMES = [
    "postgres",
    "postgresql", 
    "postgresql+psycopg", 
    "postgresql+psycopg2",
    "postgresql+psycopg2cffi"
]

VALID_ASYNC_SCHEMES = [
    "postgresql+asyncpg",
]
VALID_SCHEMES = VALID_SYNC_SCHEMES + VALID_ASYNC_SCHEMES

# Indexes configuration
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


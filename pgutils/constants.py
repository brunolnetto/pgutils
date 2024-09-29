# Constants

PAGINATION_BATCH_SIZE=1000
DEFAULT_POOL_SIZE = 20
DEFAULT_MAX_OVERFLOW = 10
NOT_EMPTY_STR_COUNT = 1
DEFAULT_MINIMUM_PASSWORD_SIZE = 1

VALID_SCHEMES = (
    "postgresql", 
    "postgresql+psycopg", 
    "postgresql+psycopg2",
    "postgresql+asyncpg"
)
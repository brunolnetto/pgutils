from typing import (
    Dict, List, Any, Generator, AsyncGenerator, Union, Optional
)
from sqlalchemy.ext.asyncio import AsyncSession, AsyncConnection
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session


AsyncPageGenerator = AsyncGenerator[List[Any], None] 
SyncPageGenerator = Generator[List[Any], None, None]
PageGenerator = Union[AsyncPageGenerator, SyncPageGenerator]

DatabaseConnection = Union[Session, AsyncSession, Connection, AsyncConnection]
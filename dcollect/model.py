import os
from typing import AsyncGenerator, Awaitable, Optional, Tuple

import databases


SETUP_QUERIES = [
    """CREATE TABLE IF NOT EXISTS cas (hash BLOB(8) PRIMARY KEY, data BLOB) """,
    """CREATE TABLE IF NOT EXISTS vsn (entity TEXT, vsn INTEGER, pointer BLOB(8), PRIMARY KEY (entity, vsn))""",
    """CREATE TABLE IF NOT EXISTS watch (entity TEXT, url TEXT, vsn INTEGER DEFAULT 0 NOT NULL, PRIMARY KEY (entity, url))""",
]

WATCH_STORE_QUERY = (
    "INSERT OR IGNORE INTO watch (entity, url) " + "VALUES (:entity, :url)"
)

WATCH_DELETE_QUERY = "DELETE FROM watch WHERE entity = :entity AND url = :url"


class Model:
    database: databases.Database

    async def setup(self):
        await self.database.connect()
        for query in SETUP_QUERIES:
            await self.database.execute(query=query)

    async def teardown(self):
        await self.database.disconnect()

    def __init__(self):
        self.database = databases.Database(os.environ["DATABASE_URL"])

    async def cas_insert(self, hash_: bytes, data: bytes):
        CAS_STORE = "INSERT OR IGNORE INTO cas (hash, data) VALUES (:hash, :data)"
        await self.database.execute(CAS_STORE, values={"hash": hash_, "data": data})

    async def get_ca(self, hs: bytes) -> Optional[bytes]:
        CAS_GET = "SELECT data FROM cas WHERE hash = :hash"
        res = await self.database.fetch_one(CAS_GET, values={"hash": hs})
        if res is None:
            return None
        (data,) = res
        return data

    async def watch_store(self, entity: str, url: str):
        await self.database.execute(
            WATCH_STORE_QUERY, values={"entity": entity, "url": url}
        )

    async def watch_delete(self, entity: str, url: str):
        await self.database.execute(
            WATCH_DELETE_QUERY, values={"entity": entity, "url": url}
        )

    def store_vsn(
        self, entity: str, vsn: Optional[int], pointer: bytes
    ) -> Awaitable[int]:
        STORE = (
            "INSERT OR IGNORE INTO vsn (entity, vsn, pointer) "
            + "VALUES (:entity, :vsn, :pointer)"
        )
        return self.database.execute(
            STORE, values={"entity": entity, "vsn": vsn, "pointer": pointer}
        )

    async def get_latest_pointer(self, entity: str) -> Optional[bytes]:
        Q = """
        SELECT pointer
        FROM vsn
        WHERE entity = :entity
        ORDER BY vsn DESC LIMIT 1
        """
        res = await self.database.fetch_one(Q, values={"entity": entity})
        if res is None:
            return None
        (ptr,) = res
        return ptr

    async def get_trailing_watches_for_entity(
        self, entity: str,
    ) -> AsyncGenerator[Tuple[str, int], None]:
        Q = """
        WITH latest AS
        (SELECT entity, vsn as latest_vsn FROM vsn WHERE entity = :entity ORDER BY vsn DESC LIMIT 1)
        SELECT url, latest_vsn FROM watch
        INNER JOIN latest on watch.entity = latest.entity
        WHERE latest_vsn > watch.vsn;
        """
        async for e in self.database.iterate(query=Q, values={"entity": entity}):
            yield (e[0], int(e[1]))

    async def update_watch(self, entity: str, url: str, version: int):
        Q = """
        UPDATE watch SET version = :version WHERE entity = :entity AND url = :url AND version < :version
        """
        await self.database.execute(
            Q, values={"entity": entity, "url": url, "version": version}
        )

    async def get_history(self, entity: str) -> AsyncGenerator[Tuple[int, bytes], None]:
        Q = """
        SELECT vsn, pointer FROM vsn WHERE entity = :entity
        """
        res = self.database.iterate(Q, values={"entity": entity,})
        async for row in res:
            yield (row[0], row[1])

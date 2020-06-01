from typing import Optional, Union

class UnitTestRedis():
    def __init__(self):
        import fakeredis # type: ignore
        self.server = fakeredis.FakeServer()
        self.redis = fakeredis.FakeStrictRedis(server=self.server)

    async def set(self, key: bytes, value: bytes):
        return self.redis.set(key, value)

    async def get(self, key: bytes) -> Optional[bytes]:
        return self.redis.get(key)

    async def sadd(self, key: bytes, data: bytes):
        return self.redis.sadd(key, data)

    async def srem(self, key: bytes, data: bytes):
        return self.redis.srem(key, data)

    async def zadd(self, key: bytes, score: int, data: bytes):
        return self.redis.zadd(key, {data: score})

    async def zrem(self, key: bytes, data: bytes):
        return self.redis.zrem(key, data)

    async def lpush(self, key: bytes, data: bytes):
        return self.redis.lpush(key, data)

    async def lrange(self, key: bytes, start: int, end: int):
        return self.redis.lrange(key, start, end)

    async def zrangebyscore(self, key: bytes, start: Union[str, int], end: Union[str, int]):
        return self.redis.zrangebyscore(key, start, end)

class RedisWrap:
    def __init__(self, url: str):
        from aredis import StrictRedis # type: ignore
        self.redis = StrictRedis.from_url(url)

    async def set(self, key: bytes, value: bytes):
        return await self.redis.set(key, value)

    async def get(self, key: bytes) -> Optional[bytes]:
        return await self.redis.get(key)

    async def sadd(self, key: bytes, data: bytes):
        return await self.redis.sadd(key, data)

    async def srem(self, key: bytes, data: bytes):
        return await self.redis.srem(key, data)

    async def zadd(self, key: bytes, score: int, data: bytes):
        return await self.redis.zadd(key, score, data)

    async def zrem(self, key: bytes, data: bytes):
        return await self.redis.zrem(key, data)

    async def lpush(self, key: bytes, data: bytes):
        return await self.redis.lpush(key, data)

    async def lrange(self, key: bytes, start: int, end: int):
        return await self.redis.lrange(key, start, end)

    async def zrangebyscore(self, key: bytes, start: Union[str, int], end: Union[str, int]):
        return await self.redis.zrangebyscore(key, start, end)

def from_url(url: str):
    if url == "__unittest__":
        return UnitTestRedis()
    else:
        return RedisWrap(url)

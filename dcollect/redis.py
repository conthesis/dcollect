from typing import List, Optional, Union

RedisArg = Union[str, bytes]


class UnitTestRedis:
    def __init__(self) -> None:
        import fakeredis  # type: ignore

        self.server = fakeredis.FakeServer()
        self.redis = fakeredis.FakeStrictRedis(server=self.server)

    async def set(self, key: RedisArg, value: RedisArg) -> str:
        return self.redis.set(key, value)

    async def get(self, key: RedisArg) -> Optional[bytes]:
        return self.redis.get(key)

    async def sadd(self, key: RedisArg, data: RedisArg) -> int:
        return self.redis.sadd(key, data)

    async def srem(self, key: RedisArg, data: RedisArg) -> int:
        return self.redis.srem(key, data)

    async def zadd(self, key: RedisArg, score: int, data: RedisArg) -> int:
        return self.redis.zadd(key, {data: score})

    async def zrem(self, key: RedisArg, data: RedisArg) -> int:
        return self.redis.zrem(key, data)

    async def lpush(self, key: RedisArg, *data: List[RedisArg]) -> int:
        return self.redis.lpush(key, *data)

    async def rpush(self, key: RedisArg, *data: List[RedisArg]) -> int:
        return self.redis.lpush(key, *data)

    async def blpop(self, keys: List[RedisArg], timeout: int) -> List[bytes]:
        return self.redis.blpop(keys, timeout)

    async def lpop(self, key: RedisArg) -> Optional[bytes]:
        return self.redis.lpop(key)

    async def lrange(self, key: RedisArg, start: int, end: int) -> List[bytes]:
        return self.redis.lrange(key, start, end)

    async def zrangebyscore(
        self, key: RedisArg, start: Union[str, int], end: Union[str, int]
    ) -> List[bytes]:
        return self.redis.zrangebyscore(key, start, end)

    async def llen(self, key: RedisArg) -> int:
        return self.redis.llen(key)

    async def srandmember(self, name, number=1) -> List[bytes]:
        return self.redis.srandmember(name, number=number)


class RedisWrap:
    def __init__(self, url: str) -> None:
        from aredis import StrictRedis  # type: ignore

        self.redis = StrictRedis.from_url(url)

    async def set(self, key: RedisArg, value: RedisArg) -> str:
        return await self.redis.set(key, value)

    async def get(self, key: RedisArg) -> Optional[bytes]:
        return await self.redis.get(key)

    async def sadd(self, key: RedisArg, data: RedisArg) -> int:
        return await self.redis.sadd(key, data)

    async def srem(self, key: RedisArg, data: RedisArg) -> int:
        return await self.redis.srem(key, data)

    async def zadd(self, key: RedisArg, score: int, data: RedisArg) -> int:
        return await self.redis.zadd(key, score, data)

    async def zrem(self, key: RedisArg, data: RedisArg) -> int:
        return await self.redis.zrem(key, data)

    async def lpush(self, key: RedisArg, *data: List[RedisArg]) -> int:
        return await self.redis.lpush(key, *data)

    async def rpush(self, key: RedisArg, *data: List[RedisArg]) -> int:
        return await self.redis.lpush(key, *data)

    async def blpop(self, keys: List[RedisArg], timeout: int) -> List[bytes]:
        return await self.redis.blpop(keys, timeout)

    async def lpop(self, key: RedisArg) -> Optional[bytes]:
        return await self.redis.lpop(key)

    async def lrange(self, key: RedisArg, start: int, end: int) -> List[bytes]:
        return await self.redis.lrange(key, start, end)

    async def zrangebyscore(
        self, key: RedisArg, start: Union[str, int], end: Union[str, int]
    ) -> List[bytes]:
        return await self.redis.zrangebyscore(key, start, end)

    async def srandmember(self, name, number=1) -> List[bytes]:
        return await self.redis.srandmember(name, number=number)

    async def llen(self, key: bytes) -> int:
        return await self.redis.llen(key)


def from_url(url: str) -> Union[UnitTestRedis, RedisWrap]:
    if url == "__unittest__":
        return UnitTestRedis()
    else:
        return RedisWrap(url)

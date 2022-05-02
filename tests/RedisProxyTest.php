<?php

namespace RedisProxy\Tests;

use PHPUnit\Framework\TestCase;
use RedisProxy\RedisProxy;
use RedisProxy\RedisProxyException;

class RedisProxyTest extends TestCase
{
    public function testRedisDriver(): void
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        self::assertInstanceOf(RedisProxy::class, $redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_REDIS, $redisProxy->actualDriver());
    }

    public function testPredisDriver(): void
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        self::assertInstanceOf(RedisProxy::class, $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_PREDIS, $redisProxy->actualDriver());
    }

    public function testRedisDriverFirst(): void
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        self::assertInstanceOf(RedisProxy::class,
            $redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS, RedisProxy::DRIVER_PREDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_REDIS, $redisProxy->actualDriver());
    }

    public function testPredisDriverFirst(): void
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        self::assertInstanceOf(RedisProxy::class,
            $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS, RedisProxy::DRIVER_REDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_PREDIS, $redisProxy->actualDriver());
    }

    public function testNoDriverAvailable(): void
    {
        $this->expectExceptionMessage("No driver available");
        $this->expectException(RedisProxyException::class);
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        self::assertInstanceOf(RedisProxy::class, $redisProxy->setDriversOrder([]));
        self::assertNull($redisProxy->actualDriver());
        $redisProxy->flushall();
    }

    public function testUnsupportedDriverInDriversOrder(): void
    {
        $this->expectExceptionMessage("Driver \"unsupported_driver\" is not supported");
        $this->expectException(RedisProxyException::class);
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        $redisProxy->setDriversOrder(['unsupported_driver']);
    }
}

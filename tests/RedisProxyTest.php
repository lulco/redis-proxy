<?php

namespace RedisProxy\Tests;

use PHPUnit\Framework\TestCase;
use RedisProxy\RedisProxy;

class RedisProxyTest extends TestCase
{
    public function testRedisDriver()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        self::assertInstanceOf(RedisProxy::class, $redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_REDIS, $redisProxy->actualDriver());
    }

    public function testPredisDriver()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        self::assertInstanceOf(RedisProxy::class, $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_PREDIS, $redisProxy->actualDriver());
    }

    public function testRedisDriverFirst()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        self::assertInstanceOf(RedisProxy::class,
            $redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS, RedisProxy::DRIVER_PREDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_REDIS, $redisProxy->actualDriver());
    }

    public function testPredisDriverFirst()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        self::assertInstanceOf(RedisProxy::class,
            $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS, RedisProxy::DRIVER_REDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_PREDIS, $redisProxy->actualDriver());
    }

    public function testNoDriverAvailable()
    {
        $this->expectExceptionMessage("No driver available");
        $this->expectException(\RedisProxy\RedisProxyException::class);
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        self::assertInstanceOf(RedisProxy::class, $redisProxy->setDriversOrder([]));
        self::assertNull($redisProxy->actualDriver());
        $redisProxy->flushall();
    }

    public function testUnsupportedDriverInDriversOrder()
    {
        $this->expectExceptionMessage("Driver \"unsupported_driver\" is not supported");
        $this->expectException(\RedisProxy\RedisProxyException::class);
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        $redisProxy->setDriversOrder(['unsupported_driver']);
    }
}

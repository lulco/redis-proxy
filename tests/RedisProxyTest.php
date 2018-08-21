<?php

namespace RedisProxy\Tests;

use PHPUnit\Framework\TestCase;
use RedisProxy\RedisProxy;

class RedisProxyTest extends TestCase
{
    public function testRedisDriver()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        self::assertInstanceOf(RedisProxy::class, $redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_REDIS, $redisProxy->actualDriver());
    }

    public function testPredisDriver()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        self::assertInstanceOf(RedisProxy::class, $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_PREDIS, $redisProxy->actualDriver());
    }

    public function testRedisDriverFirst()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        self::assertInstanceOf(RedisProxy::class, $redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS, RedisProxy::DRIVER_PREDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_REDIS, $redisProxy->actualDriver());
    }

    public function testPredisDriverFirst()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        self::assertInstanceOf(RedisProxy::class, $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS, RedisProxy::DRIVER_REDIS]));
        self::assertNull($redisProxy->actualDriver());
        self::assertTrue($redisProxy->flushall());
        self::assertEquals(RedisProxy::DRIVER_PREDIS, $redisProxy->actualDriver());
    }

    /**
     * @expectedException \RedisProxy\RedisProxyException
     * @expectedExceptionMessage No driver available
     */
    public function testNoDriverAvailable()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        self::assertInstanceOf(RedisProxy::class, $redisProxy->setDriversOrder([]));
        self::assertNull($redisProxy->actualDriver());
        $redisProxy->flushall();
    }

    /**
     * @expectedException \RedisProxy\RedisProxyException
     * @expectedExceptionMessage Driver "unsupported_driver" is not supported
     */
    public function testUnsupportedDriverInDriversOrder()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        $redisProxy->setDriversOrder(['unsupported_driver']);
    }
}

<?php

namespace RedisProxy\Tests;

use PHPUnit_Framework_TestCase;
use RedisProxy\InfoHelper;
use RedisProxy\RedisProxy;

class InfoHelperTest extends PHPUnit_Framework_TestCase
{
    public function testCreateEmptyInfoArrayForRedis()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        $redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS]);
        $redisProxy->flushall();
        self::assertEquals(RedisProxy::DRIVER_REDIS, $redisProxy->actualDriver());
        self::assertEmpty(InfoHelper::createInfoArray($redisProxy, []));
    }

    public function testOnlyDatabasesInfoArrayForRedis()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        $redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS]);
        $redisProxy->flushall();
        self::assertEquals(RedisProxy::DRIVER_REDIS, $redisProxy->actualDriver());
        $info = InfoHelper::createInfoArray($redisProxy, [], 5);
        self::assertArrayHasKey('keyspace', $info);
        self::assertCount(5, $info['keyspace']);
        for ($i = 0; $i < 5; ++$i) {
            self::assertArrayHasKey("db$i", $info['keyspace']);
            self::assertEquals(0, $info['keyspace']["db$i"]['keys']);
            self::assertNull($info['keyspace']["db$i"]['expires']);
            self::assertNull($info['keyspace']["db$i"]['avg_ttl']);
        }
    }

    public function testCreateEmptyInfoArrayForPredis()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS]);
        $redisProxy->flushall();
        self::assertEquals(RedisProxy::DRIVER_PREDIS, $redisProxy->actualDriver());
        self::assertEmpty(InfoHelper::createInfoArray($redisProxy, []));
    }

    public function testOnlyDatabasesInfoArrayForPredis()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS]);
        $redisProxy->flushall();
        self::assertEquals(RedisProxy::DRIVER_PREDIS, $redisProxy->actualDriver());
        $info = InfoHelper::createInfoArray($redisProxy, [], 5);
        self::assertArrayHasKey('keyspace', $info);
        self::assertCount(5, $info['keyspace']);
        for ($i = 0; $i < 5; ++$i) {
            self::assertArrayHasKey("db$i", $info['keyspace']);
            self::assertEquals(0, $info['keyspace']["db$i"]['keys']);
            self::assertNull($info['keyspace']["db$i"]['expires']);
            self::assertNull($info['keyspace']["db$i"]['avg_ttl']);
        }
    }
}

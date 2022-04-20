<?php

namespace RedisProxy\Tests;

use RedisProxy\RedisProxy;

class RedisDriverTest extends BaseDriverTest
{
    protected function initializeDriver(): RedisProxy
    {
        if (!extension_loaded('redis')) {
            self::markTestSkipped('redis extension is not loaded');
        }
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        $redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS]);
        return $redisProxy;
    }
}

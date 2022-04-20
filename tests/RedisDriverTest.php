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
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        $redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS]);
        return $redisProxy;
    }
}

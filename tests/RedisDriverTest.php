<?php

namespace RedisProxy\Tests;

use RedisProxy\RedisProxy;

class RedisDriverTest extends BaseDriverTest
{
    protected function initializeDriver()
    {
        if (!extension_loaded('redis')) {
            self::markTestSkipped('redis extension is not loaded');
        }
        $this->redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'));
        $this->redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS]);
        $this->redisProxy->flushAll();
    }
}

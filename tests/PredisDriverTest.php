<?php

namespace RedisProxy\Tests;

use RedisProxy\RedisProxy;

class PredisDriverTest extends BaseDriverTest
{
    protected function initializeDriver()
    {
        if (!class_exists('Predis\Client')) {
            self::markTestSkipped('Predis client is not installed');
        }
        $this->redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'));
        $this->redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS]);
        $this->redisProxy->flushall();
    }
}

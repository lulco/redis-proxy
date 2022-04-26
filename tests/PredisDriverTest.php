<?php

namespace RedisProxy\Tests;

use RedisProxy\RedisProxy;

class PredisDriverTest extends BaseDriverTest
{
    protected function initializeDriver(): RedisProxy
    {
        if (!class_exists('Predis\Client')) {
            self::markTestSkipped('Predis client is not installed');
        }
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost', getenv('REDIS_PROXY_REDIS_PORT') ?: 6379, getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0);
        $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS]);
        return $redisProxy;
    }
}

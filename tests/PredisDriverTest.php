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
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'), getenv('REDIS_PROXY_REDIS_DATABASE'));
        $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS]);
        return $redisProxy;
    }
}

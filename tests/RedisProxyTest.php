<?php

namespace RedisProxy\Tests;

use PHPUnit_Framework_TestCase;
use RedisProxy\RedisProxy;

class RedisProxyTest extends PHPUnit_Framework_TestCase
{
    /**
     * @expectedException \RedisProxy\RedisProxyException
     * @expectedExceptionMessage No driver available
     */
    public function testNoDriverAvailable()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'));
        $redisProxy->setDriversOrder([]);
        $redisProxy->dbsize();
    }

    /**
     * @expectedException \RedisProxy\RedisProxyException
     * @expectedExceptionMessage Driver "unsupported_driver" is not supported
     */
    public function testUnsupportedDriverInDriversOrder()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'));
        $redisProxy->setDriversOrder(['unsupported_driver']);
    }
}

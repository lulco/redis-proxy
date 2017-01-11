<?php

namespace RedisProxy\Tests;

use PHPUnit_Framework_TestCase;
use RedisProxy\RedisProxy;

class RedisProxyTest extends PHPUnit_Framework_TestCase
{
    /**
     * @expectedException \RedisProxy\RedisProxyException
     * @expectedExceptionMessage You need to set at least one driver
     */
    public function testEmptyDriversOrder()
    {
        $redisProxy = new RedisProxy(getenv('REDIS_PROXY_REDIS_HOST'), getenv('REDIS_PROXY_REDIS_PORT'));
        $redisProxy->setDriversOrder([]);
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

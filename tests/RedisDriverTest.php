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

    // zpopmin is supported only for redis driver
    public function testZpopmin(): void
    {
        self::assertEquals(0, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertEquals([], $this->redisProxy->zpopmin('my_sorted_set_key'));
        self::assertEquals(3, $this->redisProxy->zadd('my_sorted_set_key', -1, 'element_1', 0, 'element_2', 1, 'element_3'));

        self::assertEquals(['element_1' => -1], $this->redisProxy->zpopmin('my_sorted_set_key'));
        self::assertEquals(['element_2' => 0], $this->redisProxy->zpopmin('my_sorted_set_key'));
        self::assertEquals(['element_3' => 1], $this->redisProxy->zpopmin('my_sorted_set_key'));

        self::assertEquals(3, $this->redisProxy->zadd('my_sorted_set_key', -1, 'element_1', 0, 'element_2', 1, 'element_3'));
        self::assertEquals(['element_1' => -1, 'element_2' => 0], $this->redisProxy->zpopmin('my_sorted_set_key', 2));
    }

    // zpopmax is supported only for redis driver
    public function testZpopmax(): void
    {
        self::assertEquals(0, $this->redisProxy->zcard('my_sorted_set_key'));
        self::assertEquals([], $this->redisProxy->zpopmax('my_sorted_set_key'));
        self::assertEquals(3, $this->redisProxy->zadd('my_sorted_set_key', -1, 'element_1', 0, 'element_2', 1, 'element_3'));

        self::assertEquals(['element_3' => 1], $this->redisProxy->zpopmax('my_sorted_set_key'));
        self::assertEquals(['element_2' => 0], $this->redisProxy->zpopmax('my_sorted_set_key'));
        self::assertEquals(['element_1' => -1], $this->redisProxy->zpopmax('my_sorted_set_key'));

        self::assertEquals(3, $this->redisProxy->zadd('my_sorted_set_key', -1, 'element_1', 0, 'element_2', 1, 'element_3'));
        self::assertEquals(['element_2' => 0, 'element_3' => 1], $this->redisProxy->zpopmax('my_sorted_set_key', 2));
    }
}

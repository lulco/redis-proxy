<?php

namespace RedisProxy\Tests;

use Override;
use RedisException;
use RedisProxy\ConnectionFactory\Serializers;
use RedisProxy\RedisProxy;
use RedisProxy\RedisProxyException;
use Throwable;
use function getenv;
use function ini_get;
use function microtime;
use function set_time_limit;

class RedisDriverTest extends BaseDriverTest
{
    protected function initializeDriver(): RedisProxy
    {
        if (!extension_loaded('redis')) {
            self::markTestSkipped('redis extension is not loaded');
        }
        $redisProxy = new RedisProxy(
            getenv('REDIS_PROXY_REDIS_HOST') ?: 'localhost',
            getenv('REDIS_PROXY_REDIS_PORT') ?: 6379,
            getenv('REDIS_PROXY_REDIS_DATABASE') ?: 0,
            (float)(getenv('REDIS_PROXY_REDIS_TIMEOUT') ?: 0.0),
            null,
            null,
            Serializers::NONE,
            (float)(getenv('REDIS_PROXY_REDIS_OPERATION_TIMEOUT')) ?: null,
        );
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

    #[Override]
    public function testHexpire(): void
    {
        self::markTestSkipped('HEXPIRE is not supported for RedisDriver');
    }

    public function testConnectionTimeout(): void
    {
        $time = microtime(true);

        self::expectException(RedisException::class);
        self::expectExceptionMessage('Connection timed out');

        try {
            $redisProxy = new RedisProxy(
                getenv('REDIS_PROXY_REDIS_FAKE_HOST') ?: '192.0.2.1',
                (int)(getenv('REDIS_PROXY_REDIS_PORT') ?: 6379),
                0,
                1.0
            );
            $redisProxy->setDriversOrder([RedisProxy::DRIVER_REDIS]);
            $redisProxy->info('server');
        } catch (RedisProxyException $e) {
            throw $e->getPrevious() ?? $e;
        }

        self::assertEqualsWithDelta(1.0, microtime(true) - $time, 0.1, 'Connection timeout duration is out of range');
    }

    public function testOperationTimeout(): void
    {
        set_time_limit(3); // not working?

        self::expectException(RedisException::class);
        self::expectExceptionMessageMatches('/^read error on connection to /');

        try {
            $this->redisProxy->blpop(['testlist'], 0);
        } catch (Throwable $e) {
            throw $e->getPrevious() ?? $e;
        }

        set_time_limit((int)ini_get('max_execution_time'));
    }
}

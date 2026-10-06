<?php

namespace RedisProxy\Tests;

use Composer\InstalledVersions;
use Override;
use Predis\Connection\ConnectionException;
use RedisProxy\ConnectionFactory\Serializers;
use RedisProxy\RedisProxy;
use RedisProxy\RedisProxyException;
use Throwable;
use function getenv;
use function ini_get;
use function microtime;
use function set_time_limit;

class PredisDriverTest extends BaseDriverTest
{
    protected function initializeDriver(): RedisProxy
    {
        if (!class_exists('Predis\Client')) {
            self::markTestSkipped('Predis client is not installed');
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
        $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS]);
        return $redisProxy;
    }

    #[Override]
    public function testHexpire(): void
    {
        $predisVersion = InstalledVersions::getVersion('predis/predis');
        if (version_compare($predisVersion, '2.0.0', '<')) {
            self::markTestSkipped('predis version < 2.0 does not support HEXPIRE');
        }
        $server = $this->redisProxy->info('server');
        if (version_compare($server['redis_version'], '7.0.0', '<') && !array_key_exists('dragonfly_version', $server)) {
            self::markTestSkipped('redis version < 7.0 does not support HEXPIRE');
        }
        parent::testHexpire();
    }

    public function testXaddXlenXrangeXdel(): void
    {
        $predisVersion = InstalledVersions::getVersion('predis/predis');
        if (version_compare($predisVersion, '2.0.0', '<')) {
            self::markTestSkipped('predis version < 2.0 does not support XADD');
        }
        parent::testHexpire();
    }

    public function testConnectionTimeout(): void
    {
        $time = microtime(true);

        self::expectException(ConnectionException::class);
        self::expectExceptionMessageMatches('/^Connection timed out \[.+\]$/');

        try {
            $redisProxy = new RedisProxy(
                getenv('REDIS_PROXY_REDIS_FAKE_HOST') ?: '192.0.2.1',
                (int)(getenv('REDIS_PROXY_REDIS_PORT') ?: 6379),
                0,
                1.0
            );
            $redisProxy->setDriversOrder([RedisProxy::DRIVER_PREDIS]);
            $redisProxy->info('server');
        } catch (RedisProxyException $e) {
            throw $e->getPrevious() ?? $e;
        }

        self::assertEqualsWithDelta(1.0, microtime(true) - $time, 0.1, 'Connection timeout duration is out of range');
    }

    public function testOperationTimeout(): void
    {
        set_time_limit(3); // not working?

        self::expectException(ConnectionException::class);
        self::expectExceptionMessageMatches('/^Error while reading line from the server. \[.+\]$/');

        try {
            $this->redisProxy->blpop(['testlist'], 0);
        } catch (Throwable $e) {
            throw $e->getPrevious() ?? $e;
        }

        set_time_limit((int)ini_get('max_execution_time'));
    }
}

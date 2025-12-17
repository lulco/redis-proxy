<?php

namespace RedisProxy\Tests;

use Composer\InstalledVersions;
use RedisProxy\RedisProxy;

class PredisDriverTest extends BaseDriverTestCase
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

    #[\Override]
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
}

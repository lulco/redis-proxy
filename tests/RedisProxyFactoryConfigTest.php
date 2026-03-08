<?php

namespace RedisProxy\Tests;

use PHPUnit\Framework\TestCase;
use RedisProxy\ConnectionFactory\Serializers;
use RedisProxy\ConnectionPoolFactory\SentinelConnectionPoolFactory;
use RedisProxy\ConnectionPoolFactory\SingleNodeConnectionPoolFactory;
use RedisProxy\RedisProxy;
use RedisProxy\RedisProxyFactory;
use ReflectionClass;

class RedisProxyFactoryConfigTest extends TestCase
{
    public function testSingleNodeConfigMapsAllOptionalParameters(): void
    {
        $config = [
            'host' => '10.0.0.1',
            'port' => 6380,
            'database' => 2,
            'timeout' => 1.25,
            'retryWait' => 250,
            'maxFails' => 4,
            'optSerializer' => Serializers::JSON,
            'operationTimeout' => 2.5,
            'connectMode' => RedisProxy::CONNECT_MODE_PCONNECT,
        ];

        $proxy = (new RedisProxyFactory())->createFromConfig($config);
        self::assertInstanceOf(RedisProxy::class, $proxy);

        $connectionPoolFactory = $this->readPrivate($proxy, 'connectionPoolFactory');
        self::assertInstanceOf(SingleNodeConnectionPoolFactory::class, $connectionPoolFactory);
        self::assertSame('10.0.0.1', $this->readPrivate($connectionPoolFactory, 'host'));
        self::assertSame(6380, $this->readPrivate($connectionPoolFactory, 'port'));
        self::assertSame(2, $this->readPrivate($connectionPoolFactory, 'database'));
        self::assertSame(1.25, $this->readPrivate($connectionPoolFactory, 'timeout'));
        self::assertSame(250, $this->readPrivate($connectionPoolFactory, 'retryWait'));
        self::assertSame(4, $this->readPrivate($connectionPoolFactory, 'maxFails'));
        self::assertSame(2.5, $this->readPrivate($connectionPoolFactory, 'operationTimeout'));
        self::assertSame(RedisProxy::CONNECT_MODE_PCONNECT, $this->readPrivate($connectionPoolFactory, 'connectMode'));
        self::assertSame(Serializers::JSON, $this->readPrivate($proxy, 'optSerializer'));
    }

    public function testSingleNodeConfigUsesDefaultsForMissingOptionalParameters(): void
    {
        $config = [
            'host' => '127.0.0.1',
            'port' => 6379,
        ];

        $proxy = (new RedisProxyFactory())->createFromConfig($config);
        $connectionPoolFactory = $this->readPrivate($proxy, 'connectionPoolFactory');

        self::assertInstanceOf(SingleNodeConnectionPoolFactory::class, $connectionPoolFactory);
        self::assertSame(0, $this->readPrivate($connectionPoolFactory, 'database'));
        self::assertSame(0.0, $this->readPrivate($connectionPoolFactory, 'timeout'));
        self::assertNull($this->readPrivate($connectionPoolFactory, 'retryWait'));
        self::assertNull($this->readPrivate($connectionPoolFactory, 'maxFails'));
        self::assertNull($this->readPrivate($connectionPoolFactory, 'operationTimeout'));
        self::assertSame(RedisProxy::CONNECT_MODE_CONNECT, $this->readPrivate($connectionPoolFactory, 'connectMode'));
        self::assertSame(Serializers::NONE, $this->readPrivate($proxy, 'optSerializer'));
    }

    public function testSentinelConfigMapsConnectModeAndTimeouts(): void
    {
        $config = [
            'sentinel' => [
                'sentinels' => [
                    ['host' => '10.0.0.11', 'port' => 26379],
                    ['host' => '10.0.0.12', 'port' => 26379],
                ],
                'clusterId' => 'mymaster',
                'database' => 3,
                'timeout' => 0.75,
                'retryWait' => 100,
                'maxFails' => 5,
                'writeToReplicas' => false,
                'operationTimeout' => 4.5,
                'connectMode' => RedisProxy::CONNECT_MODE_PCONNECT,
            ],
        ];

        $proxy = (new RedisProxyFactory())->createFromConfig($config);
        $connectionPoolFactory = $this->readPrivate($proxy, 'connectionPoolFactory');

        self::assertInstanceOf(SentinelConnectionPoolFactory::class, $connectionPoolFactory);
        self::assertSame('mymaster', $this->readPrivate($connectionPoolFactory, 'clusterId'));
        self::assertSame(3, $this->readPrivate($connectionPoolFactory, 'database'));
        self::assertSame(0.75, $this->readPrivate($connectionPoolFactory, 'timeout'));
        self::assertSame(100, $this->readPrivate($connectionPoolFactory, 'retryWait'));
        self::assertSame(5, $this->readPrivate($connectionPoolFactory, 'maxFails'));
        self::assertFalse($this->readPrivate($connectionPoolFactory, 'writeToReplicas'));
        self::assertSame(4.5, $this->readPrivate($connectionPoolFactory, 'operationTimeout'));
        self::assertSame(RedisProxy::CONNECT_MODE_PCONNECT, $this->readPrivate($connectionPoolFactory, 'connectMode'));
    }

    private function readPrivate(object $object, string $property)
    {
        $reflection = new ReflectionClass($object);
        $prop = $reflection->getProperty($property);
        $prop->setAccessible(true);
        return $prop->getValue($object);
    }
}

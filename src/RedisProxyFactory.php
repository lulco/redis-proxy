<?php

namespace RedisProxy;

use RedisProxy\ConnectionFactory\Serializers;

class RedisProxyFactory
{
    /**
     * <code>
     * Single node configuration
     * $config = [
     *      'host' => <string>, # mandatory
     *      'port' => <int>, # mandatory
     *      'database' => <int>, # optional (default: 0)
     *      'timeout' => <float>, # optional (default: 0.0)
     *      'retryWait' => <int|null>, # optional (default: null)
     *      'maxFails' => <int|null>, # optional (default: null)
     *      'operationTimeout' => <float|null>, # optional (default: null)
     *      'connectMode' => <string>, # optional ('connect' or 'pconnect', default: 'connect')
     *      'optSerializer' => <string>, # optional ('none', 'php', 'json', 'msgpack', 'igbinary')
     * ];
     *
     * Sentinel configuration
     * $config = [
     *      'sentinel' => [
     *          'sentinels' => <array> ['IP:PORT', 'IP:PORT', ...], # mandatory
     *          'clusterId' => <string>, # mandatory
     *          'database' => <int>, # optional (default: 0)
     *          'timeout' => <float>, # optional (default: 0.0)
     *          'retryWait' => <int|null>, # optional (default: null)
     *          'maxFails' => <int|null>, # optional (default: null)
     *          'writeToReplicas' => <bool>, # optional (default: false)
     *          'operationTimeout' => <float|null>, # optional (default: null)
     *          'connectMode' => <string>, # optional ('connect' or 'pconnect', default: 'connect')
     *      ]
     * ];
     * </code>
     *
     * @throws RedisProxyException
     */
    public function createFromConfig(array $config): RedisProxy
    {
        if (array_key_exists('sentinel', $config)) {
            $sentinelConfig = $config['sentinel'];
            $proxy = new RedisProxy();
            $proxy->setSentinelConnectionPool(
                $sentinelConfig['sentinels'],
                $sentinelConfig['clusterId'],
                array_key_exists('database', $sentinelConfig) ? $sentinelConfig['database'] : 0,
                array_key_exists('timeout', $sentinelConfig) ? $sentinelConfig['timeout'] : 0.0,
                array_key_exists('retryWait', $sentinelConfig) ? $sentinelConfig['retryWait'] : null,
                array_key_exists('maxFails', $sentinelConfig) ? $sentinelConfig['maxFails'] : null,
                array_key_exists('writeToReplicas', $sentinelConfig) ? $sentinelConfig['writeToReplicas'] : true,
                array_key_exists('operationTimeout', $sentinelConfig) ? $sentinelConfig['operationTimeout'] : null,
                array_key_exists('connectMode', $sentinelConfig) ? $sentinelConfig['connectMode'] : RedisProxy::CONNECT_MODE_CONNECT,
            );
            return $proxy;
        }

        if (array_key_exists('host', $config) && array_key_exists('port', $config)) {
            return new RedisProxy(
                $config['host'],
                $config['port'],
                array_key_exists('database', $config) ? $config['database'] : 0,
                array_key_exists('timeout', $config) ? $config['timeout'] : 0,
                array_key_exists('retryWait', $config) ? $config['retryWait'] : null,
                array_key_exists('maxFails', $config) ? $config['maxFails'] : null,
                array_key_exists('optSerializer', $config) ? $config['optSerializer'] : Serializers::NONE,
                array_key_exists('operationTimeout', $config) ? $config['operationTimeout'] : null,
                array_key_exists('connectMode', $config) ? $config['connectMode'] : RedisProxy::CONNECT_MODE_CONNECT
            );
        }

        throw new RedisProxyException('Wrong configuration');
    }
}

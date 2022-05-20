<?php

namespace RedisProxy;

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
     *      ]
     * ];
     * </code>
     *
     * @throws RedisProxyException
     */
    public function createFromConfig(array $config): RedisProxy
    {
        if (array_key_exists('sentinel', $config)) {
            if (count($config) > 1) {
                throw new RedisProxyException('Wrong "sentinel" configuration');
            }

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
            );
            return $proxy;
        }

        if (array_key_exists('host', $config) && array_key_exists('port', $config)) {
            return new RedisProxy(
                $config['host'],
                $config['port'],
                array_key_exists('database', $config) ? $config['database'] : 0,
                array_key_exists('timeout', $config) ? $config['timeout'] : 0
            );
        }

        throw new RedisProxyException('Wrong configuration');
    }
}

<?php

namespace RedisProxy;

class RedisProxyFactory
{
    /**
     * <code>
     * $config = [
     *      'single' => [
     *          'host' => <string>, # mandatory
     *          'port' => <int>, # mandatory
     *          'database' => <int>, # optional (default: 0)
     *          'timeout' => <float>, # optional (default: 0.0)
     *      ],
     *      'sentinel' => [
     *          'sentinels' => <array> ['IP:PORT', 'IP:PORT', ...], # mandatory
     *          'clusterId' => <string>, # mandatory
     *          'database' => <int>, # optional (default: 0)
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
        if (array_key_exists('single', $config)) {
            $singleConfig = $config['single'];
            return new RedisProxy(
                $singleConfig['host'],
                $singleConfig['port'],
                array_key_exists('database', $singleConfig) ? $singleConfig['database'] : 0,
                array_key_exists('timeout', $singleConfig) ? $singleConfig['timeout'] : 0
            );
        }

        if (array_key_exists('sentinel', $config)) {
            $sentinelConfig = $config['sentinel'];
            $proxy = new RedisProxy();
            $proxy->setSentinelConnectionPool(
                $sentinelConfig['sentinels'],
                $sentinelConfig['clusterId'],
                array_key_exists('database', $sentinelConfig) ? $sentinelConfig['database'] : 0,
                array_key_exists('retryWait', $sentinelConfig) ? $sentinelConfig['retryWait'] : null,
                array_key_exists('maxFails', $sentinelConfig) ? $sentinelConfig['maxFails'] : null,
                array_key_exists('writeToReplicas', $sentinelConfig) ? $sentinelConfig['writeToReplicas'] : true,
            );
            return $proxy;
        }

        throw new RedisProxyException('Wrong configuration');
    }
}

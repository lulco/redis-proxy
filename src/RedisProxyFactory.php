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
     * @param array<mixed> $config
     * @throws RedisProxyException
     */
    public function createFromConfig(array $config): RedisProxy
    {
        if (array_key_exists('sentinel', $config)) {
            /** @var array<mixed> $sentinelConfig */
            $sentinelConfig = $config['sentinel'];
            $proxy = new RedisProxy();
            /** @var array<array{host: string, port: int}> $sentinels */
            $sentinels = $sentinelConfig['sentinels'];
            /** @var string $clusterId */
            $clusterId = $sentinelConfig['clusterId'];
            $database = array_key_exists('database', $sentinelConfig) && is_int($sentinelConfig['database']) ? $sentinelConfig['database'] : 0;
            $timeout = array_key_exists('timeout', $sentinelConfig) && (is_float($sentinelConfig['timeout']) || is_int($sentinelConfig['timeout'])) ? (float)$sentinelConfig['timeout'] : 0.0;
            $retryWait = array_key_exists('retryWait', $sentinelConfig) && is_int($sentinelConfig['retryWait']) ? $sentinelConfig['retryWait'] : null;
            $maxFails = array_key_exists('maxFails', $sentinelConfig) && is_int($sentinelConfig['maxFails']) ? $sentinelConfig['maxFails'] : null;
            $writeToReplicas = array_key_exists('writeToReplicas', $sentinelConfig) ? (bool)$sentinelConfig['writeToReplicas'] : true;

            $proxy->setSentinelConnectionPool(
                $sentinels,
                $clusterId,
                $database,
                $timeout,
                $retryWait,
                $maxFails,
                $writeToReplicas,
            );
            return $proxy;
        }

        if (array_key_exists('host', $config) && array_key_exists('port', $config)) {
            $host = is_string($config['host']) ? $config['host'] : '127.0.0.1';
            $port = is_int($config['port']) ? $config['port'] : 6379;
            $database = array_key_exists('database', $config) && is_int($config['database']) ? $config['database'] : 0;
            $timeout = array_key_exists('timeout', $config) && (is_float($config['timeout']) || is_int($config['timeout'])) ? (float)$config['timeout'] : 0.0;

            return new RedisProxy($host, $port, $database, $timeout);
        }

        throw new RedisProxyException('Wrong configuration');
    }
}

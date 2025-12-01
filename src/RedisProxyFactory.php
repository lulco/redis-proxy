<?php

declare(strict_types=1);

namespace RedisProxy;

class RedisProxyFactory
{
    /**
     * <code>
     * Single node configuration
     * $config = [
     *     'host' => <string>, // mandatory
     *     'port' => <int>, // mandatory
     *     'database' => <int>, // optional (default: 0)
     *     'timeout' => <float>, // optional (default: 0.0)
     * ];
     *
     * Sentinel configuration
     * $config = [
     *     'sentinel' => [
     *         'sentinels' => <array> ['IP:PORT', 'IP:PORT', ...], // mandatory
     *         'clusterId' => <string>, // mandatory
     *         'database' => <int>, // optional (default: 0)
     *         'timeout' => <float>, // optional (default: 0.0)
     *         'retryWait' => <int|null>, // optional (default: null)
     *         'maxFails' => <int|null>, // optional (default: null)
     *         'writeToReplicas' => <bool>, // optional (default: false)
     *     ],
     * ];
     * </code>
     *
     * @param array{
     *     host: string,
     *     port: int,
     *     database?: int,
     *     timeout?: float
     * }|array{
     *     sentinel: array{
     *         sentinels: array<string>,
     *         clusterId: string,
     *         database?: int,
     *         timeout?: float,
     *         retryWait?: int|null,
     *         maxFails?: int|null,
     *         writeToReplicas?: bool
     *     }
     * } $config
     * @throws RedisProxyException
     */
    public function createFromConfig(array $config): RedisProxy
    {
        if (array_key_exists('sentinel', $config)) {
            $sentinelConfig = $config['sentinel'];

            /** @var array<int, string> $sentinelStrings */
            $sentinelStrings = $sentinelConfig['sentinels'];

            /** @var list<array{host: string, port: int}> $sentinels */
            $sentinels = [];
            foreach ($sentinelStrings as $s) {
                if (!is_string($s) || strpos($s, ':') === false) {
                    throw new RedisProxyException('Invalid sentinel address format, expected "host:port".');
                }
                [$host, $port] = explode(':', $s, 2);
                $sentinels[] = [
                    'host' => $host,
                    'port' => (int) $port,
                ];
            }

            $proxy = new RedisProxy();
            $proxy->setSentinelConnectionPool(
                $sentinels,
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

        /** @phpstan-ignore-next-line  Unreachable by documented config union, but kept for runtime safety */
        throw new RedisProxyException('Wrong configuration');
    }
}

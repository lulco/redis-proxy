<?php

declare(strict_types=1);

namespace RedisProxy;

class InfoHelper
{
    /** @var array<string, string> */
    private static array $keyStartToSectionMap = [
        'redis_' => 'server',
        'uptime_' => 'server',
        'client_' => 'clients',
        'used_memory' => 'memory',
        'mem_' => 'memory',
        'rdb_' => 'persistence',
        'aof_' => 'persistence',
        'total_' => 'stats',
        'sync_' => 'stats',
        'keyspace_' => 'stats',
        'pubsub_' => 'stats',
        'repl_backlog_' => 'replication',
        'used_cpu_' => 'cpu',
        'db' => 'keyspace',
    ];

    /** @var array<string, string> */
    private static array $keyToSectionMap = [
        'os' => 'server',
        'arch_bits' => 'server',
        'multiplexing_api' => 'server',
        'gcc_version' => 'server',
        'process_id' => 'server',
        'run_id' => 'server',
        'tcp_port' => 'server',
        'hz' => 'server',
        'lru_clock' => 'server',
        'config_file' => 'server',
        'connected_clients' => 'clients',
        'blocked_clients' => 'clients',
        'loading' => 'persistence',
        'instantaneous_ops_per_sec' => 'stats',
        'rejected_connections' => 'stats',
        'expired_keys' => 'stats',
        'evicted_keys' => 'stats',
        'latest_fork_usec' => 'stats',
        'role' => 'replication',
        'connected_slaves' => 'replication',
        'master_repl_offset' => 'replication',
    ];

    /**
     * @param array<string, mixed> $result
     * @return array<string, mixed>
     */
    public static function createInfoArray(RedisProxy $redisProxy, array $result, ?int $databases = null): array
    {
        $groupedResult = self::initializeKeyspace($databases);
        if ($redisProxy->actualDriver() === RedisProxy::DRIVER_PREDIS) {
            return self::createInfoForPredis($result, $groupedResult);
        }

        return self::createInfoForRedis($result, $groupedResult);
    }

    /**
     * @return array<string, mixed>
     */
    private static function initializeKeyspace(?int $databases = null): array
    {
        $groupedResult = [];
        if ($databases === null) {
            return $groupedResult;
        }
        $groupedResult['keyspace'] = [];
        for ($db = 0; $db < $databases; ++$db) {
            $groupedResult['keyspace']["db$db"] = [
                'keys' => 0,
                'expires' => null,
                'avg_ttl' => null,
            ];
        }
        return $groupedResult;
    }

    /**
     * @param array<string, mixed> $result
     * @param array<string, mixed> $groupedResult
     * @return array<string, mixed>
     */
    private static function createInfoForPredis(array $result, array $groupedResult): array
    {
        $result = array_change_key_case($result, CASE_LOWER);
        if (isset($groupedResult['keyspace']) && isset($result['keyspace'])) {
            $groupedResult['keyspace'] = array_merge($groupedResult['keyspace'], $result['keyspace']);
            unset($result['keyspace']);
        }
        return array_merge($groupedResult, $result);
    }

    /**
     * @param array<string, mixed> $result
     * @param array<string, mixed> $groupedResult
     * @return array<string, mixed>
     */
    private static function createInfoForRedis(array $result, array $groupedResult): array
    {
        foreach ($result as $key => $value) {
            if (isset(self::$keyToSectionMap[$key])) {
                $groupedResult[self::$keyToSectionMap[$key]][$key] = $value;
                continue;
            }

            foreach (self::$keyStartToSectionMap as $keyStart => $targetSection) {
                if (str_starts_with($key, $keyStart) && $keyStart === 'db') {
                    $value = self::createKeyspaceInfo($value);
                }
                if (str_starts_with($key, $keyStart)) {
                    $groupedResult[$targetSection][$key] = $value;
                    continue;
                }
            }
        }
        return $groupedResult;
    }

    /**
     * @return array{keys: int, expires: int|null, avg_ttl: int|null}
     */
    private static function createKeyspaceInfo(string $keyspaceInfo): array
    {
        [$keys, $expires, $avgTtl] = explode(',', $keyspaceInfo);
        return [
            'keys' => str_contains($keys, '=') ? (int) explode('=', $keys)[1] : 0,
            'expires' => str_contains($expires, '=') ? (int) explode('=', $expires)[1] : null,
            'avg_ttl' => str_contains($avgTtl, '=') ? (int) explode('=', $avgTtl)[1] : null,
        ];
    }
}

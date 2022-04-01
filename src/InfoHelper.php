<?php

namespace RedisProxy;

class InfoHelper
{
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
     * @param RedisProxy $redisProxy
     * @param array $result
     * @param integer|null $databases
     * @return array
     */
    public static function createInfoArray(RedisProxy $redisProxy, array $result, ?int $databases = null): array
    {
        $groupedResult = self::initializeKeyspace($databases);
        if ($redisProxy->actualDriver() === RedisProxy::DRIVER_PREDIS) {
            return self::createInfoForPredis($result, $groupedResult);
        }

        return self::createInfoForRedis($result, $groupedResult);
    }

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

    private static function createInfoForPredis(array $result, array $groupedResult): array
    {
        $result = array_change_key_case($result, CASE_LOWER);
        if (isset($groupedResult['keyspace']) && isset($result['keyspace'])) {
            $groupedResult['keyspace'] = array_merge($groupedResult['keyspace'], $result['keyspace']);
            unset($result['keyspace']);
        }
        return array_merge($groupedResult, $result);
    }

    private static function createInfoForRedis(array $result, array $groupedResult): array
    {
        foreach ($result as $key => $value) {
            if (isset(self::$keyToSectionMap[$key])) {
                $groupedResult[self::$keyToSectionMap[$key]][$key] = $value;
                continue;
            }

            foreach (self::$keyStartToSectionMap as $keyStart => $targetSection) {
                if (strpos($key, $keyStart) === 0 && $keyStart === 'db') {
                    $value = self::createKeyspaceInfo($value);
                }
                if (strpos($key, $keyStart) === 0) {
                    $groupedResult[$targetSection][$key] = $value;
                    continue;
                }
            }
        }
        return $groupedResult;
    }

    private static function createKeyspaceInfo($keyspaceInfo): array
    {
        [$keys, $expires, $avgTtl] = explode(',', $keyspaceInfo);
        return [
            'keys' => strpos($keys, '=') !== false ? explode('=', $keys)[1] : 0,
            'expires' => strpos($expires, '=') !== false ? explode('=', $expires)[1] : null,
            'avg_ttl' => strpos($avgTtl, '=') !== false ? explode('=', $avgTtl)[1] : null,
        ];
    }
}

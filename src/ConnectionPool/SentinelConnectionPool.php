<?php

namespace RedisProxy\ConnectionPool;

use RedisProxy\ConnectionPoolFactory\SingleNodeConnectionPoolFactory;
use RedisProxy\Driver\Driver;
use RedisProxy\RedisProxyException;
use Throwable;

class SentinelConnectionPool implements ConnectionPool
{
    private Driver $driver;

    private array $sentinels;

    private string $clusterId;

    private int $database;

    private float $timeout;

    private $masterConnection = null;

    private array $replicas = [];

    private array $replicasConnection = [];

    private int $failedCount = 0;

    private int $maxFails = 3;

    private float $retryWait = 1000;

    public function __construct(Driver $driver, array $sentinels, string $clusterId, int $database = 0, float $timeout = 0.0)
    {
        shuffle($sentinels);

        $this->driver = $driver;
        $this->sentinels = $sentinels;
        $this->clusterId = $clusterId;
        $this->database = $database;
        $this->timeout = $timeout;
    }

    public function setRetryWait(int $retryWait): SentinelConnectionPool
    {
        $this->retryWait = $retryWait;
        return $this;
    }

    public function setMaxFails(int $maxFails): SentinelConnectionPool
    {
        $this->maxFails = $maxFails;
        return $this;
    }

    public function getConnection(string $command)
    {
        if ($this->getMasterConnection() === null) {
            if (!$this->loadMasterReplicasDataFromSentinel()) {
                throw new RedisProxyException('Cannot load or estabilis connection to master/replicas from sentinel configuration');
            }
        }

        if (in_array($command, $this->getReadOnlyOperations())) {
            $connection = $this->getReplicaConnection();
            return $connection ?? $this->getMasterConnection();
        }

        return $this->getMasterConnection();
    }

    public function handleFailed(): bool
    {
        $this->failedCount++;
        $result = $this->loadMasterReplicasDataFromSentinel();

        if ($result === true) {
            return true;
        }

        usleep($this->retryWait * 1000);
        return $this->failedCount < $this->maxFails;
    }

    private function loadMasterReplicasDataFromSentinel(): bool
    {
        $this->reset();

        $sentinels = $this->sentinels;
        foreach ($sentinels as $sentinel) {
            $sentinelConnection = $this->driver->getDriverFactory()->create(new SingleNodeConnectionPoolFactory($sentinel['host'], $sentinel['port'], 0, 0.0, false));

            try {
                $masterData = $sentinelConnection->callSentinel('get-master-addr-by-name', [$this->clusterId]);
                $replicasData = $sentinelConnection->callSentinel('replicas', [$this->clusterId]);
            } catch (Throwable $e) {
                $this->shiftSentinels();
                continue;
            }

            try {
                $this->masterConnection = $this->driver->getConnectionFactory()->create($masterData[0], $masterData[1], $this->timeout);
                $this->masterConnection->select($this->database);
                $role = $this->driver->connectionRole($this->masterConnection);
                if ($role !== 'master') {
                    $this->reset();
                    continue;
                }
            } catch (Throwable $t) {
                continue;
            }
            foreach ($replicasData as $replicaData) {
                $normalizedRepolicaData = $this->normalizeResponze($replicaData);
                if (isset($normalizedRepolicaData['flags']) &&
                    !array_intersect(explode(',', $normalizedRepolicaData['flags']), ['s_down', 'o_down', 'disconnected']) &&
                    !empty($normalizedRepolicaData['ip']) &&
                    !empty($normalizedRepolicaData['port'])
                ) {
                    $replicaKey = $normalizedRepolicaData['ip'] . ':' . $normalizedRepolicaData['port'];
                    if (isset($this->replicasConnection[$replicaKey])) {
                        continue;
                    }

                    $this->replicas[$replicaKey] = [
                        'ip' => $normalizedRepolicaData['ip'],
                        'port' => $normalizedRepolicaData['port'],
                    ];
                }
            }
            $this->failedCount = 0;
            $sentinelConnection->call('close');
            return true;
        }

        return false;
    }

    private function getMasterConnection()
    {
        return $this->masterConnection;
    }

    private function getReplicaConnection()
    {
        if (count($this->replicas) > 0) {
            while ($replica = array_shift($this->replicas)) {
                try {
                    $replicaConnection = $this->driver->getConnectionFactory()->create($replica['ip'], $replica['port'], $this->timeout);
                    $replicaConnection->select($this->database);

                    $role = $this->driver->connectionRole($replicaConnection);
                    if ($role !== 'slave') {
                        continue;
                    }

                    $this->replicasConnection[] = $replicaConnection;
                    return $replicaConnection;
                } catch (Throwable $t) {
                    continue;
                }
            }
        }

        if (count($this->replicasConnection) === 0) {
            return null;
        }

        return $this->replicasConnection[array_rand($this->replicasConnection)];
    }

    private function shiftSentinels(): void
    {
        $sentinel = array_shift($this->sentinels);
        $this->sentinels[] = $sentinel;
    }

    private function reset(): void
    {
        $this->masterConnection = null;
        $this->replicas = [];
        $this->replicasConnection = [];
    }

    private function normalizeResponze(array $arr): array
    {
        $keys = array_values(array_filter($arr, function ($key) {
            return $key % 2 == 0;
        }, ARRAY_FILTER_USE_KEY));
        $values = array_values(array_filter($arr, function ($key) {
            return $key % 2 == 1;
        }, ARRAY_FILTER_USE_KEY));

        if (count($keys) != count($values)) {
            throw new RedisProxyException("Wrong number of arguments");
        }
        return array_combine($keys, $values);
    }
    
    private function getReadOnlyOperations(): array
    {
        return [
            'exists',
            'type',
            'keys',
            'scan',
            'randomkey',
            'ttl',
            'get',
            'mget',
            'substr',
            'strlen',
            'getrange',
            'getbit',
            'llen',
            'lrange',
            'lindex',
            'scard',
            'sismemer',
            'sinter',
            'sunion',
            'sdiff',
            'smembers',
            'sscan',
            'srandommember',
            'zrange',
            'zrevrange',
            'zrangebyscore',
            'zrevrangebyscore',
            'zcard',
            'zscore',
            'zcount',
            'zrank',
            'zrevrank',
            'zscan',
            'zlexcount',
            'zrangebylex',
            'zrevrangebylex',
            'hget',
            'hmget',
            'hexists',
            'hlen',
            'hkeys',
            'hvals',
            'hgetall',
            'hscan',
            'hstrlen',
            'ping',
            'auth',
            'select',
            'echo',
            'quit',
            'object',
            'bitcount',
            'bitpos',
            'time',
            'pfcount',
            'geohash',
            'geopos',
            'geodist',
        ];
    }
}

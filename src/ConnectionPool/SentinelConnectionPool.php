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

    private int $retryWait = 1000;

    private bool $writeToReplicas = true;

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

    public function setWriteToReplicas(bool $writeToReplicas): SentinelConnectionPool
    {
        $this->writeToReplicas = $writeToReplicas;
        return $this;
    }

    /**
     * @throws RedisProxyException
     */
    public function getConnection(string $command)
    {
        if ($this->getMasterConnection() === null) {
            if (!$this->loadMasterReplicasDataFromSentinel()) {
                throw new RedisProxyException('Cannot load or establish connection to master/replicas from sentinel configuration');
            }
        }

        if ($this->writeToReplicas && in_array($command, $this->getReadOnlyOperations())) {
            $connection = $this->getReplicaConnection();
            return $connection ?? $this->getMasterConnection();
        }

        return $this->getMasterConnection();
    }

    public function resetConnection(): void
    {
        $this->reset();
    }

    public function handleFailed(int $attempt): bool
    {
        $this->failedCount++;
        $result = $this->loadMasterReplicasDataFromSentinel();

        if ($result === true) {
            return true;
        }

        usleep($this->retryWait * 1000);
        return $this->failedCount < $this->maxFails;
    }

    /**
     * @throws RedisProxyException
     */
    private function loadMasterReplicasDataFromSentinel(): bool
    {
        $this->reset();

        $sentinels = $this->sentinels;
        foreach ($sentinels as $sentinel) {
            try {
                $sentinelConnection = $this->driver->getDriverFactory()->create(new SingleNodeConnectionPoolFactory($sentinel['host'], $sentinel['port'], 0, 0.0, false));
                $masterData = $sentinelConnection->callSentinel('get-master-addr-by-name', [$this->clusterId]);
                $replicasData = $sentinelConnection->callSentinel('replicas', [$this->clusterId]);
            } catch (Throwable $e) {
                $this->shiftSentinels();
                continue;
            }

            try {
                $this->masterConnection = $this->driver->getConnectionFactory()->create($masterData[0], $masterData[1], $this->timeout);
                $this->driver->connectionSelect($this->masterConnection, $this->database);
                $role = $this->driver->connectionRole($this->masterConnection);
                if ($role !== 'master') {
                    $this->reset();
                    continue;
                }
            } catch (RedisProxyException $e) {
                throw $e;
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

    /**
     * @throws RedisProxyException
     */
    private function getReplicaConnection()
    {
        if ($this->replicas !== []) {
            while ($replica = array_shift($this->replicas)) {
                try {
                    $replicaConnection = $this->driver->getConnectionFactory()->create($replica['ip'], $replica['port'], $this->timeout);
                    $this->driver->connectionSelect($replicaConnection, $this->database);

                    $role = $this->driver->connectionRole($replicaConnection);
                    if ($role !== 'slave') {
                        continue;
                    }

                    $this->replicasConnection[] = $replicaConnection;
                    return $replicaConnection;
                } catch (RedisProxyException $e) {
                    throw $e;
                } catch (Throwable $t) {
                    continue;
                }
            }
        }

        if ($this->replicasConnection === []) {
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
        $keys = array_values(array_filter($arr, fn($key): bool => $key % 2 == 0, ARRAY_FILTER_USE_KEY));
        $values = array_values(array_filter($arr, fn($key): bool => $key % 2 == 1, ARRAY_FILTER_USE_KEY));

        if (count($keys) != count($values)) {
            throw new RedisProxyException('Wrong number of arguments');
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

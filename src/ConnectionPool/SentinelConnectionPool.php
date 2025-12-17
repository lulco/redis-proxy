<?php

namespace RedisProxy\ConnectionPool;

use Predis\Client;
use RedisProxy\ConnectionPoolFactory\SingleNodeConnectionPoolFactory;
use RedisProxy\Driver\Driver;
use RedisProxy\RedisProxyException;
use Throwable;

class SentinelConnectionPool implements ConnectionPool
{
    private Driver $driver;

    /** @var array<array{host: string, port: int}> */
    private array $sentinels;

    private string $clusterId;

    private int $database;

    private float $timeout;

    private \Redis|Client|null $masterConnection = null;

    /** @var array<string, array{ip: string, port: int}> */
    private array $replicas = [];

    /** @var array<mixed> */
    private array $replicasConnection = [];

    private int $failedCount = 0;

    private int $maxFails = 3;

    private int $retryWait = 1000;

    private bool $writeToReplicas = true;

    /**
     * @param array<array{host: string, port: int}> $sentinels
     */
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
    public function getConnection(string $command): mixed
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
                /** @var array{0: string, 1: int} $masterData */
                $this->masterConnection = $this->driver->getConnectionFactory()->create((string)$masterData[0], (int)$masterData[1], $this->timeout);
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
            /** @var array<mixed> $replicasData */
            foreach ($replicasData as $replicaData) {
                /** @var array<mixed> $replicaData */
                $normalizedRepolicaData = $this->normalizeResponze($replicaData);
                if (isset($normalizedRepolicaData['flags']) &&
                    is_string($normalizedRepolicaData['flags']) &&
                    !array_intersect(explode(',', $normalizedRepolicaData['flags']), ['s_down', 'o_down', 'disconnected']) &&
                    !empty($normalizedRepolicaData['ip']) &&
                    is_string($normalizedRepolicaData['ip']) &&
                    !empty($normalizedRepolicaData['port']) &&
                    is_int($normalizedRepolicaData['port'])
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

    private function getMasterConnection(): \Redis|Client|null
    {
        return $this->masterConnection;
    }

    /**
     * @throws RedisProxyException
     */
    private function getReplicaConnection(): mixed
    {
        if (count($this->replicas) > 0) {
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

        if (count($this->replicasConnection) === 0) {
            return null;
        }

        return $this->replicasConnection[array_rand($this->replicasConnection)];
    }

    private function shiftSentinels(): void
    {
        $sentinel = array_shift($this->sentinels);
        if ($sentinel !== null) {
            $this->sentinels[] = $sentinel;
        }
    }

    private function reset(): void
    {
        $this->masterConnection = null;
        $this->replicas = [];
        $this->replicasConnection = [];
    }

    /**
     * @param array<mixed> $arr
     * @return array<string, mixed>
     */
    private function normalizeResponze(array $arr): array
    {
        $keys = array_values(array_filter($arr, function ($key) {
            return $key % 2 == 0;
        }, ARRAY_FILTER_USE_KEY));
        $values = array_values(array_filter($arr, function ($key) {
            return $key % 2 == 1;
        }, ARRAY_FILTER_USE_KEY));

        if (count($keys) != count($values)) {
            throw new RedisProxyException('Wrong number of arguments');
        }
        /** @var array<string> $stringKeys */
        $stringKeys = [];
        foreach ($keys as $key) {
            if (is_string($key)) {
                $stringKeys[] = $key;
            } else {
                /** @var int|float|bool|null $key */
                $stringKeys[] = (string)$key;
            }
        }
        /** @var array<string, mixed> */
        return array_combine($stringKeys, $values);
    }

    /**
     * @return array<string>
     */
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

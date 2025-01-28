<?php

namespace RedisProxy\ConnectionPool;

use Redis;
use RedisProxy\Driver\Driver;
use RedisProxy\RedisProxyException;
use Throwable;
use Tracy\Debugger;

class MultiConnectionPool implements ConnectionPool
{
    public const MAX_FAILS = 3;

    public const RETRY_WAIT = 1000;

    private const MICRO_TO_SECONDS = 1000;

    /**
     * @var array{host: string, port: int} $master
     */
    private array $master;

    /**
     * @var array{array{host: string, port: int}} $slaves
     */
    private array $slaves;

    private int $database;

    private float $timeout;

    private Driver $driver;

    private ?Redis $masterConnection = null;

    /**
     * @var Redis[] $slavesConnection
     */
    private array $slavesConnection = [];

    private int $failedCount = 0;

    private int $maxFails = self::MAX_FAILS;

    private int $retryWait = self::RETRY_WAIT;

    private bool $writeToReplicas = true;

    /**
     * @param array{host: string, port: int} $master
     * @param array{array{host: string, port: int}} $slaves
     */
    public function __construct(Driver $driver, array $master, array $slaves, int $database = 0, float $timeout = 0.0)
    {
        $this->driver = $driver;
        $this->master = $master;
        $this->slaves = $slaves;
        $this->database = $database;
        $this->timeout = $timeout;
    }

    public function setRetryWait(int $retryWait): MultiConnectionPool
    {
        $this->retryWait = $retryWait;
        return $this;
    }

    public function setMaxFails(int $maxFails): MultiConnectionPool
    {
        $this->maxFails = $maxFails;
        return $this;
    }

    public function setWriteToReplicas(bool $writeToReplicas): MultiConnectionPool
    {
        $this->writeToReplicas = $writeToReplicas;
        return $this;
    }

    /**
     * @throws RedisProxyException
     */
    public function getConnection(string $command): Redis|null
    {
        if ($this->getMasterConnection() === null) {
            if (!$this->loadConnections()) {
                throw new RedisProxyException('Cannot load or establish connection to master/replicas from configuration');
            }
        }

        if ($this->writeToReplicas && in_array($command, $this->getReadOnlyOperations(), true)) {
            return $this->getReplicaConnection();
        }
        Debugger::log('Using master connection', 'debug');
        return $this->getMasterConnection();
    }

    /**
     * @throws RedisProxyException
     */
    private function loadConnections(): bool
    {
        $this->reset();
        // load master
        try {
            $this->masterConnection = $this->driver->getConnectionFactory()->create($this->master['host'], $this->master['port'], $this->timeout);
        } catch (RedisProxyException $e) {
            throw $e;
        } catch (Throwable $t) {

        }
        // load replicas
        if ($this->writeToReplicas) {
            foreach ($this->slaves as $slave) {
                try {
                    $replicaConnection = $this->driver->getConnectionFactory()->create($slave['host'], $slave['port'], $this->timeout);
                    $this->slavesConnection[] = $replicaConnection;
                } catch (RedisProxyException $e) {
                    throw $e;
                } catch (Throwable $t) {
                    continue;
                }
            }
        }

        if ($this->masterConnection === null) {
            return false;
        }

        return true;
    }

    public function handleFailed(int $attempt): bool
    {
        $this->failedCount++;
        $result = $this->loadConnections();

        if ($result === true) {
            return true;
        }

        /**@pstan  */
        usleep($this->retryWait * self::MICRO_TO_SECONDS);
        return $this->failedCount < $this->maxFails;
    }

    private function getMasterConnection(): Redis|null
    {
        if ($this->database) {
            $this->driver->connectionSelect($this->masterConnection, $this->database);
        }
        return $this->masterConnection;
    }

    /**
     * @throws RedisProxyException
     */
    private function getReplicaConnection(): Redis|null
    {
        if (count($this->slavesConnection) === 0) {
            Debugger::log('Cant find slave using master', 'debug');
            return $this->masterConnection;
        }
        Debugger::log('Using slave connection', 'debug');
        $slaveConnection = $this->slavesConnection[array_rand($this->slavesConnection)];

        if ($this->database) {
            $this->driver->connectionSelect($slaveConnection, $this->database);
        }

        return $slaveConnection;
    }

    private function reset(): void
    {
        $this->masterConnection = null;
        $this->slavesConnection = [];
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

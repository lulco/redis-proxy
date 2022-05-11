<?php

namespace RedisProxy\ConnectionPool;

use RedisProxy\ConnectionPoolFactory\SingleNodeConnectionPoolFactory;
use RedisProxy\Driver\Driver;

class SentinelConnectionPool implements ConnectionPool
{
    private Driver $driver;

    private array $sentinels;

    private string $clusterId;

    private int $database;

    private float $timeout;

    public function __construct(Driver $driver, array $sentinels, string $clusterId, int $database = 0, float $timeout = 0.0)
    {
        $this->driver = $driver;
        $this->sentinels = $sentinels;
        $this->clusterId = $clusterId;
        $this->database = $database;
        $this->timeout = $timeout;
    }

    public function getConnection(string $command)
    {
        foreach ($this->sentinels as $sentinel) {
            $sentinelConnection = $this->driver->getDriverFactory()->create(new SingleNodeConnectionPoolFactory($sentinel['host'], $sentinel['port']));
            var_dump($sentinelConnection->sentinelReplicas($this->clusterId));
            exit;
        }
    }

    public function handleFailed(): bool
    {

    }
}
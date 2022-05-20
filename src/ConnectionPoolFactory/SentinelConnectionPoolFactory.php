<?php

namespace RedisProxy\ConnectionPoolFactory;

use RedisProxy\ConnectionPool\SentinelConnectionPool;
use RedisProxy\Driver\Driver;

class SentinelConnectionPoolFactory implements ConnectionPoolFactory
{
    private array $sentinels;

    private string $clusterId;

    private int $database;

    private float $timeout;

    private ?int $retryWait;

    private ?int $maxFails;

    private bool $writeToReplicas;

    public function __construct(array $sentinels, string $clusterId, int $database = 0, float $timeout = 0.0, ?int $retryWait = null, ?int $maxFails = null, bool $writeToReplicas = true)
    {
        $this->sentinels = $sentinels;
        $this->clusterId = $clusterId;
        $this->database = $database;
        $this->timeout = $timeout;
        $this->retryWait = $retryWait;
        $this->maxFails = $maxFails;
        $this->writeToReplicas = $writeToReplicas;
    }

    public function create(Driver $driver): SentinelConnectionPool
    {
        $connectionPool = new SentinelConnectionPool($driver, $this->sentinels, $this->clusterId, $this->database, $this->timeout);
        $connectionPool->setWriteToReplicas($this->writeToReplicas);
        if ($this->retryWait) {
            $connectionPool->setRetryWait($this->retryWait);
        }
        if ($this->maxFails) {
            $connectionPool->setMaxFails($this->maxFails);
        }
        return $connectionPool;
    }
}

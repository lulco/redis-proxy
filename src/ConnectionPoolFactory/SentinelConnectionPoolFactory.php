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

    private ?float $operationTimeout;

    private ?int $retryWait;

    private ?int $maxFails;

    private bool $writeToReplicas;

    private string $connectMode;

    public function __construct(array $sentinels, string $clusterId, int $database = 0, float $timeout = 0.0, ?int $retryWait = null, ?int $maxFails = null, bool $writeToReplicas = true, ?float $operationTimeout = null, string $connectMode = 'connect')
    {
        $this->sentinels = $sentinels;
        $this->clusterId = $clusterId;
        $this->database = $database;
        $this->timeout = $timeout;
        $this->operationTimeout = $operationTimeout;
        $this->retryWait = $retryWait;
        $this->maxFails = $maxFails;
        $this->writeToReplicas = $writeToReplicas;
        $this->connectMode = $connectMode;
    }

    public function create(Driver $driver): SentinelConnectionPool
    {
        $connectionPool = new SentinelConnectionPool($driver, $this->sentinels, $this->clusterId, $this->database, $this->timeout, $this->operationTimeout, $this->connectMode);
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

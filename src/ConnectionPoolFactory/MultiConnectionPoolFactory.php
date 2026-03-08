<?php

namespace RedisProxy\ConnectionPoolFactory;

use RedisProxy\ConnectionPool\MultiConnectionPool;
use RedisProxy\Driver\Driver;

class MultiConnectionPoolFactory implements ConnectionPoolFactory
{
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

    private ?float $operationTimeout;

    private ?int $retryWait;

    private ?int $maxFails;

    private bool $writeToReplicas;

    private string $connectMode;

    /**
     * @param array{host: string, port: int} $master
     * @param array{array{host: string, port: int}} $slaves
     */
    public function __construct(array $master, array $slaves, int $database = 0, float $timeout = 0.0, ?int $retryWait = null, ?int $maxFails = null, bool $writeToReplicas = true, ?float $operationTimeout = null, string $connectMode = 'connect')
    {
        $this->master = $master;
        $this->slaves = $slaves;
        $this->database = $database;
        $this->timeout = $timeout;
        $this->operationTimeout = $operationTimeout;
        $this->retryWait = $retryWait;
        $this->maxFails = $maxFails;
        $this->writeToReplicas = $writeToReplicas;
        $this->connectMode = $connectMode;
    }

    public function create(Driver $driver): MultiConnectionPool
    {
        $connectionPool = new MultiConnectionPool($driver, $this->master, $this->slaves, $this->database, $this->timeout, $this->operationTimeout, $this->connectMode);
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

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

    private ?int $retryWait;

    private ?int $maxFails;

    private bool $writeToReplicas;

    /**
     * @param array{host: string, port: int} $master
     * @param array{array{host: string, port: int}} $slaves
     */
    public function __construct(array $master, array $slaves, int $database = 0, float $timeout = 0.0, ?int $retryWait = null, ?int $maxFails = null, bool $writeToReplicas = true)
    {
        $this->master = $master;
        $this->slaves = $slaves;
        $this->database = $database;
        $this->timeout = $timeout;
        $this->retryWait = $retryWait;
        $this->maxFails = $maxFails;
        $this->writeToReplicas = $writeToReplicas;
    }

    public function create(Driver $driver): MultiConnectionPool
    {
        $connectionPool = new MultiConnectionPool($driver, $this->master, $this->slaves, $this->database, $this->timeout);
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

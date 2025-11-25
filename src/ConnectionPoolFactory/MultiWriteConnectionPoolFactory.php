<?php

declare(strict_types=1);

namespace RedisProxy\ConnectionPoolFactory;

use RedisProxy\ConnectionPool\MultiWriteConnectionPool;
use RedisProxy\Driver\Driver;

class MultiWriteConnectionPoolFactory implements ConnectionPoolFactory
{
    /**
     * @var list<array{host: string, port: int}>
     */
    private array $masters;

    /**
     * @var list<array{host: string, port: int}>
     */
    private array $slaves;

    private int $database;

    private float $timeout;

    private ?int $retryWait;

    private ?int $maxFails;

    private bool $writeToReplicas;

    private string $strategy;

    /**
     * @param list<array{host: string, port: int}> $masters
     * @param list<array{host: string, port: int}> $slaves
     */
    public function __construct(array $masters, array $slaves, int $database = 0, float $timeout = 0.0, ?int $retryWait = null, ?int $maxFails = null, bool $writeToReplicas = true, string $strategy = MultiWriteConnectionPool::STRATEGY_RANDOM)
    {
        $this->masters = $masters;
        $this->slaves = $slaves;
        $this->database = $database;
        $this->timeout = $timeout;
        $this->retryWait = $retryWait;
        $this->maxFails = $maxFails;
        $this->writeToReplicas = $writeToReplicas;
        $this->strategy = $strategy;
    }

    public function create(Driver $driver): MultiWriteConnectionPool
    {
        $connectionPool = new MultiWriteConnectionPool($driver, $this->masters, $this->slaves, $this->database, $this->timeout, $this->strategy);
        $connectionPool->setWriteToReplicas($this->writeToReplicas);
        if ($this->retryWait !== null) {
            $connectionPool->setRetryWait($this->retryWait);
        }
        if ($this->maxFails !== null) {
            $connectionPool->setMaxFails($this->maxFails);
        }
        return $connectionPool;
    }
}

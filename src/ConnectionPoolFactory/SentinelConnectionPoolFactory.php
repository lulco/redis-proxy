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

    public function __construct( array $sentinels, string $clusterId, int $database = 0, float $timeout = 0.0)
    {
        $this->sentinels = $sentinels;
        $this->clusterId = $clusterId;
        $this->database = $database;
        $this->timeout = $timeout;
    }

    public function create(Driver $driver): SentinelConnectionPool
    {
        return new SentinelConnectionPool($driver, $this->sentinels, $this->clusterId, $this->database, $this->timeout);
    }
}
<?php

namespace RedisProxy\ConnectionPool;

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

    }

    public function handleFailed(): bool
    {

    }
}
<?php

namespace RedisProxy\ConnectionPoolFactory;

use RedisProxy\ConnectionPool\SingleNodeConnectionPool;
use RedisProxy\Driver\Driver;

class SingleNodeConnectionPoolFactory implements ConnectionPoolFactory
{
    private string $host;

    private int $port;

    private int $database;

    private float $timeout;

    public function __construct(string $host, int $port, int $database = 0, float $timeout = 0.0)
    {
        $this->host = $host;
        $this->port = $port;
        $this->database = $database;
        $this->timeout = $timeout;
    }

    public function create(Driver $driver): SingleNodeConnectionPool
    {
        return new SingleNodeConnectionPool($driver, $this->host, $this->port, $this->database, $this->timeout);
    }
}
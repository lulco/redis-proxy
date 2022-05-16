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

    private bool $autoSelectDb;

    public function __construct(string $host, int $port, int $database = 0, float $timeout = 0.0, bool $autoSelectDb = true)
    {
        $this->host = $host;
        $this->port = $port;
        $this->database = $database;
        $this->timeout = $timeout;
        $this->autoSelectDb = $autoSelectDb;
    }

    public function create(Driver $driver): SingleNodeConnectionPool
    {
        return new SingleNodeConnectionPool($driver, $this->host, $this->port, $this->database, $this->timeout, $this->autoSelectDb);
    }
}

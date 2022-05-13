<?php

namespace RedisProxy\ConnectionPool;

use Redis;
use Predis\Client;
use RedisProxy\Driver\Driver;
use RedisProxy\RedisProxyException;

class SingleNodeConnectionPool implements ConnectionPool
{
    private Driver $driver;

    private string $host;

    private int $port;

    private int $database;

    private float $timeout;

    private bool $autoSelectDb;

    /** @var Redis|Client */
    private $connection = null;

    public function __construct(Driver $driver, string $host, int $port, int $database = 0, float $timeout = 0.0, bool $autoSelectDb = true)
    {
        $this->driver = $driver;
        $this->host = $host;
        $this->port = $port;
        $this->database = $database;
        $this->timeout = $timeout;
        $this->autoSelectDb = $autoSelectDb;
    }

    /**
     * @throws RedisProxyException
     */
    public function getConnection(string $command)
    {
        if ($this->connection !== null) {
            return $this->connection;
        }
        $this->connection = $this->driver->getConnectionFactory()->create($this->host, $this->port, $this->timeout);

        if ($this->autoSelectDb) {
            $this->driver->connectionSelect($this->connection, $this->database);
        }

        return $this->connection;
    }

    public function handleFailed(): bool
    {
        return false;
    }
}

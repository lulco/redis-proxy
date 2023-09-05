<?php

namespace RedisProxy\ConnectionPool;

use Predis\Client;
use Redis;
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

    private int $retryWait;

    private int $maxFails;

    public function __construct(Driver $driver, string $host, int $port, int $database = 0, float $timeout = 0.0, bool $autoSelectDb = true, ?int $retryWait = null, ?int $maxFails = null)
    {
        $this->driver = $driver;
        $this->host = $host;
        $this->port = $port;
        $this->database = $database;
        $this->timeout = $timeout;
        $this->autoSelectDb = $autoSelectDb;
        $this->retryWait = $retryWait ?? 1000;
        $this->maxFails = $maxFails ?? 1;
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

    public function handleFailed(int $attempt): bool
    {
        if ($attempt < $this->maxFails) {
            usleep($this->retryWait * 1000);
            return true;
        }
        return false;
    }

    public function setRetryWait(int $retryWait): self
    {
        $this->retryWait = $retryWait;
        return $this;
    }

    public function setMaxFails(int $maxFails): self
    {
        $this->maxFails = $maxFails;
        return $this;
    }
}

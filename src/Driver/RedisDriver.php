<?php

namespace RedisProxy\Driver;

use RedisException;
use RedisProxy\ConnectionFactory\RedisConnectionFactory;
use RedisProxy\ConnectionPool\ConnectionPool;
use RedisProxy\ConnectionPoolFactory\ConnectionPoolFactory;
use RedisProxy\DriverFactory\RedisDriverFactory;
use RedisProxy\RedisProxy;
use RedisProxy\RedisProxyException;
use Throwable;

class RedisDriver implements Driver
{
    private ConnectionPool $connectionPool;

    private ?RedisConnectionFactory $connectionFactory = null;

    private ?RedisDriverFactory $driverFactory = null;

    private array $typeMap = [
        1 => RedisProxy::TYPE_STRING,
        2 => RedisProxy::TYPE_SET,
        3 => RedisProxy::TYPE_LIST,
        4 => RedisProxy::TYPE_SORTED_SET,
        5 => RedisProxy::TYPE_HASH,
    ];

    public function __construct(ConnectionPoolFactory $connectionPollFactory)
    {
        $this->connectionPool = $connectionPollFactory->create($this);
    }

    public function getConnectionFactory(): RedisConnectionFactory
    {
        if ($this->connectionFactory === null) {
            $this->connectionFactory = new RedisConnectionFactory();
        }
        return $this->connectionFactory;
    }

    public function getDriverFactory(): RedisDriverFactory
    {
        if ($this->driverFactory === null) {
            $this->driverFactory = new RedisDriverFactory();
        }
        return $this->driverFactory;
    }

    /**
     * @throws RedisProxyException
     */
    public function call(string $command, array $params = [])
    {
        try {
            if (method_exists($this, $command)) {
                return call_user_func_array([$this, $command], $params);
            }

            return call_user_func_array([$this->connectionPool->getConnection($command), $command], $params);
        } catch (Throwable $t) {
            if ($t instanceof RedisException && $this->connectionPool->handleFailed()) {
                return $this->call($command, $params);
            }
            throw new RedisProxyException('Redis driver exception: ' . $t->getMessage(), 0, $t);
        }
    }

    /**
     * @throws Throwable
     */
    public function callSentinel(string $command, array $params = [])
    {
        if (method_exists($this, $command)) {
            return call_user_func_array([$this, $command], $params);
        }

        return $this->connectionPool->getConnection('sentinel')->rawCommand('sentinel', $command, ...$params);
    }

    /**
     * @throws RedisProxyException
     */
    private function select(int $database): bool
    {
        try {
            $result = $this->connectionPool->getConnection('select')->select($database);
        } catch (Throwable $t) {
            throw new RedisProxyException('Invalid DB index');
        }
        return (bool) $result;
    }

    private function type(string $key): ?string
    {
        $result = $this->connectionPool->getConnection('type')->type($key);
        return $this->typeMap[$result] ?? null;
    }

    private function psetex(string $key, int $milliseconds, string $value): bool
    {
        $result = $this->connectionPool->getConnection('psetex')->psetex($key, $milliseconds, $value);
        if ($result == '+OK') {
            return true;
        }
        return !!$result;
    }

    public function connectionRole($connection): string
    {
        $result = $connection->rawCommand('role');
        return is_array($result) ? $result[0] : '';
    }
}

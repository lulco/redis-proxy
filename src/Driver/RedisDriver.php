<?php

namespace RedisProxy\Driver;

use RedisException;
use RedisProxy\ConnectionFactory\RedisConnectionFactory;
use RedisProxy\ConnectionFactory\Serializers;
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

    private string $optSerializer = Serializers::NONE;

    private array $typeMap = [
        1 => RedisProxy::TYPE_STRING,
        2 => RedisProxy::TYPE_SET,
        3 => RedisProxy::TYPE_LIST,
        4 => RedisProxy::TYPE_SORTED_SET,
        5 => RedisProxy::TYPE_HASH,
    ];

    public function __construct(ConnectionPoolFactory $connectionPollFactory, string $optSerializer = Serializers::NONE)
    {
        $this->connectionPool = $connectionPollFactory->create($this);
        $this->optSerializer = $optSerializer;
    }

    public function getConnectionFactory(): RedisConnectionFactory
    {
        if ($this->connectionFactory === null) {
            $this->connectionFactory = new RedisConnectionFactory($this->optSerializer);
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
        $attempt = 0;
        while (true) {
            try {
                if (method_exists($this, $command)) {
                    return call_user_func_array([$this, $command], $params);
                }

                return call_user_func_array([$this->connectionPool->getConnection($command), $command], $params);
            } catch (RedisProxyException $e) {
                throw $e;
            } catch (Throwable $t) {
                if (!$t instanceof RedisException || !$this->connectionPool->handleFailed(++$attempt)) {
                    throw new RedisProxyException("Error for command '$command', use getPrevious() for more info", 1484162284, $t);
                }
            }
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

    private function type(string $key): ?string
    {
        $result = $this->connectionPool->getConnection('type')->type($key);
        return $this->typeMap[$result] ?? null;
    }

    private function hexists(string $key, string $field): bool
    {
        return (bool)$this->connectionPool->getConnection('hexists')->hexists($key, $field);
    }

    private function psetex(string $key, int $milliseconds, string $value): bool
    {
        $result = $this->connectionPool->getConnection('psetex')->psetex($key, $milliseconds, $value);
        if ($result == '+OK') {
            return true;
        }
        return !!$result;
    }

    /**
     * @return int[]|null
     */
    private function hexpire(string $key, int $seconds, string ...$fields): ?array
    {
        return $this
            ->connectionPool
            ->getConnection('hexpire')
            ->hexpire($key, $seconds, $fields);
    }

    private function select(int $database): bool
    {
        return $this->connectionSelect($this->connectionPool->getConnection('select'), $database);
    }

    public function connectionRole($connection): string
    {
        $result = $connection->rawCommand('role');
        return is_array($result) ? $result[0] : '';
    }

    /**
     * @throws RedisProxyException
     */
    public function connectionSelect($connection, int $database): bool
    {
        try {
            $result = $connection->select($database);
        } catch (Throwable $t) {
            throw new RedisProxyException('Invalid DB index');
        }
        if ($result === false) {
            throw new RedisProxyException('Invalid DB index');
        }
        return (bool) $result;
    }

    public function connectionReset(): void
    {
        $this->connectionPool->resetConnection();
    }
}

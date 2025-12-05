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

    /** @var array<int, string> */
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
     * @param array<mixed> $params
     * @throws RedisProxyException
     */
    public function call(string $command, array $params = []): mixed
    {
        $attempt = 0;
        while (true) {
            try {
                if (method_exists($this, $command)) {
                    /** @var callable $callable */
                    $callable = [$this, $command];
                    return call_user_func_array($callable, $params);
                }

                $connection = $this->connectionPool->getConnection($command);
                /** @var callable $callable */
                $callable = [$connection, $command];
                return call_user_func_array($callable, $params);
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
     * @param array<mixed> $params
     * @throws Throwable
     */
    public function callSentinel(string $command, array $params = []): mixed
    {
        if (method_exists($this, $command)) {
            /** @var callable $callable */
            $callable = [$this, $command];
            return call_user_func_array($callable, $params);
        }

        /** @var \Redis $connection */
        $connection = $this->connectionPool->getConnection('sentinel');
        return $connection->rawCommand('sentinel', $command, ...$params);
    }

    private function type(string $key): ?string
    {
        /** @var \Redis $connection */
        $connection = $this->connectionPool->getConnection('type');
        $result = $connection->type($key);
        /** @var int $result */
        return $this->typeMap[$result] ?? null;
    }

    private function hexists(string $key, string $field): bool
    {
        /** @var \Redis $connection */
        $connection = $this->connectionPool->getConnection('hexists');
        return (bool)$connection->hexists($key, $field);
    }

    private function psetex(string $key, int $milliseconds, string $value): bool
    {
        /** @var \Redis $connection */
        $connection = $this->connectionPool->getConnection('psetex');
        $result = $connection->psetex($key, $milliseconds, $value);
        if ($result == '+OK') {
            return true;
        }
        return !!$result;
    }

    /**
     * @return array<int>|null
     */
    private function hexpire(string $key, int $seconds, string ...$fields): ?array
    {
        /** @var \Redis $connection */
        $connection = $this->connectionPool->getConnection('hexpire');
        /** @phpstan-ignore-next-line */
        $result = $connection->hexpire($key, $seconds, $fields);
        /** @var array<int>|null $result */
        return $result;
    }

    private function select(int $database): bool
    {
        return $this->connectionSelect($this->connectionPool->getConnection('select'), $database);
    }

    public function connectionRole(mixed $connection): string
    {
        /** @var \Redis $connection */
        $result = $connection->rawCommand('role');
        if (is_array($result) && isset($result[0])) {
            /** @var string|int|float $firstElement */
            $firstElement = $result[0];
            return is_string($firstElement) ? $firstElement : (string)$firstElement;
        }
        return '';
    }

    /**
     * @throws RedisProxyException
     */
    public function connectionSelect(mixed $connection, int $database): bool
    {
        try {
            /** @var \Redis $connection */
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

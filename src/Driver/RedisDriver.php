<?php

declare(strict_types=1);

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

    /**
     * @var array<int, string>
     */
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
    /**
     * @param list<mixed> $params
     */
    public function call(string $command, array $params = []): mixed
    {
        $attempt = 0;
        while (true) {
            try {
                if (method_exists($this, $command)) {
                    return $this->$command(...$params);
                }

                $connection = $this->connectionPool->getConnection($command);
                return $connection->$command(...$params);
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
    /**
     * @param list<mixed> $params
     */
    public function callSentinel(string $command, array $params = []): mixed
    {
        if (method_exists($this, $command)) {
            return $this->$command(...$params);
        }

        $conn = $this->connectionPool->getConnection('sentinel');
        if (method_exists($conn, 'rawcommand')) {
            /** @var \Redis $conn */
            return $conn->rawcommand('sentinel', $command, ...$params);
        }
        if (method_exists($conn, 'executeRaw')) {
            /** @var \Predis\Client $conn */
            return $conn->executeRaw(['sentinel', $command, ...$params]);
        }
        return null;
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
        return (bool) $result;
    }

    /**
     * @return int[]|null
     */
    private function hexpire(string $key, int $seconds, string ...$fields): ?array
    {
        $conn = $this->connectionPool->getConnection('hexpire');
        if (method_exists($conn, 'rawcommand')) {
            /** @var \Redis $conn */
            $res = $conn->rawcommand('hexpire', $key, $seconds, ...$fields);
            return is_array($res) ? $res : null;
        }
        if (method_exists($conn, 'executeRaw')) {
            /** @var \Predis\Client $conn */
            $res = $conn->executeRaw(['hexpire', $key, $seconds, ...$fields]);
            return is_array($res) ? $res : null;
        }
        return null;
    }

    private function select(int $database): bool
    {
        return $this->connectionSelect($this->connectionPool->getConnection('select'), $database);
    }

    public function connectionRole(mixed $connection): string
    {
        $result = null;
        if (is_object($connection)) {
            if (method_exists($connection, 'rawcommand')) {
                /** @var \Redis $connection */
                $result = $connection->rawcommand('role');
            } elseif (method_exists($connection, 'executeRaw')) {
                /** @var \Predis\Client $connection */
                $result = $connection->executeRaw(['role']);
            }
        }
        return is_array($result) ? $result[0] : '';
    }

    /**
     * @throws RedisProxyException
     */
    public function connectionSelect(mixed $connection, int $database): bool
    {
        try {
            if (!is_object($connection) || !method_exists($connection, 'select')) {
                throw new RedisProxyException('Invalid connection');
            }
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

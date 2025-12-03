<?php

declare(strict_types=1);

namespace RedisProxy\Driver;

use Predis\Client;
use Predis\Connection\ConnectionException;
use Predis\Response\Status;
use RedisProxy\ConnectionFactory\PredisConnectionFactory;
use RedisProxy\ConnectionFactory\Serializers;
use RedisProxy\ConnectionPool\ConnectionPool;
use RedisProxy\ConnectionPoolFactory\ConnectionPoolFactory;
use RedisProxy\DriverFactory\PredisDriverFactory;
use RedisProxy\RedisProxy;
use RedisProxy\RedisProxyException;
use Throwable;

class PredisDriver implements Driver
{
    private ConnectionPool $connectionPool;

    private ?PredisConnectionFactory $connectionFactory = null;

    private ?PredisDriverFactory $driverFactory = null;

    private string $optSerializer = Serializers::NONE;

    /**
     * @var array<string, string>
     */
    private array $typeMap = [
        'string' => RedisProxy::TYPE_STRING,
        'set' => RedisProxy::TYPE_SET,
        'list' => RedisProxy::TYPE_LIST,
        'zset' => RedisProxy::TYPE_SORTED_SET,
        'hash' => RedisProxy::TYPE_HASH,
    ];

    public function __construct(ConnectionPoolFactory $connectionPollFactory, string $optSerializer = Serializers::NONE)
    {
        $this->connectionPool = $connectionPollFactory->create($this);
        $this->optSerializer = $optSerializer;
    }

    public function getConnectionFactory(): PredisConnectionFactory
    {
        if ($this->connectionFactory === null) {
            $this->connectionFactory = new PredisConnectionFactory($this->optSerializer);
        }
        return $this->connectionFactory;
    }

    public function getDriverFactory(): PredisDriverFactory
    {
        if ($this->driverFactory === null) {
            $this->driverFactory = new PredisDriverFactory();
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
        /*var_dump($params);
        var_dump($command);*/
        $attempt = 0;
        while (true) {
            try {
                if (method_exists($this, $command)) {
                    return $this->$command(...$params);
                }

                $connection = $this->connectionPool->getConnection($command);
                $result = $connection->$command(...$params);
                return $this->transformResult($result);
            } catch (RedisProxyException $e) {
                throw $e;
            } catch (Throwable $t) {
                var_dump($t->getMessage());
                if (!$t instanceof ConnectionException || !$this->connectionPool->handleFailed(++$attempt)) {
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
        /** @var Client $conn */
        return $conn->executeRaw(['sentinel', $command, ...$params]);
    }

    private function type(string $key): ?string
    {
        $result = $this->connectionPool->getConnection('type')->type($key);
        $result = $result instanceof Status ? $result->getPayload() : $result;
        return $this->typeMap[$result] ?? null;
    }

    private function psetex(string $key, int $milliseconds, string $value): bool
    {
        $result = $this->connectionPool->getConnection('type')->psetex($key, $milliseconds, $value);
        if ($result == '+OK') {
            return true;
        }
        return (bool) $this->transformResult($result);
    }

    private function mset($dictionary): bool
    {
        $result = $this->connectionPool->getConnection('mset')->mset($dictionary);
        return (bool) $this->transformResult($result);
    }

    private function hexists(string $key, string $field): bool
    {
        return (bool)$this->connectionPool->getConnection('hexists')->hexists($key, $field);
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

    /**
     * @param string|null $iterator
     * @return array<int, mixed>
     */
    private function scan(?string &$iterator, ?string $pattern = null, ?int $count = null): array
    {
        if ($iterator === null) {
            $iterator = '0';
        }
        $returned = $this->connectionPool->getConnection('scan')->scan($iterator, ['match' => $pattern, 'count' => $count]);
        if (!is_array($returned) || !isset($returned[0], $returned[1])) {
            return [];
        }
        $iterator = $returned[0];
        return $returned[1];
    }

    /**
     * @param string|null $iterator
     * @return array<int, mixed>
     */
    private function hscan(string $key, ?string &$iterator, ?string $pattern = null, int $count = 0): array
    {
        if ($iterator === null) {
            $iterator = '0';
        }
        $returned = $this->connectionPool->getConnection('hscan')->hscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
        if (!is_array($returned) || !isset($returned[0], $returned[1])) {
            return [];
        }
        $iterator = $returned[0];
        return $returned[1];
    }

    /**
     * @param string|null $iterator
     * @return array<int, mixed>
     * @deprecated in PHP 8.4 implicit nullable defaults; using nullable types
     */
    private function sscan(string $key, ?string &$iterator, ?string $pattern = null, ?int $count = null): array
    {
        if ($iterator === null) {
            $iterator = '0';
        }
        $returned = $this->connectionPool->getConnection('sscan')->sscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
        if (!is_array($returned) || !isset($returned[0], $returned[1])) {
            return [];
        }
        $iterator = $returned[0];
        return $returned[1];
    }

    /**
     * @param null|string $iterator
     * @return array<int, mixed>
     */
    private function zscan(string $key, &$iterator, ?string $pattern = null, ?int $count = null): array
    {
        if ($iterator === null) {
            $iterator = '0';
        }
        $returned = $this->connectionPool->getConnection('zscan')->zscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
        if (!is_array($returned) || !isset($returned[0], $returned[1])) {
            return [];
        }
        $iterator = $returned[0];
        return $returned[1];
    }

    /**
     * @return array<int|string, mixed>
     */
    private function zrange(string $key, int $start, int $stop, bool $withscores = false): array
    {
        return $this->connectionPool->getConnection('zrange')->zrange($key, $start, $stop, ['WITHSCORES' => $withscores]);
    }

    /**
     * @return array<int, mixed>
     */
    private function zpopmin(string $key, int $count = 1): array
    {
        $res = $this->connectionPool->getConnection('zpopmin')->zpopmin($key, $count);
        return is_array($res) ? $res : [];
    }

    /**
     * @return array<int, mixed>
     */
    private function zpopmax(string $key, int $count = 1): array
    {
        $res = $this->connectionPool->getConnection('zpopmax')->zpopmax($key, $count);
        return is_array($res) ? $res : [];
    }

    /**
     * @return array<int|string, mixed>
     */
    public function zrevrange(string $key, int $start, int $stop, bool $withscores = false): array
    {
        return $this->connectionPool->getConnection('zrevrange')->zrevrange($key, $start, $stop, ['WITHSCORES' => $withscores]);
    }

    public function close(): void
    {
        $conn = $this->connectionPool->getConnection('close');
        if (method_exists($conn, 'executeRaw')) {
            /** @var Client $conn */
            $conn->executeRaw(['close']);
        }
    }

    public function select(int $database): bool
    {
        return $this->connectionSelect($this->connectionPool->getConnection('select'), $database);
    }

    public function rawCommand(mixed ...$params): mixed
    {
        $result = null;
        $connection = $this->connectionPool
            ->getConnection('rawCommand');
        if ($connection instanceof Client) {
            $result = $connection->executeRaw($params);
        }

        return $this->transformResult($result);
    }

    public function connectionRole(mixed $connection): string
    {
        $result = null;
        if ($connection instanceof Client) {
            $result = $connection->executeRaw(['role']);
        }
        return is_array($result) ? $result[0] : '';
    }

    /**
     * @throws RedisProxyException
     */
    public function connectionSelect(mixed $connection, int $database): bool
    {
        try {
            if (!is_object($connection) || !is_callable([$connection, 'select'])) {
                throw new RedisProxyException('Invalid connection');
            }
            $result = $connection->select($database);
        } catch (Throwable $t) {
            throw new RedisProxyException('Invalid DB index');
        }
        $result = $this->transformResult($result);
        if ($result === false) {
            throw new RedisProxyException('Invalid DB index');
        }
        return (bool) $result;
    }

    public function connectionReset(): void
    {
        $this->connectionPool->resetConnection();
    }

    /**
     * Transforms Predis result Payload to boolean
     * @param mixed $result
     * @return mixed
     */
    private function transformResult(mixed $result): mixed
    {
        if ($result instanceof Status) {
            $result = $result->getPayload() === 'OK';
        }
        return $result;
    }
}

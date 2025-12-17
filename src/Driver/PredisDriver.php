<?php

namespace RedisProxy\Driver;

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

    /** @var array<string, string> */
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

                /** @var \Predis\Client $connection */
                $connection = $this->connectionPool->getConnection($command);
                /** @var callable $callable */
                $callable = [$connection, $command];
                $result = call_user_func_array($callable, $params);
                return $this->transformResult($result);
            } catch (RedisProxyException $e) {
                throw $e;
            } catch (Throwable $t) {
                if (!$t instanceof ConnectionException || !$this->connectionPool->handleFailed(++$attempt)) {
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

        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('sentinel');
        return $connection->executeRaw(['sentinel', $command, ...$params]);
    }

    private function type(string $key): ?string
    {
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('type');
        $result = $connection->type($key);
        $result = $result instanceof Status ? $result->getPayload() : $result;
        /** @var string $result */
        return $this->typeMap[$result] ?? null;
    }

    private function psetex(string $key, int $milliseconds, string $value): bool
    {
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('psetex');
        $result = $connection->psetex($key, $milliseconds, $value);
        if ($result == '+OK') {
            return true;
        }
        return !!$this->transformResult($result);
    }

    private function mset(mixed $dictionary): bool
    {
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('mset');
        /** @var array<mixed> $dictionary */
        $result = $connection->mset($dictionary);
        /** @var bool $result */
        $result = $this->transformResult($result);
        return $result;
    }

    private function hexists(string $key, string $field): bool
    {
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('hexists');
        return (bool)$connection->hexists($key, $field);
    }

    /**
     * @return array<int>|null
     */
    private function hexpire(string $key, int $seconds, string ...$fields): ?array
    {
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('hexpire');
        $result = $connection->hexpire($key, $seconds, $fields);
        /** @var array<int>|null $result */
        return $result;
    }

    private function scan(mixed &$iterator, ?string $pattern = null, ?int $count = null): mixed
    {
        if ($iterator === null) {
            $iterator = '0';
        }
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('scan');
        $returned = $connection->scan($iterator, ['match' => $pattern, 'count' => $count]);
        $iterator = $returned[0];
        return $returned[1];
    }

    private function hscan(string $key, mixed &$iterator, ?string $pattern = null, int $count = 0): mixed
    {
        if ($iterator === null) {
            $iterator = '0';
        }
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('hscan');
        $returned = $connection->hscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
        $iterator = $returned[0];
        return $returned[1];
    }

    private function sscan(string $key, mixed &$iterator, ?string $pattern = null, ?int $count = null): mixed
    {
        if ($iterator === null) {
            $iterator = '0';
        }
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('sscan');
        if (is_int($iterator)) {
            $iteratorValue = $iterator;
        } else {
            /** @var string|float|bool|null $iterator */
            $iteratorValue = (int)$iterator;
        }
        $returned = $connection->sscan($key, $iteratorValue, ['match' => $pattern, 'count' => $count]);
        $iterator = $returned[0];
        return $returned[1];
    }

    private function zscan(string $key, mixed &$iterator, ?string $pattern = null, ?int $count = null): mixed
    {
        if ($iterator === null) {
            $iterator = '0';
        }
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('zscan');
        if (is_int($iterator)) {
            $iteratorValue = $iterator;
        } else {
            /** @var string|float|bool|null $iterator */
            $iteratorValue = (int)$iterator;
        }
        $returned = $connection->zscan($key, $iteratorValue, ['match' => $pattern, 'count' => $count]);
        $iterator = $returned[0];
        return $returned[1];
    }

    /**
     * @return array<mixed>
     */
    private function zrange(string $key, int $start, int $stop, bool $withscores = false): array
    {
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('zrange');
        $result = $connection->zrange($key, $start, $stop, ['WITHSCORES' => $withscores]);
        /** @var array<mixed> $result */
        return $result;
    }

    /**
     * @return array<mixed>
     */
    private function zpopmin(string $key, int $count = 1): array
    {
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('zpopmin');
        $result = $connection->zpopmin($key, $count);
        /** @var array<mixed> $result */
        return $result;
    }

    /**
     * @return array<mixed>
     */
    private function zpopmax(string $key, int $count = 1): array
    {
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('zpopmax');
        $result = $connection->zpopmax($key, $count);
        /** @var array<mixed> $result */
        return $result;
    }

    /**
     * @return array<mixed>
     */
    public function zrevrange(string $key, int $start, int $stop, bool $withscores = false): array
    {
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('zrevrange');
        $result = $connection->zrevrange($key, $start, $stop, ['WITHSCORES' => $withscores]);
        /** @var array<mixed> $result */
        return $result;
    }

    /**
     * @param array<string, mixed> $messages
     */
    public function xadd(string $key, string $id, array $messages, int $maxLen = 0, bool $isApproximate = false, bool $nomkstream = false): string
    {
        $options = ['nomkstream' => $nomkstream];
        if ($maxLen > 0) {
            $options['trim'] = ['MAXLEN', $isApproximate ? '~' : '=', $maxLen];
        }

        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('xadd');
        return $connection->xadd($key, $messages, $id, $options);
    }

    public function close(): mixed
    {
        /** @var \Predis\Client $connection */
        $connection = $this->connectionPool->getConnection('close');
        return $connection->executeRaw(['close']);
    }

    public function select(int $database): bool
    {
        return $this->connectionSelect($this->connectionPool->getConnection('select'), $database);
    }

    public function connectionRole(mixed $connection): string
    {
        /** @var \Predis\Client $connection */
        $result = $connection->executeRaw(['role']);
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
            /** @var \Predis\Client $connection */
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
    private function transformResult($result)
    {
        if ($result instanceof Status) {
            $result = $result->getPayload() === 'OK';
        }
        return $result;
    }
}

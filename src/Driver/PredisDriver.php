<?php

namespace RedisProxy\Driver;

use Predis\Response\Status;
use RedisException;
use RedisProxy\ConnectionFactory\PredisConnectionFactory;
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

    private array $typeMap = [
        'string' => RedisProxy::TYPE_STRING,
        'set' => RedisProxy::TYPE_SET,
        'list' => RedisProxy::TYPE_LIST,
        'zset' => RedisProxy::TYPE_SORTED_SET,
        'hash' => RedisProxy::TYPE_HASH,
    ];

    public function __construct(ConnectionPoolFactory $connectionPollFactory)
    {
        $this->connectionPool = $connectionPollFactory->create($this);
    }

    public function getConnectionFactory(): PredisConnectionFactory
    {
        if ($this->connectionFactory === null) {
            $this->connectionFactory = new PredisConnectionFactory();
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

    public function call(string $command, array $params = [])
    {
        try {
            if (method_exists($this, $command)) {
                return call_user_func_array([$this, $command], $params);
            }

            $result = call_user_func_array([$this->connectionPool->getConnection($command), $command], $params);
            return $this->transformResult($result);
        } catch (RedisException $e) {
            // @TODO
        }
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
        $result = $this->transformResult($result);
        if ($result === false) {
            throw new RedisProxyException('Invalid DB index');
        }
        return (bool) $result;
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
        return !!$this->transformResult($result);
    }

    private function mset($dictionary): bool
    {
        $result = $this->connectionPool->getConnection('mset')->mset($dictionary);
        return $this->transformResult($result);
    }

    private function scan(&$iterator, ?string $pattern = null, ?int $count = null)
    {
        $returned = $this->connectionPool->getConnection('scan')->scan($iterator, ['match' => $pattern, 'count' => $count]);
        $iterator = $returned[0];
        return $returned[1];
    }

    private function hscan(string $key, &$iterator, ?string $pattern = null, int $count = 0)
    {
        $returned = $this->connectionPool->getConnection('hscan')->hscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
        $iterator = $returned[0];
        return $returned[1];
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
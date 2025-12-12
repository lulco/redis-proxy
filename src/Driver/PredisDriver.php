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
    public function call(string $command, array $params = [])
    {
        $attempt = 0;
        while (true) {
            try {
                if (method_exists($this, $command)) {
                    return call_user_func_array([$this, $command], $params);
                }

                $result = call_user_func_array([$this->connectionPool->getConnection($command), $command], $params);
                return $this->transformResult($result);
            } catch (RedisProxyException $e) {
                throw $e;
            } catch (Throwable $t) {
                if (!$t instanceof ConnectionException || !$this->connectionPool->handleFailed(++$attempt)) {
                    throw new RedisProxyException(sprintf("Error for command '%s', use getPrevious() for more info", $command), 1484162284, $t);
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

        return $this->connectionPool->getConnection('sentinel')->executeRaw(['sentinel', $command, ...$params]);
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

    private function scan(&$iterator, ?string $pattern = null, ?int $count = null)
    {
        if ($iterator === null) {
            $iterator = '0';
        }

        $returned = $this->connectionPool->getConnection('scan')->scan($iterator, ['match' => $pattern, 'count' => $count]);
        $iterator = $returned[0];
        return $returned[1];
    }

    private function hscan(string $key, &$iterator, ?string $pattern = null, int $count = 0)
    {
        if ($iterator === null) {
            $iterator = '0';
        }

        $returned = $this->connectionPool->getConnection('hscan')->hscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
        $iterator = $returned[0];
        return $returned[1];
    }

    private function sscan(string $key, &$iterator, string $pattern = null, int $count = null)
    {
        if ($iterator === null) {
            $iterator = '0';
        }

        $returned = $this->connectionPool->getConnection('sscan')->sscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
        $iterator = $returned[0];
        return $returned[1];
    }

    private function zscan(string $key, &$iterator, ?string $pattern = null, ?int $count = null)
    {
        if ($iterator === null) {
            $iterator = '0';
        }

        $returned = $this->connectionPool->getConnection('zscan')->zscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
        $iterator = $returned[0];
        return $returned[1];
    }

    private function zrange(string $key, int $start, int $stop, bool $withscores = false): array
    {
        return $this->connectionPool->getConnection('zrange')->zrange($key, $start, $stop, ['WITHSCORES' => $withscores]);
    }

    private function zpopmin(string $key, int $count = 1): array
    {
        return $this->connectionPool->getConnection('zpopmin')->zpopmin($key, $count);
    }

    private function zpopmax(string $key, int $count = 1): array
    {
        return $this->connectionPool->getConnection('zpopmax')->zpopmax($key, $count);
    }

    public function zrevrange(string $key, int $start, int $stop, bool $withscores = false): array
    {
        return $this->connectionPool->getConnection('zrevrange')->zrevrange($key, $start, $stop, ['WITHSCORES' => $withscores]);
    }

    public function xadd(string $key, string $id, array $messages, int $maxLen = 0, bool $isApproximate = false, bool $nomkstream = false): string
    {
        $options = ['nomkstream' => $nomkstream];
        if ($maxLen > 0) {
            $options['trim'] = ['MAXLEN', $isApproximate ? '~' : '=', $maxLen];
        }

        return $this->connectionPool->getConnection('xadd')->xadd($key, $messages, $id, $options);
    }

    public function close()
    {
        return $this->connectionPool->getConnection('close')->executeRaw(['close']);
    }

    public function select(int $database): bool
    {
        return $this->connectionSelect($this->connectionPool->getConnection('select'), $database);
    }

    public function connectionRole($connection): string
    {
        $result = $connection->executeRaw(['role']);
        return is_array($result) ? $result[0] : '';
    }

    /**
     * @throws RedisProxyException
     */
    public function connectionSelect($connection, int $database): bool
    {
        try {
            $result = $connection->select($database);
        } catch (Throwable $throwable) {
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

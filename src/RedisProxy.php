<?php

declare(strict_types=1);

namespace RedisProxy;

use InvalidArgumentException;
use RedisProxy\ConnectionFactory\Serializers;
use RedisProxy\ConnectionPool\MultiWriteConnectionPool;
use RedisProxy\ConnectionPoolFactory\ConnectionPoolFactory;
use RedisProxy\ConnectionPoolFactory\MultiConnectionPoolFactory;
use RedisProxy\ConnectionPoolFactory\MultiWriteConnectionPoolFactory;
use RedisProxy\ConnectionPoolFactory\SentinelConnectionPoolFactory;
use RedisProxy\ConnectionPoolFactory\SingleNodeConnectionPoolFactory;
use RedisProxy\Driver\Driver;
use RedisProxy\Driver\PredisDriver;
use RedisProxy\Driver\RedisDriver;

/**
 * @method string|null type(string $key)
 * @method bool psetex(string $key, int $milliseconds, string $value) Set the value and expiration in milliseconds of a key
 * @method mixed config(string $command, $argument = null)
 * @method int dbsize() Return the number of keys in the selected database
 * @method bool restore(string $key, int $ttl, string $serializedValue)
 * @method bool set(string $key, string $value)
 * @method bool setex(string $key, int $seconds, string $value)
 * @method int ttl(string $key)
 * @method int pttl(string $key)
 * @method array<int, string> keys(string $pattern)
 * @method int hset(string $key, string $field, string $value)
 * @method array<int, string> hkeys(string $key)
 * @method array<string, string> hgetall(string $key)
 * @method int hlen(string $key)
 * @method bool hexists(string $key, string $field)
 * @method int hstrlen(string $key, string $field)
 * @method array<int, string> smembers(string $key)
 * @method int scard(string $key)
 * @method int sismember(string $key, string $member)
 * @method int llen(string $key)
 * @method bool lset(string $key, string $index, string $value)
 * @method array<int, string> lrange(string $key, int $start, int $stop)
 * @method int lrem(string $key, string $value)
 * @method int zcard(string $key)
 * @method float zscore(string $key, string $member)
 * @method bool flushall()
 * @method bool flushdb()
 * @method array<int, string|float> zrange(string $key, int $start, int $stop, bool $withscores = false)
 * @method array<int, string|float> zrangebyscore(string $key, $start, $stop, array $options = [])
 * @method array<int, string|float> zpopmin(string $key, int $count = 1)
 * @method array<int, string|float> zpopmax(string $key, int $count = 1)
 * @method array<int, string|float> zrevrange(string $key, int $start, int $stop, bool $withscores = false)
 * @method float zincrby(string $key, float $increment, string $member)
 * @method int publish(string $channel, string $message)
 * @method mixed rawCommand(string $command, mixed ...$params)
 */
class RedisProxy
{
    public const DRIVER_REDIS = 'redis';
    public const DRIVER_PREDIS = 'predis';

    public const TYPE_STRING = 'string';
    public const TYPE_SET = 'set';
    public const TYPE_HASH = 'hash';
    public const TYPE_LIST = 'list';
    public const TYPE_SORTED_SET = 'sorted_set';

    /** Default unlimited timeout */
    private const DEFAULT_TIMEOUT_UNLIMITED = 0.0;

    /** Default connection values */
    private const DEFAULT_HOST = '127.0.0.1';
    private const DEFAULT_PORT = 6379;
    private const DEFAULT_DATABASE = 0;
    private const INDEX_EVEN = 0;
    private const INDEX_ODD = 1;
    private const MODULO_BASE = 2;

    private ConnectionPoolFactory $connectionPoolFactory;

    private ?Driver $driver = null;

    /**
     * @var list<string> order of drivers to try
     */
    private array $driversOrder;

    private string $optSerializer = Serializers::NONE;

    /**
     * @var list<string>
     */
    private array $supportedDrivers = [
        self::DRIVER_REDIS,
        self::DRIVER_PREDIS,
    ];

    /**
     * @param float    $timeout   seconds (default is 0.0 = unlimited)
     * @param int|null $retryWait milliseconds (null defaults to 1 second)
     * @param int|null $maxFails  1 = no retries, one attempt (default)
     *                            2 = one retry, two attempts, ...
     */
    public function __construct(
        string $host = self::DEFAULT_HOST,
        int $port = self::DEFAULT_PORT,
        int $database = self::DEFAULT_DATABASE,
        float $timeout = self::DEFAULT_TIMEOUT_UNLIMITED,
        ?int $retryWait = null,
        ?int $maxFails = null,
        string $optSerializer = Serializers::NONE
    ) {
        $this->connectionPoolFactory = new SingleNodeConnectionPoolFactory(
            $host,
            $port,
            $database,
            $timeout,
            true,
            $retryWait,
            $maxFails
        );
        $this->driversOrder = $this->supportedDrivers;
        $this->optSerializer = $optSerializer;
    }

    /**
     * @param list<array{host: string, port: int}> $sentinels
     */
    public function setSentinelConnectionPool(
        array $sentinels,
        string $clusterId,
        int $database = self::DEFAULT_DATABASE,
        float $timeout = self::DEFAULT_TIMEOUT_UNLIMITED,
        ?int $retryWait = null,
        ?int $maxFails = null,
        bool $writeToReplicas = true
    ): void {
        $this->connectionPoolFactory = new SentinelConnectionPoolFactory(
            $sentinels,
            $clusterId,
            $database,
            $timeout,
            $retryWait,
            $maxFails,
            $writeToReplicas
        );
    }

    /**
     * @param array{host: string, port: int} $master
     * @param array{array{host: string, port: int}} $slaves
     */
    public function setMultiConnectionPool(
        array $master,
        array $slaves,
        int $database = self::DEFAULT_DATABASE,
        float $timeout = self::DEFAULT_TIMEOUT_UNLIMITED,
        ?int $retryWait = null,
        ?int $maxFails = null,
        bool $writeToReplicas = true
    ): void {
        $this->connectionPoolFactory = new MultiConnectionPoolFactory(
            $master,
            $slaves,
            $database,
            $timeout,
            $retryWait,
            $maxFails,
            $writeToReplicas
        );
    }

    /**
     * @param array{host: string, port: int} $master
     * @param list<array{host: string, port: int}> $slaves
     */
    public function setMultiWriteConnectionPool(
        array $master,
        array $slaves,
        int $database = self::DEFAULT_DATABASE,
        float $timeout = self::DEFAULT_TIMEOUT_UNLIMITED,
        ?int $retryWait = null,
        ?int $maxFails = null,
        bool $writeToReplicas = true,
        string $strategy = MultiWriteConnectionPool::STRATEGY_RANDOM
    ): void {
        $this->connectionPoolFactory = new MultiWriteConnectionPoolFactory(
            [$master],
            $slaves,
            $database,
            $timeout,
            $retryWait,
            $maxFails,
            $writeToReplicas,
            $strategy
        );
    }

    public function resetConnectionPool(): void
    {
        if ($this->driver !== null) {
            $this->driver->connectionReset();
        }
    }

    /**
     * @throws RedisProxyException
     */
    private function prepareDriver(): void
    {
        if ($this->driver !== null) {
            return;
        }

        foreach ($this->driversOrder as $preferredDriver) {
            if ($preferredDriver === self::DRIVER_REDIS && extension_loaded('redis')) {
                $this->driver = new RedisDriver($this->connectionPoolFactory, $this->optSerializer);
                return;
            }
            if ($preferredDriver === self::DRIVER_PREDIS && class_exists('Predis\Client')) {
                $this->driver = new PredisDriver($this->connectionPoolFactory, $this->optSerializer);
                return;
            }
        }
        throw new RedisProxyException('No driver available');
    }

    /**
     * Set driver priorities - default is 1. redis, 2. predis
     *
     * @param list<string> $driversOrder
     * @return $this
     * @throws RedisProxyException if some driver is not supported
     */
    public function setDriversOrder(array $driversOrder): self
    {
        foreach ($driversOrder as $driver) {
            if (!in_array($driver, $this->supportedDrivers, true)) {
                throw new RedisProxyException('Driver "' . $driver . '" is not supported');
            }
        }
        $this->driversOrder = $driversOrder;
        return $this;
    }

    /**
     * @throws RedisProxyException
     */
    private function init(): void
    {
        $this->prepareDriver();
    }

    public function actualDriver(): ?string
    {
        if ($this->driver instanceof RedisDriver) {
            return self::DRIVER_REDIS;
        }
        if ($this->driver instanceof PredisDriver) {
            return self::DRIVER_PREDIS;
        }
        return null;
    }

    /**
     * @param string $name
     * @param array<int, mixed> $arguments
     * @throws RedisProxyException
     */
    public function __call(string $name, array $arguments): mixed
    {
        $this->init();
        $name = strtolower($name);
        return $this->driver?->call($name, $arguments);
    }

    /**
     * @param int $database
     * @return bool true on success
     * @throws RedisProxyException on failure
     */
    public function select(int $database): bool
    {
        $this->init();
        $result = $this->driver?->call('select', [$database]);
        return (bool) $result;
    }

    /**
     * @param string|null $section
     * @return array<string, mixed>
     * @throws RedisProxyException
     */
    public function info(?string $section = null): array
    {
        $this->init();
        $section = $section !== null ? strtolower($section) : $section;

        $result = $section === null
            ? $this->driver?->call('info')
            : $this->driver?->call('info', [$section]);

        if (!is_array($result)) {
            throw new RedisProxyException('Unexpected response type from INFO command');
        }

        $databases = null;
        if ($section === null || $section === 'keyspace') {
            $config = $this->config('get', 'databases');
            if (is_array($config) && isset($config['databases']) && is_int($config['databases'])) {
                $databases = $config['databases'];
            }
        }

        /** @var array<string, mixed> $groupedResult */
        $groupedResult = InfoHelper::createInfoArray($this, $result, $databases);

        if ($section === null) {
            return $groupedResult;
        }
        if (isset($groupedResult[$section]) && is_array($groupedResult[$section])) {
            /** @var array<string, mixed> */
            return $groupedResult[$section];
        }

        throw new RedisProxyException('Info section "' . $section . '" doesn\'t exist');
    }

    /**
     * Determine if a key exists
     * @throws RedisProxyException
     */
    public function exists(string $key): bool
    {
        $this->init();
        $result = $this->driver?->call('exists', [$key]);
        return (bool) $result;
    }

    /**
     * Get the value of a key
     * @return string|null null if key not set
     * @throws RedisProxyException
     */
    public function get(string $key): ?string
    {
        $this->init();
        $result = $this->driver?->call('get', [$key]);
        /** @var string|null $value */
        $value = $this->convertFalseToNull($result);
        return $value;
    }

    /**
     * Set the string value of a key and return its old value
     * @return string|null
     * @throws RedisProxyException
     */
    public function getset(string $key, string $value): ?string
    {
        $this->init();
        $result = $this->driver?->call('getset', [$key, $value]);
        /** @var string|null $oldValue */
        $oldValue = $this->convertFalseToNull($result);
        return $oldValue;
    }

    /**
     * Set a key's time to live in seconds
     * @return bool true if the timeout was set, false if key does not exist or the timeout could not be set
     * @throws RedisProxyException
     */
    public function expire(string $key, int $seconds): bool
    {
        $this->init();
        $result = $this->driver?->call('expire', [$key, $seconds]);
        return (bool) $result;
    }

    /**
     * Set a key's time to live in milliseconds
     * @return bool true if the timeout was set, false if key does not exist or the timeout could not be set
     * @throws RedisProxyException
     */
    public function pexpire(string $key, int $milliseconds): bool
    {
        $this->init();
        $result = $this->driver?->call('pexpire', [$key, $milliseconds]);
        return (bool) $result;
    }

    /**
     * Set the expiration for a key as a UNIX timestamp
     * @return bool true if the timeout was set, false if key does not exist or the timeout could not be set
     * @throws RedisProxyException
     */
    public function expireat(string $key, int $timestamp): bool
    {
        $this->init();
        $result = $this->driver?->call('expireat', [$key, $timestamp]);
        return (bool) $result;
    }

    /**
     * Set the expiration for a key as a UNIX timestamp specified in milliseconds
     * @return bool true if the timeout was set, false if key does not exist or the timeout could not be set
     * @throws RedisProxyException
     */
    public function pexpireat(string $key, int $millisecondsTimestamp): bool
    {
        $this->init();
        $result = $this->driver?->call('pexpireat', [$key, $millisecondsTimestamp]);
        return (bool) $result;
    }

    /**
     * Remove the expiration from a key
     * @throws RedisProxyException
     */
    public function persist(string $key): bool
    {
        $this->init();
        $result = $this->driver?->call('persist', [$key]);
        return (bool) $result;
    }

    /**
     * Set the value of a key, only if the key does not exist
     * @return bool true if the key was set, false if the key was not set
     * @throws RedisProxyException
     */
    public function setnx(string $key, string $value): bool
    {
        $this->init();
        $result = $this->driver?->call('setnx', [$key, $value]);
        return (bool) $result;
    }

    /**
     * Delete a key(s)
     * @param string|string[] ...$keys
     * @return int number of deleted keys
     * @throws RedisProxyException
     */
    public function del(...$keys): int
    {
        $keys = $this->prepareArguments('del', ...$keys);
        $this->init();
        $result = $this->driver?->call('del', [...$keys]);
        return $this->requireInt($result, 'DEL');
    }

    /**
     * Delete a key(s)
     * @param string|string[] ...$keys
     * @return int number of deleted keys
     * @throws RedisProxyException
     */
    public function delete(...$keys): int
    {
        return $this->del(...$keys);
    }

    /**
     * Increment the integer value of a key by one
     * @return int
     * @throws RedisProxyException
     */
    public function incr(string $key): int
    {
        $this->init();
        $result = $this->driver?->call('incr', [$key]);
        return $this->requireInt($result, 'INCR');
    }

    /**
     * Increment the integer value of a key by the given amount
     * @return int
     * @throws RedisProxyException
     */
    public function incrby(string $key, int $increment = 1): int
    {
        $this->init();
        $result = $this->driver?->call('incrby', [$key, $increment]);
        return $this->requireInt($result, 'INCRBY');
    }

    /**
     * Increment the float value of a key by the given amount
     * @return float
     * @throws RedisProxyException
     */
    public function incrbyfloat(string $key, float $increment = 1.0): float
    {
        $this->init();
        $result = $this->driver?->call('incrbyfloat', [$key, $increment]);
        if (!is_float($result) && !is_int($result)) {
            throw new RedisProxyException('Unexpected response type from INCRBYFLOAT command');
        }
        return (float) $result;
    }

    /**
     * Decrement the integer value of a key by one
     * @return int
     * @throws RedisProxyException
     */
    public function decr(string $key): int
    {
        $this->init();
        $result = $this->driver?->call('decr', [$key]);
        return $this->requireInt($result, 'DECR');
    }

    /**
     * Decrement the integer value of a key by the given number
     * @return int
     * @throws RedisProxyException
     */
    public function decrby(string $key, int $decrement = 1): int
    {
        $this->init();
        $result = $this->driver?->call('decrby', [$key, $decrement]);
        return $this->requireInt($result, 'DECRBY');
    }

    /**
     * Decrement the float value of a key by the given amount
     * @return float
     * @throws RedisProxyException
     */
    public function decrbyfloat(string $key, float $decrement = 1): float
    {
        return $this->incrbyfloat($key, (-1) * $decrement);
    }

    /**
     * Return a serialized version of the value stored at the specified key
     * @return string|null serialized value, null if key doesn't exist
     * @throws RedisProxyException
     */
    public function dump(string $key): ?string
    {
        $this->init();
        $result = $this->driver?->call('dump', [$key]);
        /** @var string|null $value */
        $value = $this->convertFalseToNull($result);
        return $value;
    }

    /**
     * Set multiple values to multiple keys
     * @param array<string, string>|list<string> $dictionary
     * @return bool true on success
     * @throws RedisProxyException if number of arguments is wrong
     */
    public function mset(...$dictionary): bool
    {
        $this->init();

        // mset(['k1' => 'v1', 'k2' => 'v2'])
        if (isset($dictionary[0])) {
            $result = $this->driver?->call('mset', [$dictionary[0]]);
            return (bool) $result;
        }

        // mset('k1', 'v1', 'k2', 'v2', ...)
        /** @phpstan-var list<mixed> $dictionary */
        $dictionary = $dictionary;

        $prepared = $this->prepareKeyValue($dictionary, 'mset');
        $result = $this->driver?->call('mset', [$prepared]);
        return (bool) $result;
    }

    /**
     * Multi get
     * @param string|string[] ...$keys
     * @return array<string, string|null> key => value
     * @throws RedisProxyException
     */
    public function mget(...$keys): array
    {
        $keys = array_values(array_unique($this->prepareArguments('mget', ...$keys)));
        $this->init();

        $raw = $this->driver?->call('mget', [$keys]);
        if (!is_array($raw)) {
            throw new RedisProxyException('Unexpected response type from MGET command');
        }

        $values = [];
        foreach ($raw as $value) {
            /** @var string|null $v */
            $v = $this->convertFalseToNull($value);
            $values[] = $v;
        }

        $keysStr = array_map(
            static function ($k): string {
                if (!is_scalar($k) && $k !== null) {
                    throw new InvalidArgumentException('Expected scalar or null, ' . get_debug_type($k) . ' given.');
                }
                return (string) $k;
            },
            $keys
        );
        /** @var array<string, string|null> $combined */
        $combined = array_combine($keysStr, $values) ?: [];
        return $combined;
    }

    /**
     * Incrementally iterate the keys space
     *
     * @param int|string|null $iterator iterator / cursor, use null for start scanning, when changed to 0 or '0', scanning is finished
     * @param string|null     $pattern  pattern for keys, use * as wild card
     * @param int|null        $count
     * @return array<int, string>|bool|null list of found keys, returns null if $iterator is 0 or '0'
     * @throws RedisProxyException
     */
    public function scan(int|string|null &$iterator, ?string $pattern = null, ?int $count = null): array|bool|null
    {
        if ($iterator === 0 || $iterator === '0') {
            return null;
        }

        $this->init();
        $result = $this->driver?->call('scan', [&$iterator, $pattern, $count]);

        if ($result === null || $result === false) {
            return $result;
        }

        if (!is_array($result)) {
            throw new RedisProxyException('Unexpected response type from SCAN command');
        }

        /** @var array<int, string>|bool|null $result */
        return $result;
    }

    /**
     * Get the value of a hash field
     * @return string|null null if hash field is not set
     * @throws RedisProxyException
     */
    public function hget(string $key, string $field): ?string
    {
        $this->init();
        $result = $this->driver?->call('hget', [$key, $field]);
        /** @var string|null $value */
        $value = $this->convertFalseToNull($result);
        return $value;
    }

    /**
     * Delete one or more hash fields, returns number of deleted fields
     * @param string|string[] ...$fields
     * @throws RedisProxyException
     */
    public function hdel(string $key, ...$fields): int
    {
        $fields = $this->prepareArguments('hdel', ...$fields);
        $this->init();
        $result = $this->driver?->call('hdel', [$key, ...$fields]);

        return $this->requireInt($result, 'hdel');
    }

    /**
     * Increment the integer value of hash field by given number
     * @throws RedisProxyException
     */
    public function hincrby(string $key, string $field, int $increment = 1): int
    {
        $this->init();
        $result = $this->driver?->call('hincrby', [$key, $field, $increment]);

        return $this->requireInt($result, 'hincrby');
    }

    /**
     * Increment the float value of hash field by given amount
     * @throws RedisProxyException
     */
    public function hincrbyfloat(string $key, string $field, float $increment = 1.0): float
    {
        $this->init();
        $result = $this->driver?->call('hincrbyfloat', [$key, $field, $increment]);
        if (!is_float($result) && !is_int($result)) {
            throw new RedisProxyException('Unexpected response type from HINCRBYFLOAT command');
        }
        return (float) $result;
    }

    /**
     * Sets the expiration of one or more fields in given hash, TTL is in seconds.
     *
     * @param string|string[] ...$fields fields to set expiration on
     * @return list<int>|null
     * @throws RedisProxyException
     */
    public function hexpire(string $key, int $seconds, ...$fields): ?array
    {
        $fields = $this->prepareArguments('hexpire', ...$fields);
        $this->init();
        $result = $this->driver?->call('hexpire', [$key, $seconds, ...$fields]);

        if ($result === null) {
            return null;
        }

        if (!is_array($result)) {
            throw new RedisProxyException('Unexpected response type from HEXPIRE command');
        }

        /** @var list<int> $result */
        return array_values($result);
    }

    /**
     * Set multiple values to multiple hash fields
     * @param string                     $key
     * @param array<string, string>|list<string> $dictionary
     * @return bool true on success
     * @throws RedisProxyException if number of arguments is wrong
     */
    public function hmset(string $key, ...$dictionary): bool
    {
        $this->init();

        // hmset($key, ['f1' => 'v1', 'f2' => 'v2'])
        if (isset($dictionary[0])) {
            $result = $this->driver?->call('hmset', [$key, $dictionary[0]]);
            return (bool) $result;
        }

        // hmset($key, 'f1', 'v1', 'f2', 'v2', ...)
        /** @phpstan-var list<mixed> $dictionary */
        $dictionary = $dictionary;

        $prepared = $this->prepareKeyValue($dictionary, 'hmset');
        $result = $this->driver?->call('hmset', [$key, $prepared]);
        return (bool) $result;
    }

    /**
     * Multi hash get
     * @param string               $key
     * @param string|string[] ...$fields
     * @return array<string, string|null>
     * @throws RedisProxyException
     */
    public function hmget(string $key, ...$fields): array
    {
        $fields = array_values(array_unique($this->prepareArguments('hmget', ...$fields)));
        $this->init();

        $raw = $this->driver?->call('hmget', [$key, $fields]);
        if (!is_array($raw)) {
            throw new RedisProxyException('Unexpected response type from HMGET command');
        }

        $values = [];
        foreach ($raw as $value) {
            /** @var string|null $v */
            $v = $this->convertFalseToNull($value);
            $values[] = $v;
        }

        /** @var list<string> $fieldsStr */
        $fieldsStr = array_map(
            static function ($k): string {
                if (!is_scalar($k) && $k !== null) {
                    throw new InvalidArgumentException('Expected scalar or null, ' . get_debug_type($k) . ' given.');
                }
                return (string) $k;
            },
            $fields
        );
        /** @var array<string, string|null> $combined */
        $combined = array_combine($fieldsStr, $values) ?: [];
        return $combined;
    }

    /**
     * Incrementally iterate hash fields and associated values
     *
     * @param int|string|null $iterator
     * @param string|null     $pattern
     * @param int|null        $count
     * @return array<string, string>|bool|null
     * @throws RedisProxyException
     */
    public function hscan(string $key, int|string|null &$iterator, ?string $pattern = null, ?int $count = null): array|bool|null
    {
        if ($iterator === 0 || $iterator === '0') {
            return null;
        }

        $this->init();
        $result = $this->driver?->call('hscan', [$key, &$iterator, $pattern, $count]);

        if ($result === null || $result === false) {
            return $result;
        }

        if (!is_array($result)) {
            throw new RedisProxyException('Unexpected response type from HSCAN command');
        }

        /** @var array<string, string>|bool|null $result */
        return $result;
    }

    /**
     * Add one or more members to a set
     * @param string          $key
     * @param string|string[] ...$members
     * @return int number of new members added to set
     * @throws RedisProxyException
     */
    public function sadd(string $key, ...$members): int
    {
        $members = $this->prepareArguments('sadd', ...$members);
        $this->init();
        $result = $this->driver?->call('sadd', [$key, ...$members]);
        return $this->requireInt($result, 'SADD');
    }

    /**
     * Remove and return one or multiple random members from a set
     * @param int|null $count number of members
     * @return string|array<int, string>|null
     * @throws RedisProxyException
     */
    public function spop(string $key, ?int $count = 1): string|array|null
    {
        $this->init();
        if ($count === 1 || $count === null) {
            $result = $this->driver?->call('spop', [$key]);
            /** @var string|null $value */
            $value = $this->convertFalseToNull($result);
            return $value;
        }

        $members = [];
        for ($i = 0; $i < $count; ++$i) {
            $member = $this->driver?->call('spop', [$key]);
            if ($member === false || $member === null) {
                break;
            }
            $members[] = $member;
        }

        if ($members === []) {
            return null;
        }

        /** @var array<int, string> $members */
        return $members;
    }

    /**
     * Incrementally iterate Set elements
     *
     * @param string          $key
     * @param int|string|null $iterator
     * @param string|null     $pattern
     * @param int|null        $count
     * @return array<int, string>|bool|null
     * @throws RedisProxyException
     */
    public function sscan(string $key, int|string|null &$iterator, ?string $pattern = null, ?int $count = null): array|bool|null
    {
        if ($iterator === 0 || $iterator === '0') {
            return null;
        }
        $this->init();
        $result = $this->driver?->call('sscan', [$key, &$iterator, $pattern, $count]);

        if ($result === null || $result === false) {
            return $result;
        }

        if (!is_array($result)) {
            throw new RedisProxyException('Unexpected response type from SSCAN command');
        }

        /** @var array<int, string>|bool|null $result */
        return $result;
    }

    /**
     * Remove the specified members from the set stored at key. Non-existing members are ignored
     * @param string          $key
     * @param string|string[] ...$members
     * @return int
     * @throws RedisProxyException
     */
    public function srem(string $key, ...$members): int
    {
        $members = $this->prepareArguments('srem', ...$members);
        $this->init();
        $result = $this->driver?->call('srem', [$key, ...$members]);
        return $this->requireInt($result, 'SREM');
    }

    /**
     * Prepend one or multiple values to a list
     * @param string               $key
     * @param string|string[] ...$elements
     * @return int the length of the list after the push operations
     * @throws RedisProxyException
     */
    public function lpush(string $key, ...$elements): int
    {
        $elements = $this->prepareArguments('lpush', ...$elements);
        $this->init();
        $result = $this->driver?->call('lpush', [$key, ...$elements]);
        return $this->requireInt($result, 'LPUSH');
    }

    /**
     * Append one or multiple values to a list
     * @param string               $key
     * @param string|string[] ...$elements
     * @return int the length of the list after the push operations
     * @throws RedisProxyException
     */
    public function rpush(string $key, ...$elements): int
    {
        $elements = $this->prepareArguments('rpush', ...$elements);
        $this->init();
        $result = $this->driver?->call('rpush', [$key, ...$elements]);
        return $this->requireInt($result, 'RPUSH');
    }

    /**
     * Remove and get the first element in a list
     * @return string|null
     * @throws RedisProxyException
     */
    public function lpop(string $key): ?string
    {
        $this->init();
        $result = $this->driver?->call('lpop', [$key]);
        /** @var string|null $value */
        $value = $this->convertFalseToNull($result);
        return $value;
    }

    /**
     * Remove and get the last element in a list
     * @return string|null
     * @throws RedisProxyException
     */
    public function rpop(string $key): ?string
    {
        $this->init();
        $result = $this->driver?->call('rpop', [$key]);
        /** @var string|null $value */
        $value = $this->convertFalseToNull($result);
        return $value;
    }

    /**
     * Get an element from a list by its index
     * @param string $key
     * @param int    $index
     * @return string|null
     * @throws RedisProxyException
     */
    public function lindex(string $key, int $index = 0): ?string
    {
        $this->init();
        $result = $this->driver?->call('lindex', [$key, $index]);
        /** @var string|null $value */
        $value = $this->convertFalseToNull($result);
        return $value;
    }

    /**
     * Add one or more members to a sorted set, or update its score if it already exists
     * @param string                      $key
     * @param array<int, mixed>|mixed ...$dictionary
     * @return int
     * @throws RedisProxyException
     */
    public function zadd(string $key, ...$dictionary): int
    {
        $this->init();

        if (isset($dictionary[0]) && is_array($dictionary[0])) {
            $return = 0;
            /** @var array<string, float|int> $map */
            $map = $dictionary[0];
            foreach ($map as $member => $score) {
                $res = $this-> zadd($key, (float) $score, $member);
                $return += $res;
            }
            return $return;
        }

        $result = $this->driver?->call('zadd', [$key, ...$dictionary]);
        return $this->requireInt($result, 'ZADD');
    }

    /**
     * Removes the specified members from the sorted set stored at key. Non-existing members are ignored
     * @param string          $key
     * @param string|string[] ...$members
     * @return int
     * @throws RedisProxyException
     */
    public function zrem(string $key, ...$members): int
    {
        $members = $this->prepareArguments('zrem', ...$members);
        $this->init();
        $result = $this->driver?->call('zrem', [$key, ...$members]);
        return $this->requireInt($result, 'ZREM');
    }

    /**
     * Incrementally iterate Sorted set elements
     *
     * @param string          $key
     * @param int|string|null $iterator
     * @param string|null     $pattern
     * @param int|null        $count
     * @return array<int, string|float>|bool|null
     * @throws RedisProxyException
     */
    public function zscan(string $key, int|string|null &$iterator, ?string $pattern = null, ?int $count = null): array|bool|null
    {
        if ($iterator === 0 || $iterator === '0') {
            return null;
        }

        $this->init();
        $result = $this->driver?->call('zscan', [$key, &$iterator, $pattern, $count]);

        if ($result === null || $result === false) {
            return $result;
        }

        if (!is_array($result)) {
            throw new RedisProxyException('Unexpected response type from ZSCAN command');
        }

        /** @var array<int, string|float>|bool|null $result */
        return $result;
    }

    /**
     * Returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
     * @return int|null
     * @throws RedisProxyException
     */
    public function zrank(string $key, string $member): ?int
    {
        $this->init();
        $result = $this->driver?->call('zrank', [$key, $member]);
        /** @var int|null $value */
        $value = $this->convertFalseToNull($result);
        return $value;
    }

    /**
     * Returns the rank of member in the sorted set stored at key, with the scores ordered from high to low.
     * @return int|null
     * @throws RedisProxyException
     */
    public function zrevrank(string $key, string $member): ?int
    {
        $this->init();
        $result = $this->driver?->call('zrevrank', [$key, $member]);
        /** @var int|null $value */
        $value = $this->convertFalseToNull($result);
        return $value;
    }

    /**
     * Renames key to newkey
     */
    public function rename(string $key, string $newKey): bool
    {
        $this->init();
        try {
            $result = $this->driver?->call('rename', [$key, $newKey]);
        } catch (RedisProxyException $exception) {
            return false;
        }
        return (bool) $result;
    }

    /**
     * Subscribes the client to the specified channels
     *
     * @param callable $callback
     * @param string|string[] ...$channels
     * @return array<int, mixed>
     * @throws RedisProxyException
     */
    public function subscribe(callable $callback, ...$channels): array
    {
        $channels = $this->prepareArguments('subscribe', ...$channels);
        $this->init();
        $result = $this->driver?->call('subscribe', [...$channels, $callback]);

        if (!is_array($result)) {
            throw new RedisProxyException('Unexpected response type from SUBSCRIBE command');
        }

        /** @var array<int, mixed> $result */
        return $result;
    }

    /**
     * Create array from input array - odd keys are used as keys, even keys are used as values
     *
     * @param list<mixed> $dictionary
     * @return array<string, string>
     * @throws RedisProxyException if number of keys is not the same as number of values
     */
    private function prepareKeyValue(array $dictionary, string $command): array
    {
        $keys = array_values(
            array_filter(
                $dictionary,
                static function (int $key): bool {
                    return $key % self::MODULO_BASE === self::INDEX_EVEN;
                },
                ARRAY_FILTER_USE_KEY
            )
        );

        $values = array_values(
            array_filter(
                $dictionary,
                static function (int $key): bool {
                    return $key % self::MODULO_BASE === self::INDEX_ODD;
                },
                ARRAY_FILTER_USE_KEY
            )
        );

        if (count($keys) !== count($values)) {
            throw new RedisProxyException("Wrong number of arguments for $command command");
        }

        $keysStr = array_map(
            static function ($k): string {
                if (!is_scalar($k) && $k !== null) {
                    throw new InvalidArgumentException('Expected scalar or null, ' . get_debug_type($k) . ' given.');
                }
                return (string) $k;
            },
            $keys
        );

        /** @var list<string> $valuesStr */
        $valuesStr = array_map(
            static function ($k): string {
                if (!is_scalar($k) && $k !== null) {
                    throw new InvalidArgumentException('Expected scalar or null, ' . get_debug_type($k) . ' given.');
                }
                return (string) $k;
            },
            $values
        );
        /** @var array<string, string> $combined */
        $combined = array_combine($keysStr, $valuesStr) ?: [];
        return $combined;
    }

    /**
     * @param string $command
     * @param mixed  ...$params
     * @return list<mixed>
     * @throws RedisProxyException
     */
    private function prepareArguments(string $command, ...$params): array
    {
        if (!isset($params[0])) {
            throw new RedisProxyException("Wrong number of arguments for $command command");
        }

        if (is_array($params[0])) {
            /** @var list<mixed> $list */
            $list = array_values($params[0]);
            return $list;
        }

        /** @var list<mixed> $params */
        return array_values($params);
    }

    /**
     * Returns null instead of false
     *
     * @template T
     * @param T|false|null $result
     * @return T|null
     */
    private function convertFalseToNull(mixed $result): mixed
    {
        return $result === false ? null : $result;
    }

    /**
     * Ensure the given result is an int, otherwise throw.
     */
    private function requireInt(mixed $result, string $command): int
    {
        if (is_int($result)) {
            return $result;
        }
        if (is_string($result) && ctype_digit($result)) {
            return (int) $result;
        }
        throw new RedisProxyException("Unexpected response type from $command command");
    }
}

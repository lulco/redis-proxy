<?php

namespace RedisProxy;

use Exception;
use Predis\Client;
use Predis\Response\Status;
use Redis;

/**
 * @method mixed config(string $command, $argument = null)
 * @method int dbsize() Return the number of keys in the selected database
 * @method boolean restore(string $key, int $ttl, string $serializedValue) Create a key using the provided serialized value, previously obtained using DUMP. If ttl is 0 the key is created without any expire, otherwise the specified expire time (in milliseconds) is set
 * @method boolean set(string $key, string $value) Set the string value of a key
 * @method boolean setex(string $key, int $seconds, string $value) Set the value and expiration of a key
 * @method int ttl(string $key) Get the time to live for a key, returns TTL in seconds, -2 if the key does not exist, -1 if the key exists but has no associated expire
 * @method int pttl(string $key) Get the time to live for a key in milliseconds, returns TTL in miliseconds, -2 if the key does not exist, -1 if the key exists but has no associated expire
 * @method array keys(string $pattern) Find all keys matching the given pattern
 * @method int hset(string $key, string $field, string $value) Set the string value of a hash field
 * @method array hkeys(string $key) Get all fields in a hash (without values)
 * @method array hgetall(string $key) Get all fields and values in a hash
 * @method int hlen(string $key) Get the number of fields in a hash
 * @method array smembers(string $key) Get all the members in a set
 * @method int scard(string $key) Get the number of members in a set
 * @method int llen(string $key) Get the length of a list
 * @method bool lset(string $key, string $index, string $value) Sets the list element at index to value
 * @method array lrange(string $key, int $start, int $stop) Get a range of elements from a list
 * @method int lrem(string $key, string $value) Removes the first count occurrences of elements equal to value from the list stored at key.
 * @method int zcard(string $key) Get the number of members in a sorted set
 * @method boolean flushall() Remove all keys from all databases
 * @method boolean flushdb() Remove all keys from the current database
 */
class RedisProxy
{
    const DRIVER_REDIS = 'redis';

    const DRIVER_PREDIS = 'predis';

    const TYPE_STRING = 'string';

    const TYPE_SET = 'set';

    const TYPE_HASH = 'hash';

    const TYPE_LIST = 'list';

    const TYPE_SORTED_SET = 'sorted_set';

    private $driver;

    private $host;

    private $port;

    private $database = 0;

    private $selectedDatabase = 0;

    private $timeout;

    private $supportedDrivers = [
        self::DRIVER_REDIS,
        self::DRIVER_PREDIS,
    ];

    private $driversOrder = [];

    private $redisTypeMap = [
        self::DRIVER_REDIS => [
            1 => self::TYPE_STRING,
            2 => self::TYPE_SET,
            3 => self::TYPE_LIST,
            4 => self::TYPE_SORTED_SET,
            5 => self::TYPE_HASH,
        ],
        self::DRIVER_PREDIS => [
            'string' => self::TYPE_STRING,
            'set' => self::TYPE_SET,
            'list' => self::TYPE_LIST,
            'zset' => self::TYPE_SORTED_SET,
            'hash' => self::TYPE_HASH,
        ],
    ];

    public function __construct($host, $port, $database = 0, $timeout = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->database = $database;
        $this->timeout = $timeout;
        $this->driversOrder = $this->supportedDrivers;
    }

    /**
     * Set driver priorities - default is 1. redis, 2. predis
     * @param array $driversOrder
     * @return RedisProxy
     * @throws RedisProxyException if some driver is not supported
     */
    public function setDriversOrder(array $driversOrder)
    {
        foreach ($driversOrder as $driver) {
            if (!in_array($driver, $this->supportedDrivers)) {
                throw new RedisProxyException('Driver "' . $driver . '" is not supported');
            }
        }
        $this->driversOrder = $driversOrder;
        return $this;
    }

    private function init()
    {
        $this->prepareDriver();
        $this->select($this->database);
    }

    private function prepareDriver()
    {
        if ($this->driver !== null) {
            return;
        }

        foreach ($this->driversOrder as $preferredDriver) {
            if ($preferredDriver === self::DRIVER_REDIS && extension_loaded('redis')) {
                $this->driver = new Redis();
                return;
            }
            if ($preferredDriver === self::DRIVER_PREDIS && class_exists('Predis\Client')) {
                $this->driver = new Client([
                    'host' => $this->host,
                    'port' => $this->port,
                ]);
                return;
            }
        }
        throw new RedisProxyException('No driver available');
    }

    /**
     * @return string|null
     */
    public function actualDriver()
    {
        if ($this->driver instanceof Redis) {
            return self::DRIVER_REDIS;
        }
        if ($this->driver instanceof Client) {
            return self::DRIVER_PREDIS;
        }
        return null;
    }

    private function connect($host, $port, $timeout = null)
    {
        return $this->driver->connect($host, $port, $timeout);
    }

    private function isConnected()
    {
        return $this->driver->isConnected();
    }

    public function __call($name, $arguments)
    {
        $this->init();
        $name = strtolower($name);
        try {
            $result = call_user_func_array([$this->driver, $name], $arguments);
        } catch (Exception $e) {
            throw new RedisProxyException("Error for command '$name', use getPrevious() for more info", 1484162284, $e);
        }
        return $this->transformResult($result);
    }

    /**
     * @param int $database
     * @return boolean true on success
     * @throws RedisProxyException on failure
     */
    public function select($database)
    {
        $this->prepareDriver();
        if (!$this->isConnected()) {
            $this->connect($this->host, $this->port, $this->timeout);
        }
        if ($database == $this->selectedDatabase) {
            return true;
        }
        try {
            $result = $this->driver->select($database);
        } catch (Exception $e) {
            throw new RedisProxyException('Invalid DB index');
        }
        $result = $this->transformResult($result);
        if ($result === false) {
            throw new RedisProxyException('Invalid DB index');
        }
        $this->database = $database;
        $this->selectedDatabase = $database;
        return $result;
    }

    /**
     * @param string|null $section
     * @return array
     */
    public function info($section = null)
    {
        $this->init();
        $section = $section ? strtolower($section) : $section;
        $result = $section === null ? $this->driver->info() : $this->driver->info($section);

        $databases = $section === null || $section === 'keyspace' ? $this->config('get', 'databases')['databases'] : null;
        $groupedResult = InfoHelper::createInfoArray($this, $result, $databases);
        if ($section === null) {
            return $groupedResult;
        }
        if (isset($groupedResult[$section])) {
            return $groupedResult[$section];
        }
        throw new RedisProxyException('Info section "' . $section . '" doesn\'t exist');
    }

    /**
     * Determine if a key exists
     * @param string $key
     * @return boolean
     */
    public function exists($key)
    {
        $this->init();
        $result = $this->driver->exists($key);
        return (bool)$result;
    }

    /**
     * @param string $key
     * @return string|null
     */
    public function type($key)
    {
        $this->init();
        $result = $this->driver->type($key);
        $result = $this->actualDriver() === self::DRIVER_PREDIS && $result instanceof Status ? $result->getPayload() : $result;
        return isset($this->redisTypeMap[$this->actualDriver()][$result]) ? $this->redisTypeMap[$this->actualDriver()][$result] : null;
    }

    /**
     * Get the value of a key
     * @param string $key
     * @return string|null null if key not set
     */
    public function get($key)
    {
        $this->init();
        $result = $this->driver->get($key);
        return $this->convertFalseToNull($result);
    }

    /**
     * Set the string value of a key and return its old value
     * @param string $key
     * @param string $value
     * @return string|null null if key was not set before
     */
    public function getset($key, $value)
    {
        $this->init();
        $result = $this->driver->getset($key, $value);
        return $this->convertFalseToNull($result);
    }

    /**
     * Set a key's time to live in seconds
     * @param string $key
     * @param int $seconds
     * @return boolean true if the timeout was set, false if key does not exist or the timeout could not be set
     */
    public function expire($key, $seconds)
    {
        $this->init();
        $result = $this->driver->expire($key, $seconds);
        return (bool)$result;
    }

    /**
     * Set a key's time to live in milliseconds
     * @param string $key
     * @param int $miliseconds
     * @return boolean true if the timeout was set, false if key does not exist or the timeout could not be set
     */
    public function pexpire($key, $miliseconds)
    {
        $this->init();
        $result = $this->driver->pexpire($key, $miliseconds);
        return (bool)$result;
    }

    /**
     * Set the expiration for a key as a UNIX timestamp
     * @param string $key
     * @param int $timestamp
     * @return boolean true if the timeout was set, false if key does not exist or the timeout could not be set
     */
    public function expireat($key, $timestamp)
    {
        $this->init();
        $result = $this->driver->expireat($key, $timestamp);
        return (bool)$result;
    }

    /**
     * Set the expiration for a key as a UNIX timestamp specified in milliseconds
     * @param string $key
     * @param int $milisecondsTimestamp
     * @return boolean true if the timeout was set, false if key does not exist or the timeout could not be set
     */
    public function pexpireat($key, $milisecondsTimestamp)
    {
        $this->init();
        $result = $this->driver->pexpireat($key, $milisecondsTimestamp);
        return (bool)$result;
    }

    /**
     * Set the value and expiration in milliseconds of a key
     * @param string $key
     * @param int $miliseconds
     * @param string $value
     * @return boolean
     */
    public function psetex($key, $miliseconds, $value)
    {
        $this->init();
        $result = $this->driver->psetex($key, $miliseconds, $value);
        if ($result == '+OK') {
            return true;
        }
        return $this->transformResult($result);
    }

    /**
     * Remove the expiration from a key
     * @param string $key
     * @return boolean
     */
    public function persist($key)
    {
        $this->init();
        $result = $this->driver->persist($key);
        return (bool)$result;
    }

    /**
     * Set the value of a key, only if the key does not exist
     * @param string $key
     * @param string $value
     * @return boolean true if the key was set, false if the key was not set
     */
    public function setnx($key, $value)
    {
        $this->init();
        $result = $this->driver->setnx($key, $value);
        return (bool)$result;
    }

    /**
     * Delete a key(s)
     * @param array $keys
     * @return int number of deleted keys
     */
    public function del(...$keys)
    {
        $this->prepareArguments('del', ...$keys);
        $this->init();
        return $this->driver->del(...$keys);
    }

    /**
     * Delete a key(s)
     * @param array $keys
     * @return int number of deleted keys
     */
    public function delete(...$keys)
    {
        return $this->del(...$keys);
    }

    /**
     * Increment the integer value of a key by one
     * @param string $key
     * @return integer
     */
    public function incr($key)
    {
        $this->init();
        return $this->driver->incr($key);
    }

    /**
     * Increment the integer value of a key by the given amount
     * @param string $key
     * @param integer $increment
     * @return integer
     */
    public function incrby($key, $increment = 1)
    {
        $this->init();
        return $this->driver->incrby($key, (int)$increment);
    }

    /**
     * Increment the float value of a key by the given amount
     * @param string $key
     * @param float $increment
     * @return float
     */
    public function incrbyfloat($key, $increment = 1)
    {
        $this->init();
        return $this->driver->incrbyfloat($key, $increment);
    }

    /**
     * Decrement the integer value of a key by one
     * @param string $key
     * @return integer
     */
    public function decr($key)
    {
        $this->init();
        return $this->driver->decr($key);
    }

    /**
     * Decrement the integer value of a key by the given number
     * @param string $key
     * @param integer $decrement
     * @return integer
     */
    public function decrby($key, $decrement = 1)
    {
        $this->init();
        return $this->driver->decrby($key, (int)$decrement);
    }

    /**
     * Decrement the float value of a key by the given amount
     * @param string $key
     * @param float $decrement
     * @return float
     */
    public function decrbyfloat($key, $decrement = 1)
    {
        return $this->incrbyfloat($key, (-1) * $decrement);
    }

    /**
     * Return a serialized version of the value stored at the specified key
     * @param string $key
     * @return string|null serialized value, null if key doesn't exist
     */
    public function dump($key)
    {
        $this->init();
        $result = $this->driver->dump($key);
        return $this->convertFalseToNull($result);
    }

    /**
     * Set multiple values to multiple keys
     * @param array $dictionary
     * @return boolean true on success
     * @throws RedisProxyException if number of arguments is wrong
     */
    public function mset(...$dictionary)
    {
        $this->init();
        if (is_array($dictionary[0])) {
            $result = $this->driver->mset(...$dictionary);
            return $this->transformResult($result);
        }
        $dictionary = $this->prepareKeyValue($dictionary, 'mset');
        $result = $this->driver->mset($dictionary);
        return $this->transformResult($result);
    }

    /**
     * Multi get
     * @param array $keys
     * @return array Returns the values for all specified keys. For every key that does not hold a string value or does not exist, null is returned
     */
    public function mget(...$keys)
    {
        $keys = array_unique($this->prepareArguments('mget', ...$keys));
        $this->init();
        $values = [];
        foreach ($this->driver->mget($keys) as $value) {
            $values[] = $this->convertFalseToNull($value);
        }
        return array_combine($keys, $values);
    }

    /**
     * Incrementally iterate the keys space
     * @param mixed $iterator iterator / cursor, use $iterator = null for start scanning, when $iterator is changed to 0 or '0', scanning is finished
     * @param string $pattern pattern for keys, use * as wild card
     * @param int $count
     * @return array|boolean|null list of found keys, returns null if $iterator is 0 or '0'
     */
    public function scan(&$iterator, $pattern = null, $count = null)
    {
        if ((string)$iterator === '0') {
            return null;
        }
        $this->init();
        if ($this->actualDriver() === self::DRIVER_PREDIS) {
            $returned = $this->driver->scan($iterator, ['match' => $pattern, 'count' => $count]);
            $iterator = $returned[0];
            return $returned[1];
        }
        return $this->driver->scan($iterator, $pattern, $count);
    }

    /**
     * Get the value of a hash field
     * @param string $key
     * @param string $field
     * @return string|null null if hash field is not set
     */
    public function hget($key, $field)
    {
        $this->init();
        $result = $this->driver->hget($key, $field);
        return $this->convertFalseToNull($result);
    }

    /**
     * Delete one or more hash fields, returns number of deleted fields
     * @param array $key
     * @param array $fields
     * @return int
     */
    public function hdel($key, ...$fields)
    {
        $fields = $this->prepareArguments('hdel', ...$fields);
        $this->init();
        return $this->driver->hdel($key, ...$fields);
    }

    /**
     * Increment the integer value of hash field by given number
     * @param string $key
     * @param string $field
     * @param int $increment
     * @return int
     */
    public function hincrby($key, $field, $increment = 1)
    {
        $this->init();
        return $this->driver->hincrby($key, $field, (int)$increment);
    }

    /**
     * Increment the float value of hash field by given amount
     * @param string $key
     * @param string $field
     * @param float $increment
     * @return float
     */
    public function hincrbyfloat($key, $field, $increment = 1)
    {
        $this->init();
        return $this->driver->hincrbyfloat($key, $field, $increment);
    }

    /**
     * Set multiple values to multiple hash fields
     * @param string $key
     * @param array $dictionary
     * @return boolean true on success
     * @throws RedisProxyException if number of arguments is wrong
     */
    public function hmset($key, ...$dictionary)
    {
        $this->init();
        if (is_array($dictionary[0])) {
            $result = $this->driver->hmset($key, ...$dictionary);
            return $this->transformResult($result);
        }
        $dictionary = $this->prepareKeyValue($dictionary, 'hmset');
        $result = $this->driver->hmset($key, $dictionary);
        return $this->transformResult($result);
    }

    /**
     * Multi hash get
     * @param string $key
     * @param array $fields
     * @return array Returns the values for all specified fields. For every field that does not hold a string value or does not exist, null is returned
     */
    public function hmget($key, ...$fields)
    {
        $fields = array_unique($this->prepareArguments('hmget', ...$fields));
        $this->init();
        $values = [];
        foreach ($this->driver->hmget($key, $fields) as $value) {
            $values[] = $this->convertFalseToNull($value);
        }
        return array_combine($fields, $values);
    }

    /**
     * Incrementally iterate hash fields and associated values
     * @param string $key
     * @param mixed $iterator iterator / cursor, use $iterator = null for start scanning, when $iterator is changed to 0 or '0', scanning is finished
     * @param string $pattern pattern for fields, use * as wild card
     * @param int $count
     * @return array|boolean|null list of found fields with associated values, returns null if $iterator is 0 or '0'
     */
    public function hscan($key, &$iterator, $pattern = null, $count = null)
    {
        if ((string)$iterator === '0') {
            return null;
        }
        $this->init();
        if ($this->actualDriver() === self::DRIVER_PREDIS) {
            $returned = $this->driver->hscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
            $iterator = $returned[0];
            return $returned[1];
        }
        return $this->driver->hscan($key, $iterator, $pattern, $count);
    }

    /**
     * Add one or more members to a set
     * @param string $key
     * @param array $members
     * @return int number of new members added to set
     */
    public function sadd($key, ...$members)
    {
        $members = $this->prepareArguments('sadd', ...$members);
        $this->init();
        return $this->driver->sadd($key, ...$members);
    }

    /**
     * Remove and return one or multiple random members from a set
     * @param string $key
     * @param int $count number of members
     * @return mixed string if $count is null or 1 and $key exists, array if $count > 1 and $key exists, null if $key doesn't exist
     */
    public function spop($key, $count = 1)
    {
        $this->init();
        if ($count == 1 || $count === null) {
            $result = $this->driver->spop($key);
            return $this->convertFalseToNull($result);
        }

        $members = [];
        for ($i = 0; $i < $count; ++$i) {
            $member = $this->driver->spop($key);
            if (!$member) {
                break;
            }
            $members[] = $member;
        }
        return empty($members) ? null : $members;
    }

    /**
     * Incrementally iterate Set elements
     * @param string $key
     * @param mixed $iterator iterator / cursor, use $iterator = null for start scanning, when $iterator is changed to 0 or '0', scanning is finished
     * @param string $pattern pattern for member's values, use * as wild card
     * @param int $count
     * @return array|boolean|null list of found members, returns null if $iterator is 0 or '0'
     */
    public function sscan($key, &$iterator, $pattern = null, $count = null)
    {
        if ((string)$iterator === '0') {
            return null;
        }
        $this->init();
        if ($this->actualDriver() === self::DRIVER_PREDIS) {
            $returned = $this->driver->sscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
            $iterator = $returned[0];
            return $returned[1];
        }
        return $this->driver->sscan($key, $iterator, $pattern, $count);
    }

    /**
     * Prepend one or multiple values to a list
     * @param string $key
     * @param array $elements
     * @return int the length of the list after the push operations
     */
    public function lpush($key, ...$elements)
    {
        $elements = $this->prepareArguments('lpush', ...$elements);
        $this->init();
        return $this->driver->lpush($key, ...$elements);
    }

    /**
     * Append one or multiple values to a list
     * @param string $key
     * @param array $elements
     * @return int the length of the list after the push operations
     */
    public function rpush($key, ...$elements)
    {
        $elements = $this->prepareArguments('rpush', ...$elements);
        $this->init();
        return $this->driver->rpush($key, ...$elements);
    }

    /**
     * Remove and get the first element in a list
     * @param string $key
     * @return string|null
     */
    public function lpop($key)
    {
        $this->init();
        $result = $this->driver->lpop($key);
        return $this->convertFalseToNull($result);
    }

    /**
     * Remove and get the last element in a list
     * @param string $key
     * @return string|null
     */
    public function rpop($key)
    {
        $this->init();
        $result = $this->driver->rpop($key);
        return $this->convertFalseToNull($result);
    }

    /**
     * Get an element from a list by its index
     * @param string $key
     * @param int $index zero-based, so 0 means the first element, 1 the second element and so on. -1 means the last element, -2 means the penultimate and so forth
     * @return string|null
     */
    public function lindex($key, $index)
    {
        $this->init();
        $result = $this->driver->lindex($key, $index);
        return $this->convertFalseToNull($result);
    }

    /**
     * Add one or more members to a sorted set, or update its score if it already exists
     * @param string $key
     * @param array $dictionary (score1, member1[, score2, member2]) or associative array: [member1 => score1, member2 => score2]
     * @return int
     */
    public function zadd($key, ...$dictionary)
    {
        $this->init();
        if (is_array($dictionary[0])) {
            $return = 0;
            foreach ($dictionary[0] as $member => $score) {
                $res = $this->zadd($key, $score, $member);
                $return += $res;
            }
            return $return;
        }
        return $this->driver->zadd($key, ...$dictionary);
    }

    /**
     * Return a range of members in a sorted set, by index
     * @param string $key
     * @param int $start
     * @param int $stop
     * @param boolean $withscores
     * @return array
     */
    public function zrange($key, $start, $stop, $withscores = false)
    {
        $this->init();
        if ($this->actualDriver() === self::DRIVER_PREDIS) {
            return $this->driver->zrange($key, $start, $stop, ['WITHSCORES' => $withscores]);
        }
        return $this->driver->zrange($key, $start, $stop, $withscores);
    }

    /**
     * Incrementally iterate Sorted set elements
     * @param string $key
     * @param mixed $iterator iterator / cursor, use $iterator = null for start scanning, when $iterator is changed to 0 or '0', scanning is finished
     * @param string $pattern pattern for member's values, use * as wild card
     * @param int $count
     * @return array|boolean|null list of found members with their values, returns null if $iterator is 0 or '0'
     */
    public function zscan($key, &$iterator, $pattern = null, $count = null)
    {
        if ((string)$iterator === '0') {
            return null;
        }
        $this->init();
        if ($this->actualDriver() === self::DRIVER_PREDIS) {
            $returned = $this->driver->zscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
            $iterator = $returned[0];
            return $returned[1];
        }
        return $this->driver->zscan($key, $iterator, $pattern, $count);
    }

    /**
     * Return a range of members in a sorted set, by index, with scores ordered from high to low
     * @param string $key
     * @param int $start
     * @param int $stop
     * @param boolean $withscores
     * @return array
     */
    public function zrevrange($key, $start, $stop, $withscores = false)
    {
        $this->init();
        if ($this->actualDriver() === self::DRIVER_PREDIS) {
            return $this->driver->zrevrange($key, $start, $stop, ['WITHSCORES' => $withscores]);
        }
        return $this->driver->zrevrange($key, $start, $stop, $withscores);
    }

    /**
     * Returns null instead of false for Redis driver
     * @param mixed $result
     * @return mixed
     */
    private function convertFalseToNull($result)
    {
        return $this->actualDriver() === self::DRIVER_REDIS && $result === false ? null : $result;
    }

    /**
     * Transforms Predis result Payload to boolean
     * @param mixed $result
     * @return mixed
     */
    private function transformResult($result)
    {
        if ($this->actualDriver() === self::DRIVER_PREDIS && $result instanceof Status) {
            $result = $result->getPayload() === 'OK';
        }
        return $result;
    }

    /**
     * Create array from input array - odd keys are used as keys, even keys are used as values
     * @param array $dictionary
     * @param string $command
     * @return array
     * @throws RedisProxyException if number of keys is not the same as number of values
     */
    private function prepareKeyValue(array $dictionary, $command)
    {
        $keys = array_values(array_filter($dictionary, function ($key) {
            return $key % 2 == 0;
        }, ARRAY_FILTER_USE_KEY));
        $values = array_values(array_filter($dictionary, function ($key) {
            return $key % 2 == 1;
        }, ARRAY_FILTER_USE_KEY));

        if (count($keys) != count($values)) {
            throw new RedisProxyException("Wrong number of arguments for $command command");
        }
        return array_combine($keys, $values);
    }

    private function prepareArguments($command, ...$params)
    {
        if (!isset($params[0])) {
            throw new RedisProxyException("Wrong number of arguments for $command command");
        }
        if (is_array($params[0])) {
            $params = $params[0];
        }
        return $params;
    }
}

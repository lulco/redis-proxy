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
 * @method boolean flushall() Remove all keys from all databases
 * @method boolean flushdb() Remove all keys from the current database
 */
class RedisProxy
{
    use CommonBehavior;

    use SortedSetBehavior;

    use ListBehavior;

    const DRIVER_REDIS = 'redis';

    const DRIVER_PREDIS = 'predis';

    const TYPE_STRING = 'string';

    const TYPE_SET = 'set';

    const TYPE_HASH = 'hash';

    const TYPE_LIST = 'list';

    const TYPE_SORTED_SET = 'sorted_set';

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
            Redis::REDIS_STRING => self::TYPE_STRING,
            Redis::REDIS_SET => self::TYPE_SET,
            Redis::REDIS_HASH => self::TYPE_HASH,
            Redis::REDIS_LIST => self::TYPE_LIST,
            Redis::REDIS_ZSET => self::TYPE_SORTED_SET,
        ],
        self::DRIVER_PREDIS => [
            'string' => self::TYPE_STRING,
            'set' => self::TYPE_SET,
            'hash' => self::TYPE_HASH,
            'list' => self::TYPE_LIST,
            'zset' => self::TYPE_SORTED_SET,
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
}

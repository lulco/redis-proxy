<?php

namespace RedisProxy;

use Exception;
use Predis\Client;
use Predis\Response\Status;
use Redis;

/**
 * @method mixed config(string $command, $argument = null)
 * @method integer dbsize() Return the number of keys in the selected database
 * @method boolean set(string $key, string $value) Set the string value of a key
 * @method array keys(string $pattern) Find all keys matching the given pattern
 * @method integer hset(string $key, string $field, string $value) Set the string value of a hash field
 * @method array hkeys(string $key) Get all fields in a hash (without values)
 * @method array hgetall(string $key) Get all fields and values in a hash
 * @method integer hlen(string $key) Get the number of fields in a hash
 * @method array smembers(string $key) Get all the members in a set
 * @method integer scard(string $key) Get the number of members in a set
 * @method boolean flushall() Remove all keys from all databases
 * @method boolean flushdb() Remove all keys from the current database
 */
class RedisProxy
{
    const DRIVER_REDIS = 'redis';

    const DRIVER_PREDIS = 'predis';

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
                $this->driver = new Client();
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
     * @param integer $database
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
        $groupedResult = InfoHelper::createInfoArray($this->driver, $result, $databases);
        if ($section === null) {
            return $groupedResult;
        }
        if (isset($groupedResult[$section])) {
            return $groupedResult[$section];
        }
        throw new RedisProxyException('Info section "' . $section . '" doesn\'t exist');
    }

    /**
     * @param string $key
     * @return string|null null if hash field is not set
     */
    public function get($key)
    {
        $this->init();
        $result = $this->driver->get($key);
        return $this->convertFalseToNull($result);
    }

    /**
     * Delete a key(s)
     * @param array $keys
     * @return integer number of deleted keys
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
     * @return integer number of deleted keys
     */
    public function delete(...$keys)
    {
        return $this->del(...$keys);
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
     * @param integer $count
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
     * @return integer
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
     * @param integer $increment
     * @return integer
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
     * @param integer $count
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
     * @return integer number of new members added to set
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
     * @param integer $count number of members
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
     * @param integer $count
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

    private function convertFalseToNull($result)
    {
        return $this->actualDriver() === self::DRIVER_REDIS && $result === false ? null : $result;
    }

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

<?php

namespace RedisProxy;

use Exception;
use Predis\Client;
use Predis\Response\Status;
use Redis;

/**
 * @method mixed config(string $command, $argument = null)
 * @method boolean set(string $key, string $value) Set the string value of a key
 * @method array mget(array $keys) Multi get - Returns the values of all specified keys. For every key that does not hold a string value or does not exist, FALSE is returned.
 * @method integer hset(string $key, string $field, string $value) Set the string value of a hash field
 * @method array hgetall(string $key) Get all fields and values in hash
 * @method array hGetAll(string $key) Get all fields and values in hash
 * @method integer hlen(string $key) Get the number of fields in hash
 * @method integer hLen(string $key) Get the number of fields in hash
 * @method boolean flushall() Remove all keys from all databases
 * @method boolean flushAll() Remove all keys from all databases
 * @method boolean flushdb() Remove all keys from the current database
 * @method boolean flushDb() Remove all keys from the current database
 * @method boolean flushDB() Remove all keys from the current database
 */
class RedisProxy
{
    const DRIVER_REDIS = 'redis';

    const DRIVER_PREDIS = 'predis';

    private $driver;

    private $host;

    private $port;

    private $database = 0;

    private $timeout;

    private $supportedDrivers = [
        self::DRIVER_REDIS,
        self::DRIVER_PREDIS,
    ];

    private $driversOrder = [];

    public function __construct($host, $port, $timeout = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->timeout = $timeout;
        $this->driversOrder = $this->supportedDrivers;
    }

    public function setDriversOrder(array $driversOrder)
    {
        if (empty($driversOrder)) {
            throw new RedisProxyException('You need to set at least one driver');
        }
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
        throw new RedisProxyException('No redis library loaded (ext-redis or predis)');
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
        $result = call_user_func_array([$this->driver, $name], $arguments);
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
        return $result;
    }

    /**
     * @param string|null $section
     * @return array
     */
    public function info($section = null)
    {
        $this->init();
        if ($section === null) {
            $result = $this->driver->info();
        } else {
            $section = strtolower($section);
            $result = $this->driver->info($section);
        }

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
     * @param array ...$keys
     * @return integer number of deleted keys
     */
    public function del(...$keys)
    {
        $this->init();
        return $this->driver->del(...$keys);
    }

    /**
     * Delete a key(s)
     * @param array ...$keys
     * @return integer number of deleted keys
     */
    public function delete(...$keys)
    {
        return $this->del(...$keys);
    }

    public function scan(&$iterator, $pattern = null, $count = null)
    {
        $this->init();
        if ($this->driver instanceof Client) {
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
     * @param array ...$key
     * @param array $fields
     * @return integer
     */
    public function hdel($key, ...$fields)
    {
        if (is_array($fields[0])) {
            $fields = $fields[0];
        }
        $res = $this->driver->hdel($key, ...$fields);

        return $res;
    }

    public function hscan($key, &$iterator, $pattern = null, $count = null)
    {
        $this->init();
        if ($this->driver instanceof Client) {
            $returned = $this->driver->hscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
            $iterator = $returned[0];
            return $returned[1];
        }
        return $this->driver->hscan($key, $iterator, $pattern, $count);
    }

    /**
     * Add one or more members to a set
     * @param string $key
     * @param array ...$members
     * @return integer number of new members added to set
     */
    public function sadd($key, ...$members)
    {
        if (is_array($members[0])) {
            $members = $members[0];
        }
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

    public function sscan($key, &$iterator, $pattern = null, $count = null)
    {
        $this->init();
        if ($this->driver instanceof Client) {
            $returned = $this->driver->sscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
            $iterator = $returned[0];
            return $returned[1];
        }
        return $this->driver->sscan($key, $iterator, $pattern, $count);
    }

    public function zscan($key, &$iterator, $pattern = null, $count = null)
    {
        $this->init();
        if ($this->driver instanceof Client) {
            $returned = $this->driver->zscan($key, $iterator, ['match' => $pattern, 'count' => $count]);
            $iterator = $returned[0];
            return $returned[1];
        }
        return $this->driver->zscan($key, $iterator, $pattern, $count);
    }

    private function convertFalseToNull($result)
    {
        return $this->driver instanceof Redis && $result === false ? null : $result;
    }

    private function transformResult($result)
    {
        if ($this->driver instanceof Client && $result instanceof Status) {
            $result = $result->getPayload() === 'OK';
        }
        return $result;
    }
}

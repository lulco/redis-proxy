<?php

namespace RedisProxy;

use Exception;
use InvalidArgumentException;
use Predis\Client;
use Predis\Response\Status;
use Redis;

/**
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
            throw new InvalidArgumentException('You need to set at least one driver');
        }
        foreach ($driversOrder as $driver) {
            if (!in_array($driver, $this->supportedDrivers)) {
                throw new InvalidArgumentException('Driver "' . $driver . '" is not supported');
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
        if ($this->driver instanceof Client && $result instanceof Status) {
            $result = $result->getPayload() === 'OK';
        }
        return $result;
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
        if ($this->driver instanceof Client) {
            $result = $result->getPayload() === 'OK';
        }
        if ($result === false) {
            throw new RedisProxyException('Invalid DB index');
        }
        $this->database = $database;
        return $result;
    }

    /**
     * @param string|null $section
     * @return array
     * @todo need refactoring
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

        $groupedResult = [];
        $groupedResult['keyspace'] = [];
        for ($db = 0; $db < $this->config('get', 'databases')['databases']; ++$db) {
            $groupedResult['keyspace']["db$db"] = [
                'keys' => 0,
                'expires' => null,
                'avg_ttl' => null,
            ];
        }
        if ($this->driver instanceof Client) {
            $result = array_change_key_case($result, CASE_LOWER);
            $groupedResult['keyspace'] = array_merge($groupedResult['keyspace'], $result['keyspace']);
            unset($result['keyspace']);
            $groupedResult = array_merge($groupedResult, $result);
        } else {
            $keyStartToSectionMap = [
                'redis_' => 'server',
                'uptime_' => 'server',
                'client_' => 'clients',
                'used_memory' => 'memory',
                'mem_' => 'memory',
                'rdb_' => 'persistence',
                'aof_' => 'persistence',
                'total_' => 'stats',
                'keyspace_' => 'stats',
                'repl_backlog_' => 'replication',
                'used_cpu_' => 'cpu',
                'db' => 'keyspace',
            ];
            $keyToSectionMap = [
                'connected_clients' => 'clients',
            ];
            foreach ($result as $key => $value) {
                foreach ($keyStartToSectionMap as $keyStart => $targetSection) {
                    if (strpos($key, $keyStart) === 0) {
                        if ($keyStart === 'db') {
                            $dbKeyspace = explode(',', $value);
                            $info['keys'] = explode('=', $dbKeyspace[0])[1];
                            $info['expires'] = explode('=', $dbKeyspace[1])[1];
                            $info['avg_ttl'] = explode('=', $dbKeyspace[2])[1];
                            $value = $info;
                        }
                        $groupedResult[$targetSection][$key] = $value;
                        continue;
                    }
                }
                if (isset($keyToSectionMap[$key])) {
                    $groupedResult[$keyToSectionMap[$key]][$key] = $value;
                }
            }
        }

        if ($section !== null) {
            if (isset($groupedResult[$section])) {
                return $groupedResult[$section];
            }
        }
        return $groupedResult;
    }

    /**
     * @param string $key
     * @return string
     */
    public function get($key)
    {
        $this->init();
        $result = $this->driver->get($key);
        if ($this->driver instanceof Client) {
            $result = $result === null ? false : $result;
        }
        return $result;
    }

    public function del(...$key)
    {
        $this->init();
        return $this->driver->del(...$key);
    }

    public function delete(...$key)
    {
        return $this->del(...$key);
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
     * @return string|boolean false if hash field is not set
     */
    public function hget($key, $field)
    {
        $this->init();
        $result = $this->driver->hget($key, $field);
        if ($this->driver instanceof Client) {
            $result = $result === null ? false : $result;
        }
        return $result;
    }

    /**
     * Delete one or more hash fields, returns number of deleted fields
     * @param string $key
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
}

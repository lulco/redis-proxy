<?php

namespace RedisProxy;

use Exception;
use InvalidArgumentException;
use Predis\Client;
use Redis;

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
        return call_user_func_array([$this->driver, $name], $arguments);
    }

    /**
     * @param string $database
     * @return type
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

    /**
     * @param string $key
     * @param string $value
     * @return boolean
     */
    public function set($key, $value)
    {
        $this->init();
        $result = $this->driver->set($key, $value);
        if ($this->driver instanceof Client) {
            $result = $result->getPayload() === 'OK';
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

    public function hget($key, $field)
    {
        $this->init();
        $result = $this->driver->hget($key, $field);
        if ($this->driver instanceof Client) {
            $result = $result === null ? false : $result;
        }
        return $result;
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

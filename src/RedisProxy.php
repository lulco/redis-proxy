<?php

namespace RedisProxy;

use Exception;
use InvalidArgumentException;
use Predis\Client;
use Redis;

class RedisProxy
{
    private $driver;

    private $host;

    private $port;

    private $database;

    private $timeout;

    private $supportedDrivers = [
        'redis',
        'predis'
    ];

    private $driversOrder = [];

    public function __construct($host, $port, $database, $timeout = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->database = $database;
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

        if (!$this->driver->isConnected()) {
            $this->driver->connect($this->host, $this->port, $this->timeout);
            $this->driver->select($this->database);
        }
    }

    private function prepareDriver()
    {
        if ($this->driver !== null) {
            return;
        }

        foreach ($this->driversOrder as $preferredDriver) {
            if ($preferredDriver === 'redis' && extension_loaded('redis')) {
                $this->driver = new Redis();
                return;
            }
            if ($preferredDriver === 'predis' && class_exists('Predis\Client')) {
                $this->driver = new Client();
                return;
            }
        }
        throw new Exception('No redis library loaded (ext-redis or predis)');
    }

    public function __call($name, $arguments)
    {
        $this->init();
        return call_user_func_array([$this->driver, $name], $arguments);
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

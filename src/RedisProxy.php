<?php

namespace RedisProxy;

use Exception;
use Predis\Client;
use Redis;

class RedisProxy
{
    private $driver;
    
    private $host;
    
    private $port;
    
    private $database;
    
    public function __construct($host, $port, $database)
    {
        $this->host = $host;
        $this->port = $port;
        $this->database = $database;
    }
    
    private function init()
    {
        $this->prepareDriver();
        
        if (!$this->driver->isConnected()) {
            $this->driver->connect($this->host, $this->port);
            $this->driver->select($this->database);
        }
    }
    
    private function prepareDriver()
    {
        if ($this->driver !== null) {
            return;
        }
        
        if (extension_loaded('redis')) {
            $this->driver = new Redis();
            return;
        }
        
        if (class_exists('Predis\Client')) {
            $this->driver = new Client();
            return;
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
        $this->driver->sscan($key, $iterator, $pattern, $count);
    }
}

<?php

namespace RedisProxy\DriverFactory;

use RedisProxy\ConnectionPoolFactory\ConnectionPoolFactory;
use RedisProxy\Driver\RedisDriver;

class RedisDriverFactory implements DriverFactory
{
    public function create(ConnectionPoolFactory $connectionPoolFactory): RedisDriver
    {
        return new RedisDriver($connectionPoolFactory);
    }
}

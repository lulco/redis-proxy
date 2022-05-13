<?php

namespace RedisProxy\DriverFactory;

use RedisProxy\ConnectionPoolFactory\ConnectionPoolFactory;
use RedisProxy\Driver\PredisDriver;

class PredisDriverFactory implements DriverFactory
{
    public function create(ConnectionPoolFactory $connectionPoolFactory): PredisDriver
    {
        return new PredisDriver($connectionPoolFactory);
    }
}

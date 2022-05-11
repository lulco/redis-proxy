<?php

namespace RedisProxy\DriverFactory;

use RedisProxy\ConnectionPoolFactory\ConnectionPoolFactory;
use RedisProxy\Driver\Driver;

interface DriverFactory
{
    public function create(ConnectionPoolFactory $connectionPoolFactory): Driver;
}
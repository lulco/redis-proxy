<?php

declare(strict_types=1);

namespace RedisProxy\DriverFactory;

use RedisProxy\ConnectionPoolFactory\ConnectionPoolFactory;
use RedisProxy\Driver\Driver;

interface DriverFactory
{
    public function create(ConnectionPoolFactory $connectionPoolFactory): Driver;
}

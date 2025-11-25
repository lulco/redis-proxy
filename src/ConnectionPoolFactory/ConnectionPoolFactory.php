<?php

declare(strict_types=1);

namespace RedisProxy\ConnectionPoolFactory;

use RedisProxy\ConnectionPool\ConnectionPool;
use RedisProxy\Driver\Driver;

interface ConnectionPoolFactory
{
    public function create(Driver $driver): ConnectionPool;
}

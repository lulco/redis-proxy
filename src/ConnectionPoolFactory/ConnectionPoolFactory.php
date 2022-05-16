<?php

namespace RedisProxy\ConnectionPoolFactory;

use RedisProxy\ConnectionPool\ConnectionPool;
use RedisProxy\Driver\Driver;

interface ConnectionPoolFactory
{
    public function create(Driver $driver): ConnectionPool;
}

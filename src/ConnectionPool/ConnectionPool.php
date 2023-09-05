<?php

namespace RedisProxy\ConnectionPool;

use RedisProxy\Driver\Driver;

interface ConnectionPool
{
    public function getConnection(string $command);

    public function handleFailed(): bool;
}

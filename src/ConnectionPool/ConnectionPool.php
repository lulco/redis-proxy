<?php

namespace RedisProxy\ConnectionPool;

interface ConnectionPool
{
    public function getConnection(string $command);

    /**
     * @param int $attempt First attempt is 1
     */
    public function handleFailed(int $attempt): bool;
}

<?php

namespace RedisProxy\ConnectionPool;

interface ConnectionPool
{
    public function getConnection(string $command);

    public function resetConnection(): void;

    /**
     * @param int $attempt First attempt is 1
     */
    public function handleFailed(int $attempt): bool;

    public function setRetryWait(int $retryWait): self;

    public function setMaxFails(int $maxFails): self;
}

<?php

declare(strict_types=1);

namespace RedisProxy\ConnectionPool;

interface ConnectionPool
{
    /**
     * @return \Redis|\Predis\Client
     */
    public function getConnection(string $command): \Redis|\Predis\Client;

    public function resetConnection(): void;

    /**
     * @param int $attempt First attempt is 1
     */
    public function handleFailed(int $attempt): bool;

    public function setRetryWait(int $retryWait): self;

    public function setMaxFails(int $maxFails): self;
}

<?php

declare(strict_types=1);

namespace RedisProxy\Driver;

use RedisProxy\ConnectionFactory\ConnectionFactory;
use RedisProxy\DriverFactory\DriverFactory;
use RedisProxy\RedisProxyException;

interface Driver
{
    /**
     * @param list<mixed> $params
     */
    public function call(string $command, array $params = []): mixed;

    /**
     * @param list<mixed> $params
     */
    public function callSentinel(string $command, array $params = []): mixed;

    public function getConnectionFactory(): ConnectionFactory;

    public function getDriverFactory(): DriverFactory;

    public function connectionRole(mixed $connection): string;

    /**
     * @throws RedisProxyException ('Invalid DB index');
     */
    public function connectionSelect(mixed $connection, int $database): bool;

    public function connectionReset(): void;
}

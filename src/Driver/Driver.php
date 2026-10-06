<?php

namespace RedisProxy\Driver;

use RedisProxy\ConnectionFactory\ConnectionFactory;
use RedisProxy\DriverFactory\DriverFactory;
use RedisProxy\RedisProxyException;

interface Driver
{
    public function call(string $command, array $params = []);

    public function callSentinel(string $command, array $params = []);

    public function getConnectionFactory(): ConnectionFactory;

    public function getDriverFactory(): DriverFactory;

    public function connectionRole($connection): string;

    /**
     * @throws RedisProxyException ('Invalid DB index');
     */
    public function connectionSelect($connection, int $database): bool;

    public function connectionReset(): void;
}

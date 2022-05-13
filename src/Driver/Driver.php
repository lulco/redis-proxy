<?php

namespace RedisProxy\Driver;

use RedisProxy\ConnectionFactory\ConnectionFactory;
use RedisProxy\DriverFactory\DriverFactory;

interface Driver
{
    public function call(string $command, array $params = []);

    public function callSentinel(string $command, array $params = []);

    public function getConnectionFactory(): ConnectionFactory;

    public function getDriverFactory(): DriverFactory;

    public function connectionRole($connection): string;
}

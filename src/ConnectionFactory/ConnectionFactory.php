<?php

declare(strict_types=1);

namespace RedisProxy\ConnectionFactory;

use Predis\Client;
use Redis;

interface ConnectionFactory
{
    public function create(string $host, int $port, float $timeout = 0.0): Redis|Client;
}

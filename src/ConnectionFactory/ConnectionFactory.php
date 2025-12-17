<?php

namespace RedisProxy\ConnectionFactory;

use Predis\Client;

interface ConnectionFactory
{
    /**
     * @return \Redis|Client
     */
    public function create(string $host, int $port, float $timeout = 0.0): \Redis|Client;
}

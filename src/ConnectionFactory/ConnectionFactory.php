<?php

declare(strict_types=1);

namespace RedisProxy\ConnectionFactory;

interface ConnectionFactory
{
    /**
     * @return \Redis|\Predis\Client
     */
    public function create(string $host, int $port, float $timeout = 0.0): mixed;
}

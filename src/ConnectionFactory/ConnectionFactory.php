<?php

namespace RedisProxy\ConnectionFactory;

use RedisProxy\Driver\Driver;

interface ConnectionFactory
{
    /**
     * @return mixed    Redis or Predis\Client
     */
    public function create(string $host, int $port, int $database, float $timeout = 0.0);
}
<?php

namespace RedisProxy\ConnectionFactory;

interface ConnectionFactory
{
    /**
     * @return mixed    Redis or Predis\Client
     */
    public function create(string $host, int $port, float $timeout = 0.0): mixed;
}
